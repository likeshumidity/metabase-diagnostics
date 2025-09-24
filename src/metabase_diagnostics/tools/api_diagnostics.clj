(ns metabase-diagnostics.tools.api-diagnostics
  "API diagnostics and health checks for Metabase instances"
  (:require [clojure.tools.cli :as cli]
            [clojure.string :as str]
            [metabase-diagnostics.api.client :as api]
            [metabase-diagnostics.logging.core :as log]
            [metabase-diagnostics.output.formatter :as fmt]))

;; Tool-specific constants
(def DEFAULT_TIMEOUT_SECONDS 10)
(def DEFAULT_RETRIES 1)
(def DEFAULT_BASE_URL "http://localhost:3000")

;; Tool options
(def TOOL_OPTIONS
  [["-h" "--help" "Show help for api-diagnostics"]
   ["-u" "--base-url URL" "Metabase base URL" :required true
    :default DEFAULT_BASE_URL]
   ["-k" "--api-key KEY" "Metabase API key" :required true]
   [nil "--timeout SECONDS" "Request timeout in seconds"
    :default DEFAULT_TIMEOUT_SECONDS
    :parse-fn #(Integer/parseInt %)]
   [nil "--retries COUNT" "Number of retries"
    :default DEFAULT_RETRIES
    :parse-fn #(Integer/parseInt %)]
   [nil "--[no-]check-health" "Check instance health" :default true]
   [nil "--[no-]check-databases" "Check database connectivity" :default true]
   [nil "--[no-]check-collections" "Check collections access" :default true]])

(defn show-help
  "Show help for api-diagnostics tool"
  []
  (println "API Diagnostics - Check Metabase instance health via API")
  (println)
  (println "Options:")
  (println (:summary (cli/parse-opts [] TOOL_OPTIONS))))

(defn check-instance-health
  "Check basic instance health"
  [client]
  (log/info "Checking instance health...")
  (try
    (let [health-result (api/health-check client)]
      {:test "instance-health"
       :status (if health-result "pass" "fail")
       :details (if health-result "Instance is responding" "Instance is not responding")})
    (catch Exception e
      {:test "instance-health"
       :status "fail"
       :details (ex-message e)})))

(defn check-user-access
  "Check current user access"
  [client]
  (log/info "Checking user access...")
  (try
    (let [user-info (api/get-user client)]
      {:test "user-access"
       :status "pass"
       :details (str "User: " (:common_name user-info (:email user-info "Unknown")))
       :user-id (:id user-info)
       :is-admin (:is_superuser user-info false)})
    (catch Exception e
      {:test "user-access"
       :status "fail"
       :details (ex-message e)})))

(defn check-databases-access
  "Check database access and connectivity"
  [client]
  (log/info "Checking databases access...")
  (try
    (let [databases (:data (api/get-databases client))
          database-count (count databases)
          db-selected-keys [:id :name :engine]]
      {:test "databases-access"
       :status "pass"
       :details (str "Found " database-count " databases.")
       :database-count database-count
       :databases (map #(select-keys % db-selected-keys) databases)})
    (catch Exception e
      {:test "databases-access"
       :status "fail"
       :details (ex-message e)})))

(defn check-collections-access
  "Check collections access"
  [client]
  (log/info "Checking collections access...")
  (try
    (let [collections (api/get-collections client)
          collection-count (count collections)
          personal-collections (filter #(= (:personal_owner_id %) nil) collections)]
      {:test "collections-access"
       :status "pass"
       :details (str "Found " collection-count " collections, "
                    (count personal-collections) " non-personal")
       :collection-count collection-count
       :non-personal-count (count personal-collections)})
    (catch Exception e
      {:test "collections-access"
       :status "fail"
       :details (ex-message e)})))

(defn run-diagnostics
  "Run API diagnostics based on provided options"
  [{:keys [base-url api-key timeout retries check-health check-databases check-collections] :as opts}]
  (log/info "Starting API diagnostics for %s" base-url)

  (let [client (api/create-client {:base-url base-url
                                   :api-key api-key
                                   :timeout (* timeout 1000)
                                   :retries retries})
        results (atom [])]

    (try
      ;; Always check user access first
      (swap! results conj (check-user-access client))

      ;; Run optional checks based on configuration
      (when check-health
        (swap! results conj (check-instance-health client)))

      (when check-databases
        (swap! results conj (check-databases-access client)))

      (when check-collections
        (swap! results conj (check-collections-access client)))

      (let [final-results @results
            passed-tests (count (filter #(= (:status %) "pass") final-results))
            total-tests (count final-results)
            success (= passed-tests total-tests)]

        (log/info "Completed API diagnostics for %s - %s" base-url (if success "success" "failure"))

        {:summary {:total-tests total-tests
                   :passed-tests passed-tests
                   :failed-tests (- total-tests passed-tests)
                   :success success}
         :results final-results
         :target base-url
         :timestamp (str (java.time.Instant/now))})

      (catch Exception e
        (log/error-with-exception e "API diagnostics failed")
        {:error (ex-message e)
         :type :diagnostics-error
         :target base-url}))))

(defn run-validation
  "Main entry point for the tool (matches expected interface)"
  [{:keys [tool-args global-options] :as args}]
  (let [parsed (cli/parse-opts tool-args TOOL_OPTIONS)
        opts (:options parsed)
        errors (:errors parsed)]

    (cond
      errors
      {:error (str "Invalid options: " (str/join ", " errors))
       :type :parse-error}

      (:help opts)
      (do (show-help) {:action :help-shown})

      (not (and (:base-url opts) (:api-key opts)))
      {:error "Both --base-url and --api-key are required"
       :type :missing-required-options}

      :else
      (run-diagnostics opts))))
