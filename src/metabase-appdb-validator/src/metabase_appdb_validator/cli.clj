(ns metabase-appdb-validator.cli
  "CLI argument parsing and configuration"
  (:require [clojure.tools.cli :as cli]
            [clojure.string :as str]
            [clojure.set]))

(def cli-options
  [["-h" "--help" "Show this help message"]
   ["-v" "--version" "Show version information"]
   [nil "--verbose" "Enable verbose output"]

   ;; Database connection options
   [nil "--db-host HOST" "Database host"
    :default (or (System/getenv "METABASE_DB_HOST") "localhost")]

   [nil "--db-port PORT" "Database port"
    :default (Integer/parseInt (or (System/getenv "METABASE_DB_PORT") "5432"))
    :parse-fn #(Integer/parseInt %)]

   [nil "--db-name NAME" "Database name"
    :default (or (System/getenv "METABASE_DB_NAME") "metabase")]

   [nil "--db-user USER" "Database user"
    :default (or (System/getenv "METABASE_DB_USER") "metabase")]

   [nil "--db-password PASSWORD" "Database password"
    :default (System/getenv "METABASE_DB_PASSWORD")]

   ;; Metabase version options
   [nil "--metabase-version VERSION" "Metabase version to validate against"
    :default (or (System/getenv "METABASE_VERSION") "latest")]

   [nil "--metabase-repo-path PATH" "Path to Metabase repository"
    :default (or (System/getenv "METABASE_REPO_PATH") "/Users/jacobjoseph/dev/metabase/metabase")]

   ;; Validation scope options
   [nil "--scope SCOPES" "Validation scopes (comma-separated): structural,business-rules,data-integrity,migration-state"
    :default "structural,business-rules,data-integrity,migration-state"
    :parse-fn #(map keyword (str/split % #","))]

   ;; Output options
   [nil "--output FILE" "Output CSV file"
    :default "metabase-validation-results-TIMESTAMP.csv"]

   ;; Config file option
   [nil "--config FILE" "Configuration file (EDN format)"]])

(defn load-config-file
  "Load configuration from EDN file if specified"
  [config-file]
  (when config-file
    (try
      (read-string (slurp config-file))
      (catch Exception e
        (println "❌ Failed to load config file:" config-file)
        (println "Error:" (.getMessage e))
        (System/exit 1)))))

(defn merge-config
  "Merge CLI options with config file and environment variables"
  [cli-options config-file]
  (let [file-config (load-config-file config-file)
        env-config {:database {:host (System/getenv "METABASE_DB_HOST")
                               :port (when-let [port (System/getenv "METABASE_DB_PORT")]
                                       (Integer/parseInt port))
                               :name (System/getenv "METABASE_DB_NAME")
                               :user (System/getenv "METABASE_DB_USER")
                               :password (System/getenv "METABASE_DB_PASSWORD")}
                    :metabase-version (System/getenv "METABASE_VERSION")
                    :metabase-repo-path (System/getenv "METABASE_REPO_PATH")}]

    ;; Merge in order: defaults < env < config file < CLI
    (-> {}
        (merge env-config)
        (merge file-config)
        (merge cli-options)
        ;; Flatten database config
        (merge (select-keys (:database file-config {}) [:host :port :name :user :password]))
        ;; Rename keys to match CLI format
        (clojure.set/rename-keys {:host :db-host
                                  :port :db-port
                                  :name :db-name
                                  :user :db-user
                                  :password :db-password
                                  :output :output-file}))))

(defn validate-config
  "Validate configuration and ensure required fields are present"
  [config]
  (let [required-fields [:db-host :db-port :db-name :db-user]
        missing-fields (filter #(not (get config %)) required-fields)]

    (when (seq missing-fields)
      (println "❌ Missing required configuration:")
      (doseq [field missing-fields]
        (println "  " (name field)))
      (println "")
      (println "Specify via command line arguments, environment variables, or config file")
      (System/exit 1))

    (when-not (:db-password config)
      (println "⚠️  Warning: No database password specified")
      (println "   This may cause connection failures"))

    config))

(defn resolve-validation-scopes
  "Resolve and validate validation scopes"
  [scope-input]
  (let [valid-scopes #{:structural :business-rules :data-integrity :migration-state}
        requested-scopes (if (string? scope-input)
                           (map keyword (str/split scope-input #","))
                           scope-input)
        invalid-scopes (remove valid-scopes requested-scopes)]

    (when (seq invalid-scopes)
      (println "❌ Invalid validation scopes:" (str/join ", " invalid-scopes))
      (println "Valid scopes:" (str/join ", " (map name valid-scopes)))
      (System/exit 1))

    requested-scopes))