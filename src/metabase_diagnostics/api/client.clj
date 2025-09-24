(ns metabase-diagnostics.api.client
  "Metabase API client for diagnostic tools"
  (:require [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]))

;; HTTP Status Codes
(def HTTP_OK_START 200)
(def HTTP_OK_END 299)
(def HTTP_UNAUTHORIZED 401)
(def HTTP_FORBIDDEN 403)
(def HTTP_NOT_FOUND 404)

;; Default Configuration Values
(def DEFAULT_TIMEOUT_MS 30000)
(def DEFAULT_RETRY_COUNT 3)

(defn create-client
  "Create a Metabase API client with API key authentication.

  Options:
  - :base-url - Metabase base URL (required)
  - :api-key - API key for authentication (required)
  - :timeout - Request timeout in ms (default: 30000)
  - :retries - Number of retries (default: 3)"
  [{:keys [base-url api-key timeout retries]
    :or {timeout DEFAULT_TIMEOUT_MS retries DEFAULT_RETRY_COUNT}}]
  {:pre [(string? base-url)
         (string? api-key)]}
  (let [client-state (atom {:base-url (if (.endsWith base-url "/")
                                        (subs base-url 0 (dec (count base-url)))
                                        base-url)
                            :api-key api-key
                            :timeout timeout
                            :retries retries})]
    (log/info "Created Metabase API client with API key authentication")
    client-state))

(defn- make-request
  "Make an authenticated request to the Metabase API"
  [client-state method path & {:keys [query-params form-params json-params headers]}]
  (let [{:keys [base-url api-key timeout]} @client-state
        url (str base-url path)
        request-headers (merge {"X-API-Key" api-key}
                               headers)
        request-options {:headers request-headers
                         :accept :json
                         :socket-timeout timeout
                         :conn-timeout timeout
                         :throw-exceptions false}
        request-options (cond-> request-options
                          query-params (assoc :query-params query-params)
                          form-params (assoc :form-params form-params)
                          json-params (assoc :body (json/write-str json-params)
                                             :content-type :json))]
    (log/debug "Making API request:" method url)
    (case method
      :get (http/get url request-options)
      :post (http/post url request-options)
      :put (http/put url request-options)
      :delete (http/delete url request-options))))

(defn- parse-response
  "Parse API response and handle errors"
  [response]
  (let [status (:status response)
        body (:body response)]
    (cond
      (<= HTTP_OK_START status HTTP_OK_END)
      (try
        (json/read-str body :key-fn keyword)
        (catch Exception e
          (log/warn "Failed to parse JSON response:" body)
          {:raw-body body}))

      (= status HTTP_UNAUTHORIZED)
      (throw (ex-info "Unauthorized - check API key"
                      {:type :unauthorized :status status}))

      (= status HTTP_FORBIDDEN)
      (throw (ex-info "Forbidden - insufficient permissions"
                      {:type :forbidden :status status}))

      (= status HTTP_NOT_FOUND)
      (throw (ex-info "Not found"
                      {:type :not-found :status status}))

      :else
      (throw (ex-info "API request failed"
                      {:type :api-error :status status :body body})))))

(defn get-api
  "Make a GET request to the Metabase API"
  [client path & {:keys [query-params]}]
  (-> (make-request client :get path :query-params query-params)
      parse-response))

(defn post-api
  "Make a POST request to the Metabase API"
  [client path & {:keys [json-params form-params]}]
  (-> (make-request client :post path
                    :json-params json-params
                    :form-params form-params)
      parse-response))

(defn put-api
  "Make a PUT request to the Metabase API"
  [client path & {:keys [json-params]}]
  (-> (make-request client :put path :json-params json-params)
      parse-response))

(defn delete-api
  "Make a DELETE request to the Metabase API"
  [client path]
  (-> (make-request client :delete path)
      parse-response))

;; High-level API functions for common operations

(defn get-dashboards
  "Get all dashboards"
  [client]
  (get-api client "/api/dashboard"))

(defn get-dashboard
  "Get a specific dashboard by ID"
  [client dashboard-id]
  (get-api client (str "/api/dashboard/" dashboard-id)))

(defn get-cards
  "Get all cards (questions)"
  [client]
  (get-api client "/api/card"))

(defn get-card
  "Get a specific card by ID"
  [client card-id]
  (get-api client (str "/api/card/" card-id)))

(defn get-collections
  "Get all collections"
  [client]
  (get-api client "/api/collection"))

(defn get-collection
  "Get a specific collection by ID"
  [client collection-id]
  (get-api client (str "/api/collection/" collection-id)))

(defn get-databases
  "Get all databases"
  [client]
  (get-api client "/api/database"))

(defn get-database
  "Get a specific database by ID"
  [client database-id]
  (get-api client (str "/api/database/" database-id)))

(defn get-tables
  "Get all tables for a database"
  [client database-id]
  (get-api client (str "/api/database/" database-id "/tables")))

(defn get-table
  "Get a specific table"
  [client table-id]
  (get-api client (str "/api/table/" table-id)))

(defn get-fields
  "Get all fields for a table"
  [client table-id]
  (get-api client (str "/api/table/" table-id "/fields")))

(defn health-check
  "Check if the Metabase instance is healthy"
  [client]
  (try
    (get-api client "/api/health")
    true
    (catch Exception e
      (log/warn "Health check failed:" (ex-message e))
      false)))

(defn get-user
  "Get current user information"
  [client]
  (get-api client "/api/user/current"))