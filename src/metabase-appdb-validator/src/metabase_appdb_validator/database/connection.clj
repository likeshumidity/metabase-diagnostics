(ns metabase-appdb-validator.database.connection
  "Database connection and validation"
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [clojure.string :as str]))

(defn create-connection-spec
  "Create JDBC connection specification from config"
  [config]
  {:dbtype "postgresql"
   :host (:db-host config)
   :port (:db-port config)
   :dbname (:db-name config)
   :user (:db-user config)
   :password (:db-password config)
   :currentSchema "public"})

(defn test-connection
  "Test database connection and return connection info"
  [conn-spec]
  (try
    (with-open [conn (jdbc/get-connection conn-spec)]
      (let [db-meta (.getMetaData conn)
            db-type (.getDatabaseProductName db-meta)
            db-version (.getDatabaseProductVersion db-meta)]
        {:success true
         :db-type db-type
         :db-version db-version
         :connection conn-spec}))
    (catch Exception e
      {:success false
       :error (.getMessage e)
       :exception e})))

(defn validate-database-type
  "Validate that the database is PostgreSQL"
  [db-info]
  (let [db-type (:db-type db-info)]
    (when-not (str/includes? (str/lower-case db-type) "postgresql")
      (throw (ex-info
               (str "Unsupported database type: " db-type "\n"
                    "This validator currently supports PostgreSQL only.\n"
                    "Supported databases:\n"
                    "  - PostgreSQL (any version)\n"
                    "\n"
                    "To add support for other databases, please contribute to:\n"
                    "  https://github.com/metabase/metabase-diagnostics")
               {:db-type db-type
                :supported-types ["PostgreSQL"]})))))

(defn get-metabase-version-from-db
  "Attempt to detect Metabase version from database"
  [conn-spec]
  (try
    (with-open [conn (jdbc/get-connection conn-spec)]
      ;; Try to get version from settings table
      (let [version-query "SELECT setting_value FROM setting WHERE setting_key = 'version'"]
        (try
          (-> (jdbc/execute-one! conn [version-query] {:builder-fn rs/as-unqualified-lower-maps})
              :setting_value)
          (catch Exception _
            ;; If settings table doesn't exist or query fails, try alternative approaches
            nil))))
    (catch Exception _
      nil)))

(defn get-applied-migrations
  "Get list of applied Liquibase migrations from database"
  [conn-spec]
  (try
    (with-open [conn (jdbc/get-connection conn-spec)]
      (let [migration-query "SELECT id, author, filename, dateexecuted, orderexecuted
                            FROM databasechangelog
                            ORDER BY orderexecuted ASC"]
        (jdbc/execute! conn [migration-query] {:builder-fn rs/as-unqualified-lower-maps})))
    (catch Exception e
      ;; If databasechangelog doesn't exist, return empty list
      ;; This indicates a fresh database or non-Metabase database
      [])))

(defn validate-connection
  "Validate database connection and return connection info with Metabase metadata"
  [config]
  (let [conn-spec (create-connection-spec config)
        conn-test (test-connection conn-spec)]

    (when-not (:success conn-test)
      (throw (ex-info
               (str "Failed to connect to database: " (:error conn-test))
               {:config config
                :error (:error conn-test)})))

    ;; Validate database type
    (validate-database-type conn-test)

    ;; Get additional Metabase-specific information
    (let [metabase-version (get-metabase-version-from-db conn-spec)
          applied-migrations (get-applied-migrations conn-spec)]

      (merge conn-test
             {:metabase-version metabase-version
              :applied-migrations applied-migrations
              :migration-count (count applied-migrations)
              :has-metabase-schema (> (count applied-migrations) 0)}))))

(defn execute-query
  "Execute a SQL query against the database"
  [conn-spec query & [params]]
  (try
    (with-open [conn (jdbc/get-connection conn-spec)]
      (if params
        (jdbc/execute! conn (vec (cons query params)) {:builder-fn rs/as-unqualified-lower-maps})
        (jdbc/execute! conn [query] {:builder-fn rs/as-unqualified-lower-maps})))
    (catch Exception e
      (throw (ex-info
               (str "Query execution failed: " (.getMessage e))
               {:query query
                :params params
                :error (.getMessage e)})))))

(defn get-table-info
  "Get comprehensive table information from PostgreSQL information schema"
  [conn-spec table-name]
  (let [table-query "SELECT
                       t.table_name,
                       t.table_type,
                       t.table_schema,
                       obj_description(c.oid) as table_comment
                     FROM information_schema.tables t
                     LEFT JOIN pg_class c ON c.relname = t.table_name
                     WHERE t.table_name = ? AND t.table_schema = 'public'"

        column-query "SELECT
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length,
                        numeric_precision,
                        numeric_scale,
                        ordinal_position,
                        col_description(pgc.oid, cols.ordinal_position) as column_comment
                      FROM information_schema.columns cols
                      LEFT JOIN pg_class pgc ON pgc.relname = cols.table_name
                      WHERE cols.table_name = ? AND cols.table_schema = 'public'
                      ORDER BY cols.ordinal_position"

        constraint-query "SELECT
                           tc.constraint_name,
                           tc.constraint_type,
                           kcu.column_name,
                           ccu.table_name AS foreign_table_name,
                           ccu.column_name AS foreign_column_name
                         FROM information_schema.table_constraints tc
                         LEFT JOIN information_schema.key_column_usage kcu
                           ON tc.constraint_name = kcu.constraint_name
                         LEFT JOIN information_schema.constraint_column_usage ccu
                           ON ccu.constraint_name = tc.constraint_name
                         WHERE tc.table_name = ? AND tc.table_schema = 'public'"]

    (let [table-info (first (execute-query conn-spec table-query table-name))
          columns (execute-query conn-spec column-query table-name)
          constraints (execute-query conn-spec constraint-query table-name)]

      (when table-info
        (assoc table-info
               :columns columns
               :constraints constraints)))))

(defn list-all-tables
  "List all tables in the public schema"
  [conn-spec]
  (let [query "SELECT table_name, table_type
               FROM information_schema.tables
               WHERE table_schema = 'public'
               ORDER BY table_name"]
    (execute-query conn-spec query)))

(defn get-database-schema
  "Get complete database schema information"
  [conn-spec]
  (let [tables (list-all-tables conn-spec)]
    {:tables (map #(get-table-info conn-spec (:table_name %)) tables)
     :table-count (count tables)
     :schema-name "public"}))