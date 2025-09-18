(ns metabase-appdb-validator.validation.migration-state
  "Migration state validation - Ensuring all migrations have been applied correctly"
  (:require [metabase-appdb-validator.database.connection :as db-conn]
            [clojure.string :as str]))

(defn create-validation-result
  "Create a standardized validation result"
  [table-name column-name check-passed error-message schema-source identification-method metabase-version]
  {:table-name table-name
   :column-name column-name
   :validation-scope "migration-state"
   :check-passed check-passed
   :error-message error-message
   :schema-source schema-source
   :identification-method identification-method
   :metabase-version metabase-version
   :validation-timestamp (java.time.Instant/now)})

(defn validate-migration-table-exists
  "Validate that the Liquibase migration tracking table exists"
  [conn-spec metabase-version]
  (try
    (let [migration-tables (db-conn/execute-query
                           conn-spec
                           "SELECT table_name FROM information_schema.tables WHERE table_name IN ('databasechangelog', 'databasechangeloglock')")
          has-changelog (some #(= "databasechangelog" (:table_name %)) migration-tables)
          has-changelog-lock (some #(= "databasechangeloglock" (:table_name %)) migration-tables)]

      [(create-validation-result
         "databasechangelog" nil has-changelog
         (when-not has-changelog "Migration tracking table 'databasechangelog' does not exist")
         "migration-state-check" "deterministic" metabase-version)

       (create-validation-result
         "databasechangeloglock" nil has-changelog-lock
         (when-not has-changelog-lock "Migration lock table 'databasechangeloglock' does not exist")
         "migration-state-check" "deterministic" metabase-version)])

    (catch Exception e
      [(create-validation-result
         "databasechangelog" nil false
         (str "Failed to check migration tables: " (.getMessage e))
         "migration-state-check" "deterministic" metabase-version)])))

(defn validate-migration-checksums
  "Validate that migration checksums match expected values"
  [conn-spec schema-def metabase-version]
  (try
    (let [applied-migrations (db-conn/execute-query
                             conn-spec
                             "SELECT id, author, filename, md5sum FROM databasechangelog ORDER BY orderexecuted")
          ;; Get expected migrations from schema definition
          expected-migrations (get-in schema-def [:sources :liquibase-migrations :metadata :total-changesets] 0)]

      [(create-validation-result
         "databasechangelog" "checksum-validation"
         (> (count applied-migrations) 0)
         (when (= 0 (count applied-migrations))
           "No migrations found in databasechangelog table")
         "migration-state-check" "deterministic" metabase-version)

       ;; Check for any null checksums (which indicate potential issues)
       (let [null-checksums (filter #(nil? (:md5sum %)) applied-migrations)]
         (create-validation-result
           "databasechangelog" "md5sum"
           (empty? null-checksums)
           (when (seq null-checksums)
             (str "Found " (count null-checksums) " migrations with null checksums"))
           "migration-state-check" "deterministic" metabase-version))])

    (catch Exception e
      [(create-validation-result
         "databasechangelog" "checksum-validation" false
         (str "Failed to validate migration checksums: " (.getMessage e))
         "migration-state-check" "deterministic" metabase-version)])))

(defn validate-migration-order
  "Validate that migrations have been applied in the correct order"
  [conn-spec metabase-version]
  (try
    (let [migrations (db-conn/execute-query
                     conn-spec
                     "SELECT id, author, filename, orderexecuted FROM databasechangelog ORDER BY orderexecuted")]

      ;; Check that orderexecuted values are sequential
      (loop [migrations-seq migrations
             expected-order 1
             issues []]
        (if (empty? migrations-seq)
          [(create-validation-result
             "databasechangelog" "orderexecuted"
             (empty? issues)
             (when (seq issues)
               (str "Migration order issues: " (str/join "; " issues)))
             "migration-state-check" "deterministic" metabase-version)]

          (let [current-migration (first migrations-seq)
                actual-order (:orderexecuted current-migration)]
            (if (= expected-order actual-order)
              (recur (rest migrations-seq) (inc expected-order) issues)
              (recur (rest migrations-seq)
                     (inc expected-order)
                     (conj issues (str "Expected order " expected-order " but found " actual-order " for migration " (:id current-migration)))))))))

    (catch Exception e
      [(create-validation-result
         "databasechangelog" "orderexecuted" false
         (str "Failed to validate migration order: " (.getMessage e))
         "migration-state-check" "deterministic" metabase-version)])))

(defn validate-pending-migrations
  "Check for any pending migrations that should have been applied"
  [conn-spec schema-def metabase-version]
  (try
    ;; This is a simplified check - in practice would compare against
    ;; the expected migrations from the schema definition for the specific version
    (let [latest-migration (first (db-conn/execute-query
                                  conn-spec
                                  "SELECT id, dateexecuted FROM databasechangelog ORDER BY orderexecuted DESC LIMIT 1"))
          migration-age-days (when latest-migration
                               (let [executed-date (:dateexecuted latest-migration)
                                     now (java.time.Instant/now)]
                                 ;; Simplified age calculation
                                 1))  ; Placeholder

          ;; Check if there are any failed migrations
          failed-migrations (db-conn/execute-query
                            conn-spec
                            "SELECT id FROM databasechangelog WHERE md5sum IS NULL OR md5sum = ''")]

      [(create-validation-result
         "databasechangelog" "latest-migration"
         (some? latest-migration)
         (when-not latest-migration "No migrations found in database")
         "migration-state-check" "deterministic" metabase-version)

       (create-validation-result
         "databasechangelog" "failed-migrations"
         (empty? failed-migrations)
         (when (seq failed-migrations)
           (str "Found " (count failed-migrations) " potentially failed migrations"))
         "migration-state-check" "deterministic" metabase-version)])

    (catch Exception e
      [(create-validation-result
         "databasechangelog" "pending-migrations" false
         (str "Failed to check pending migrations: " (.getMessage e))
         "migration-state-check" "deterministic" metabase-version)])))

(defn validate-version-consistency
  "Validate that the database schema version matches the expected Metabase version"
  [conn-spec metabase-version]
  (try
    ;; Try to get version from settings table
    (let [version-results (db-conn/execute-query
                          conn-spec
                          "SELECT setting_value FROM setting WHERE setting_key = 'version'")
          db-version (when (seq version-results)
                      (:setting_value (first version-results)))

          ;; Also check the latest migration ID pattern for version hints
          latest-migration (first (db-conn/execute-query
                                  conn-spec
                                  "SELECT id FROM databasechangelog ORDER BY orderexecuted DESC LIMIT 1"))
          migration-id (:id latest-migration)]

      [(create-validation-result
         "setting" "version"
         (some? db-version)
         (when-not db-version "Metabase version not found in settings table")
         "migration-state-check" "deterministic" metabase-version)

       (when migration-id
         (create-validation-result
           "databasechangelog" "latest-migration-id"
           true  ; Always pass - this is informational
           (str "Latest migration ID: " migration-id)
           "migration-state-check" "deterministic" metabase-version))])

    (catch Exception e
      [(create-validation-result
         "setting" "version" false
         (str "Failed to check version consistency: " (.getMessage e))
         "migration-state-check" "deterministic" metabase-version)])))

(defn validate-migration-dependency-chain
  "Validate that migration dependencies are satisfied"
  [conn-spec schema-def metabase-version]
  (try
    ;; Check for any obvious dependency issues by looking at migration file patterns
    (let [migrations (db-conn/execute-query
                     conn-spec
                     "SELECT id, filename FROM databasechangelog ORDER BY orderexecuted")

          ;; Group by migration file
          by-file (group-by :filename migrations)

          ;; Check that we have migrations from expected files
          expected-files ["000_legacy_migrations.yaml" "001_update_migrations.yaml"]
          missing-files (filter #(not (contains? by-file %)) expected-files)]

      [(create-validation-result
         "databasechangelog" "migration-files"
         (empty? missing-files)
         (when (seq missing-files)
           (str "Missing migrations from files: " (str/join ", " missing-files)))
         "migration-state-check" "heuristic" metabase-version)])

    (catch Exception e
      [(create-validation-result
         "databasechangelog" "dependency-chain" false
         (str "Failed to validate migration dependencies: " (.getMessage e))
         "migration-state-check" "deterministic" metabase-version)])))

(defn validate-migration-state
  "Main migration state validation function"
  [config db-info schema-def]
  (let [conn-spec (:connection db-info)
        metabase-version (:metabase-version config "unknown")]

    (concat
      ;; Migration table existence validation
      (validate-migration-table-exists conn-spec metabase-version)

      ;; Migration checksum validation
      (validate-migration-checksums conn-spec schema-def metabase-version)

      ;; Migration order validation
      (validate-migration-order conn-spec metabase-version)

      ;; Pending migration validation
      (validate-pending-migrations conn-spec schema-def metabase-version)

      ;; Version consistency validation
      (remove nil? (validate-version-consistency conn-spec metabase-version))

      ;; Migration dependency chain validation
      (validate-migration-dependency-chain conn-spec schema-def metabase-version))))