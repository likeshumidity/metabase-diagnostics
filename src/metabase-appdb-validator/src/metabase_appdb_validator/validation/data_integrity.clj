(ns metabase-appdb-validator.validation.data-integrity
  "Data integrity validation - Value constraints, pattern matching, custom business logic"
  (:require [metabase-appdb-validator.database.connection :as db-conn]
            [clojure.string :as str]))

(defn create-validation-result
  "Create a standardized validation result"
  [table-name column-name check-passed error-message schema-source identification-method metabase-version]
  {:table-name table-name
   :column-name column-name
   :validation-scope "data-integrity"
   :check-passed check-passed
   :error-message error-message
   :schema-source schema-source
   :identification-method identification-method
   :metabase-version metabase-version
   :validation-timestamp (java.time.Instant/now)})

(defn validate-referential-integrity
  "Validate referential integrity beyond standard foreign keys"
  [conn-spec schema-def metabase-version]
  (let [expected-tables (:tables (:unified-schema schema-def))]

    ;; Example: Check that user references in various tables point to existing users
    (try
      (let [user-check-query "SELECT COUNT(*) as orphaned_count
                             FROM report_card rc
                             LEFT JOIN core_user cu ON rc.creator_id = cu.id
                             WHERE rc.creator_id IS NOT NULL AND cu.id IS NULL"
            result (first (db-conn/execute-query conn-spec user-check-query))
            orphaned-count (:orphaned_count result 0)]

        [(create-validation-result
           "report_card" "creator_id"
           (= 0 orphaned-count)
           (when (> orphaned-count 0)
             (str "Found " orphaned-count " orphaned creator_id references"))
           "data-integrity-check" "deterministic" metabase-version)])

      (catch Exception e
        [(create-validation-result
           "report_card" "creator_id" false
           (str "Failed to check referential integrity: " (.getMessage e))
           "data-integrity-check" "deterministic" metabase-version)]))))

(defn validate-enum-value-constraints
  "Validate that columns contain only expected enum values"
  [conn-spec schema-def metabase-version]
  (let [checks [;; Example: Check visualization types in report_card
                {:table "report_card"
                 :column "display"
                 :expected-values #{"table" "bar" "line" "area" "pie" "scalar" "smartscalar" "gauge" "funnel"}
                 :query "SELECT DISTINCT display FROM report_card WHERE display IS NOT NULL"}

                ;; Example: Check database engines
                {:table "metabase_database"
                 :column "engine"
                 :expected-values #{"postgres" "mysql" "h2" "sqlite" "sqlserver" "oracle" "bigquery" "snowflake"}
                 :query "SELECT DISTINCT engine FROM metabase_database WHERE engine IS NOT NULL"}]]

    (reduce (fn [acc check]
              (try
                (let [results (db-conn/execute-query conn-spec (:query check))
                      actual-values (set (map (comp str first vals) results))
                      expected-values (:expected-values check)
                      invalid-values (clojure.set/difference actual-values expected-values)]

                  (conj acc
                        (create-validation-result
                          (:table check) (:column check)
                          (empty? invalid-values)
                          (when (seq invalid-values)
                            (str "Invalid enum values found: " (str/join ", " invalid-values)))
                          "data-integrity-check" "deterministic" metabase-version)))

                (catch Exception e
                  (conj acc
                        (create-validation-result
                          (:table check) (:column check) false
                          (str "Failed to check enum values: " (.getMessage e))
                          "data-integrity-check" "deterministic" metabase-version)))))
            []
            checks)))

(defn validate-data-format-patterns
  "Validate data format patterns using regex"
  [conn-spec schema-def metabase-version]
  (let [checks [;; Example: Check email format in core_user
                {:table "core_user"
                 :column "email"
                 :pattern #"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                 :query "SELECT email FROM core_user WHERE email IS NOT NULL AND email ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$' = false LIMIT 10"}

                ;; Example: Check UUID format where applicable
                {:table "metabase_database"
                 :column "id"
                 :pattern #"^\d+$"  ; Simplified - Metabase uses integer IDs
                 :query "SELECT id FROM metabase_database WHERE id::text !~ '^\\d+$' LIMIT 10"}]]

    (reduce (fn [acc check]
              (try
                (let [invalid-results (db-conn/execute-query conn-spec (:query check))
                      invalid-count (count invalid-results)]

                  (conj acc
                        (create-validation-result
                          (:table check) (:column check)
                          (= 0 invalid-count)
                          (when (> invalid-count 0)
                            (str "Found " invalid-count " records with invalid format"))
                          "data-integrity-check" "deterministic" metabase-version)))

                (catch Exception e
                  (conj acc
                        (create-validation-result
                          (:table check) (:column check) false
                          (str "Failed to check data format: " (.getMessage e))
                          "data-integrity-check" "deterministic" metabase-version)))))
            []
            checks)))

(defn validate-business-logic-constraints
  "Validate custom business logic constraints"
  [conn-spec schema-def metabase-version]
  (let [checks [;; Example: Check that archived items have archived_at timestamp
                {:name "archived-timestamp-consistency"
                 :query "SELECT COUNT(*) as inconsistent_count FROM report_card WHERE archived = true AND updated_at IS NULL"
                 :table "report_card"
                 :column "archived"}

                ;; Example: Check that public sharing requires appropriate permissions
                {:name "public-sharing-permissions"
                 :query "SELECT COUNT(*) as violation_count FROM report_card WHERE public_uuid IS NOT NULL AND made_public_by_id IS NULL"
                 :table "report_card"
                 :column "public_uuid"}]]

    (reduce (fn [acc check]
              (try
                (let [result (first (db-conn/execute-query conn-spec (:query check)))
                      violation-count (or (:inconsistent_count result)
                                         (:violation_count result)
                                         0)]

                  (conj acc
                        (create-validation-result
                          (:table check) (:column check)
                          (= 0 violation-count)
                          (when (> violation-count 0)
                            (str "Business logic violation (" (:name check) "): " violation-count " records"))
                          "data-integrity-check" "deterministic" metabase-version)))

                (catch Exception e
                  (conj acc
                        (create-validation-result
                          (:table check) (:column check) false
                          (str "Failed to check business logic (" (:name check) "): " (.getMessage e))
                          "data-integrity-check" "deterministic" metabase-version)))))
            []
            checks)))

(defn validate-json-structure
  "Validate JSON column structures"
  [conn-spec schema-def metabase-version]
  (let [checks [;; Example: Check settings table JSON structure
                {:table "setting"
                 :column "setting_value"
                 :query "SELECT setting_key FROM setting WHERE setting_value IS NOT NULL AND setting_value::json IS NULL LIMIT 10"}

                ;; Example: Check report_card visualization_settings
                {:table "report_card"
                 :column "visualization_settings"
                 :query "SELECT id FROM report_card WHERE visualization_settings IS NOT NULL AND visualization_settings::json IS NULL LIMIT 10"}]]

    (reduce (fn [acc check]
              (try
                (let [invalid-results (db-conn/execute-query conn-spec (:query check))
                      invalid-count (count invalid-results)]

                  (conj acc
                        (create-validation-result
                          (:table check) (:column check)
                          (= 0 invalid-count)
                          (when (> invalid-count 0)
                            (str "Found " invalid-count " records with invalid JSON structure"))
                          "data-integrity-check" "deterministic" metabase-version)))

                (catch Exception e
                  (conj acc
                        (create-validation-result
                          (:table check) (:column check) false
                          (str "Failed to check JSON structure: " (.getMessage e))
                          "data-integrity-check" "deterministic" metabase-version)))))
            []
            checks)))

(defn validate-data-integrity
  "Main data integrity validation function"
  [config db-info schema-def]
  (let [conn-spec (:connection db-info)
        metabase-version (:metabase-version config "unknown")]

    (concat
      ;; Referential integrity validation
      (validate-referential-integrity conn-spec schema-def metabase-version)

      ;; Enum value constraint validation
      (validate-enum-value-constraints conn-spec schema-def metabase-version)

      ;; Data format pattern validation
      (validate-data-format-patterns conn-spec schema-def metabase-version)

      ;; Business logic constraint validation
      (validate-business-logic-constraints conn-spec schema-def metabase-version)

      ;; JSON structure validation
      (validate-json-structure conn-spec schema-def metabase-version))))