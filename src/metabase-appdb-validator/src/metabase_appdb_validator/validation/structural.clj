(ns metabase-appdb-validator.validation.structural
  "Structural validation - Table/column existence, data types, constraints"
  (:require [metabase-appdb-validator.database.connection :as db-conn]
            [clojure.string :as str]))

(defn create-validation-result
  "Create a standardized validation result"
  [table-name column-name check-passed error-message schema-source identification-method metabase-version]
  {:table-name table-name
   :column-name column-name
   :validation-scope "structural"
   :check-passed check-passed
   :error-message error-message
   :schema-source schema-source
   :identification-method identification-method
   :metabase-version metabase-version
   :validation-timestamp (java.time.Instant/now)})

(defn validate-table-existence
  "Validate that expected tables exist in the database"
  [conn-spec schema-def metabase-version]
  (let [expected-tables (keys (:tables (:unified-schema schema-def)))
        existing-tables (set (map (comp keyword :table_name) (db-conn/list-all-tables conn-spec)))]

    (map (fn [table-key]
           (let [table-def (get-in schema-def [:unified-schema :tables table-key])
                 table-name (:table-name table-def (name table-key))
                 schema-source (:source table-def "unknown")
                 identification-method (:identification-method table-def "unknown")]

             (if (contains? existing-tables table-key)
               (create-validation-result
                 table-name nil true nil
                 schema-source identification-method metabase-version)
               (create-validation-result
                 table-name nil false
                 (str "Table '" table-name "' does not exist in database")
                 schema-source identification-method metabase-version))))
         expected-tables)))

(defn validate-column-existence
  "Validate that expected columns exist in tables"
  [conn-spec schema-def metabase-version]
  (let [expected-tables (:tables (:unified-schema schema-def))]

    (reduce (fn [acc [table-key table-def]]
              (let [table-name (:table-name table-def (name table-key))
                    expected-columns (:columns table-def {})
                    db-table-info (db-conn/get-table-info conn-spec table-name)]

                (if db-table-info
                  (let [existing-columns (set (map (comp keyword str/lower-case :column_name)
                                                  (:columns db-table-info)))]

                    (concat acc
                            (map (fn [[col-key col-def]]
                                   (let [column-name (:column-name col-def (name col-key))
                                         schema-source (:source col-def "unknown")
                                         identification-method (:identification-method col-def "unknown")]

                                     (if (contains? existing-columns (keyword (str/lower-case column-name)))
                                       (create-validation-result
                                         table-name column-name true nil
                                         schema-source identification-method metabase-version)
                                       (create-validation-result
                                         table-name column-name false
                                         (str "Column '" column-name "' does not exist in table '" table-name "'")
                                         schema-source identification-method metabase-version))))
                                 expected-columns)))

                  ;; Table doesn't exist - skip column validation
                  acc)))
            []
            expected-tables)))

(defn normalize-data-type
  "Normalize database data types for comparison"
  [db-type]
  (let [type-str (str/lower-case (str db-type))]
    (cond
      (str/includes? type-str "varchar") "varchar"
      (str/includes? type-str "character varying") "varchar"
      (str/includes? type-str "text") "text"
      (str/includes? type-str "integer") "integer"
      (str/includes? type-str "bigint") "bigint"
      (str/includes? type-str "smallint") "smallint"
      (str/includes? type-str "boolean") "boolean"
      (str/includes? type-str "timestamp") "timestamp"
      (str/includes? type-str "date") "date"
      (str/includes? type-str "time") "time"
      (str/includes? type-str "decimal") "decimal"
      (str/includes? type-str "numeric") "numeric"
      (str/includes? type-str "real") "real"
      (str/includes? type-str "double") "double"
      (str/includes? type-str "bytea") "bytea"
      (str/includes? type-str "uuid") "uuid"
      (str/includes? type-str "json") "json"
      :else type-str)))

(defn validate-column-data-types
  "Validate that column data types match expectations"
  [conn-spec schema-def metabase-version]
  (let [expected-tables (:tables (:unified-schema schema-def))]

    (reduce (fn [acc [table-key table-def]]
              (let [table-name (:table-name table-def (name table-key))
                    expected-columns (:columns table-def {})
                    db-table-info (db-conn/get-table-info conn-spec table-name)]

                (if db-table-info
                  (let [db-columns (reduce (fn [col-acc col-info]
                                            (assoc col-acc
                                                   (keyword (str/lower-case (:column_name col-info)))
                                                   col-info))
                                          {}
                                          (:columns db-table-info))]

                    (concat acc
                            (map (fn [[col-key col-def]]
                                   (let [column-name (:column-name col-def (name col-key))
                                         expected-type (:data-type col-def)
                                         schema-source (:source col-def "unknown")
                                         identification-method (:identification-method col-def "unknown")
                                         db-col-info (get db-columns (keyword (str/lower-case column-name)))]

                                     (if db-col-info
                                       (let [actual-type (normalize-data-type (:data_type db-col-info))
                                             normalized-expected (normalize-data-type expected-type)]

                                         (if (or (= normalized-expected actual-type)
                                                (= expected-type "unknown"))  ; Skip validation for unknown types
                                           (create-validation-result
                                             table-name column-name true nil
                                             schema-source identification-method metabase-version)
                                           (create-validation-result
                                             table-name column-name false
                                             (str "Data type mismatch: expected '" expected-type "', found '" actual-type "'")
                                             schema-source identification-method metabase-version)))

                                       ;; Column doesn't exist - skip type validation
                                       (create-validation-result
                                         table-name column-name false
                                         (str "Column '" column-name "' does not exist - cannot validate data type")
                                         schema-source identification-method metabase-version))))
                                 expected-columns)))

                  ;; Table doesn't exist - skip validation
                  acc)))
            []
            expected-tables)))

(defn validate-nullable-constraints
  "Validate nullable constraints"
  [conn-spec schema-def metabase-version]
  (let [expected-tables (:tables (:unified-schema schema-def))]

    (reduce (fn [acc [table-key table-def]]
              (let [table-name (:table-name table-def (name table-key))
                    expected-columns (:columns table-def {})
                    db-table-info (db-conn/get-table-info conn-spec table-name)]

                (if db-table-info
                  (let [db-columns (reduce (fn [col-acc col-info]
                                            (assoc col-acc
                                                   (keyword (str/lower-case (:column_name col-info)))
                                                   col-info))
                                          {}
                                          (:columns db-table-info))]

                    (concat acc
                            (keep (fn [[col-key col-def]]
                                   (let [column-name (:column-name col-def (name col-key))
                                         expected-nullable (:nullable col-def)
                                         schema-source (:source col-def "unknown")
                                         identification-method (:identification-method col-def "unknown")
                                         db-col-info (get db-columns (keyword (str/lower-case column-name)))]

                                     ;; Only validate if we have explicit nullable information
                                     (when (and db-col-info (some? expected-nullable))
                                       (let [actual-nullable (= "YES" (:is_nullable db-col-info))]
                                         (if (= expected-nullable actual-nullable)
                                           (create-validation-result
                                             table-name column-name true nil
                                             schema-source identification-method metabase-version)
                                           (create-validation-result
                                             table-name column-name false
                                             (str "Nullable constraint mismatch: expected " expected-nullable ", found " actual-nullable)
                                             schema-source identification-method metabase-version))))))
                                  expected-columns)))

                  ;; Table doesn't exist - skip validation
                  acc)))
            []
            expected-tables)))

(defn validate-primary-key-constraints
  "Validate primary key constraints"
  [conn-spec schema-def metabase-version]
  (let [expected-tables (:tables (:unified-schema schema-def))]

    (reduce (fn [acc [table-key table-def]]
              (let [table-name (:table-name table-def (name table-key))
                    expected-columns (:columns table-def {})
                    db-table-info (db-conn/get-table-info conn-spec table-name)
                    expected-pk-columns (filter #(:primary-key (second %)) expected-columns)]

                (if (and db-table-info (seq expected-pk-columns))
                  (let [db-pk-constraints (filter #(= "PRIMARY KEY" (:constraint_type %))
                                                 (:constraints db-table-info))
                        db-pk-columns (set (map :column_name db-pk-constraints))]

                    (concat acc
                            (map (fn [[col-key col-def]]
                                   (let [column-name (:column-name col-def (name col-key))
                                         schema-source (:source col-def "unknown")
                                         identification-method (:identification-method col-def "unknown")]

                                     (if (contains? db-pk-columns column-name)
                                       (create-validation-result
                                         table-name column-name true nil
                                         schema-source identification-method metabase-version)
                                       (create-validation-result
                                         table-name column-name false
                                         (str "Primary key constraint missing for column '" column-name "'")
                                         schema-source identification-method metabase-version))))
                                 expected-pk-columns)))

                  ;; No expected primary keys or table doesn't exist
                  acc)))
            []
            expected-tables)))

(defn validate-structural
  "Main structural validation function"
  [config db-info schema-def]
  (let [conn-spec (:connection db-info)
        metabase-version (:metabase-version config "unknown")]

    (concat
      ;; Table existence validation
      (validate-table-existence conn-spec schema-def metabase-version)

      ;; Column existence validation
      (validate-column-existence conn-spec schema-def metabase-version)

      ;; Data type validation
      (validate-column-data-types conn-spec schema-def metabase-version)

      ;; Nullable constraint validation
      (validate-nullable-constraints conn-spec schema-def metabase-version)

      ;; Primary key constraint validation
      (validate-primary-key-constraints conn-spec schema-def metabase-version))))