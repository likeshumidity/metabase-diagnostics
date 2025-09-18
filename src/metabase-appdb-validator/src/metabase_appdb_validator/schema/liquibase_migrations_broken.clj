(ns metabase-appdb-validator.schema.liquibase-migrations
  "Extract schema definitions from Liquibase migration files"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [babashka.fs :as fs]
            [clj-yaml.core :as yaml]))

(defn find-migration-files
  "Find all Liquibase migration files"
  [repo-path]
  (let [migrations-path (fs/path repo-path "resources" "migrations")]
    (when (fs/exists? migrations-path)
      (->> (fs/glob migrations-path "*.yaml")
           (filter #(re-matches #".*\d+.*migrations\.yaml$" (str %)))
           (map str)
           (sort)))))

(defn parse-yaml-migration
  "Parse a Liquibase YAML migration file"
  [file-path]
  (try
    (let [content (slurp file-path)
          parsed (yaml/parse-string content)]
      {:file-path file-path
       :parsed-content parsed
       :changesets (get-in parsed ["databaseChangeLog"] [])})
    (catch Exception e
      (println "Warning: Failed to parse migration file" file-path ":" (.getMessage e))
      {:file-path file-path
       :error (.getMessage e)})))

(defn extract-create-table-changes
  "Extract createTable changes from changesets"
  [changesets]
  (reduce (fn [acc changeset]
            (let [changes (get changeset "changes" [])]
              (reduce (fn [inner-acc change]
                        (if-let [create-table (get change "createTable")]
                          (let [table-name (get create-table "tableName")
                                columns (get create-table "columns" [])]
                            (assoc inner-acc (keyword table-name)
                                   {:table-name table-name
                                    :columns (parse-liquibase-columns columns)
                                    :changeset-id (get changeset "id")
                                    :changeset-author (get changeset "author")
                                    :source "liquibase-migration"
                                    :identification-method "deterministic"}))
                          inner-acc))
                      acc
                      changes)))
          {}
          changesets))

(defn extract-add-column-changes
  "Extract addColumn changes from changesets"
  [changesets]
  (reduce (fn [acc changeset]
            (let [changes (get changeset "changes" [])]
              (reduce (fn [inner-acc change]
                        (if-let [add-column (get change "addColumn")]
                          (let [table-name (get add-column "tableName")
                                columns (get add-column "columns" [])]
                            (update-in inner-acc [(keyword table-name) :added-columns]
                                       (fnil concat [])
                                       (parse-liquibase-columns columns)))
                          inner-acc))
                      acc
                      changes)))
          {}
          changesets))

(defn extract-modify-column-changes
  "Extract modifyDataType and other column modification changes"
  [changesets]
  (reduce (fn [acc changeset]
            (let [changes (get changeset "changes" [])]
              (reduce (fn [inner-acc change]
                        (cond
                          ;; modifyDataType
                          (get change "modifyDataType")
                          (let [modify (get change "modifyDataType")
                                table-name (get modify "tableName")
                                column-name (get modify "columnName")
                                new-type (get modify "newDataType")]
                            (update-in inner-acc [(keyword table-name) :modified-columns]
                                       (fnil conj [])
                                       {:column-name column-name
                                        :new-data-type new-type
                                        :changeset-id (get changeset "id")
                                        :source "liquibase-migration"
                                        :identification-method "deterministic"}))

                          ;; addNotNullConstraint
                          (get change "addNotNullConstraint")
                          (let [constraint (get change "addNotNullConstraint")
                                table-name (get constraint "tableName")
                                column-name (get constraint "columnName")]
                            (update-in inner-acc [(keyword table-name) :constraints]
                                       (fnil conj [])
                                       {:type "not-null"
                                        :column-name column-name
                                        :changeset-id (get changeset "id")
                                        :source "liquibase-migration"
                                        :identification-method "deterministic"}))

                          :else inner-acc))
                      acc
                      changes)))
          {}
          changesets))

(defn extract-constraint-changes
  "Extract constraint-related changes (primary keys, foreign keys, unique, etc.)"
  [changesets]
  (reduce (fn [acc changeset]
            (let [changes (get changeset "changes" [])]
              (reduce (fn [inner-acc change]
                        (cond
                          ;; addPrimaryKey
                          (get change "addPrimaryKey")
                          (let [pk (get change "addPrimaryKey")
                                table-name (get pk "tableName")
                                column-names (get pk "columnNames")]
                            (update-in inner-acc [(keyword table-name) :constraints]
                                       (fnil conj [])
                                       {:type "primary-key"
                                        :columns (str/split column-names #",")
                                        :constraint-name (get pk "constraintName")
                                        :changeset-id (get changeset "id")
                                        :source "liquibase-migration"
                                        :identification-method "deterministic"}))

                          ;; addForeignKeyConstraint
                          (get change "addForeignKeyConstraint")
                          (let [fk (get change "addForeignKeyConstraint")
                                table-name (get fk "baseTableName")]
                            (update-in inner-acc [(keyword table-name) :constraints]
                                       (fnil conj [])
                                       {:type "foreign-key"
                                        :base-column (get fk "baseColumnNames")
                                        :referenced-table (get fk "referencedTableName")
                                        :referenced-column (get fk "referencedColumnNames")
                                        :constraint-name (get fk "constraintName")
                                        :changeset-id (get changeset "id")
                                        :source "liquibase-migration"
                                        :identification-method "deterministic"}))

                          ;; addUniqueConstraint
                          (get change "addUniqueConstraint")
                          (let [unique (get change "addUniqueConstraint")
                                table-name (get unique "tableName")]
                            (update-in inner-acc [(keyword table-name) :constraints]
                                       (fnil conj [])
                                       {:type "unique"
                                        :columns (str/split (get unique "columnNames") #",")
                                        :constraint-name (get unique "constraintName")
                                        :changeset-id (get changeset "id")
                                        :source "liquibase-migration"
                                        :identification-method "deterministic"}))

                          :else inner-acc))
                      acc
                      changes)))
          {}
          changesets))

(defn parse-liquibase-columns
  "Parse column definitions from Liquibase format"
  [columns]
  (reduce (fn [acc column]
            (let [column-name (get column "name")
                  column-type (get column "type")
                  constraints (get column "constraints" {})]
              (assoc acc (keyword column-name)
                     {:column-name column-name
                      :data-type column-type
                      :nullable (not (get constraints "nullable" true))
                      :primary-key (get constraints "primaryKey" false)
                      :unique (get constraints "unique" false)
                      :default-value (get column "defaultValue")
                      :auto-increment (get constraints "autoIncrement" false)
                      :source "liquibase-migration"
                      :identification-method "deterministic"})))
          {}
          columns))

(defn extract-index-changes
  "Extract index creation and modification changes"
  [changesets]
  (reduce (fn [acc changeset]
            (let [changes (get changeset "changes" [])]
              (reduce (fn [inner-acc change]
                        (if-let [create-index (get change "createIndex")]
                          (let [table-name (get create-index "tableName")
                                index-name (get create-index "indexName")
                                columns (get create-index "columns" [])]
                            (update-in inner-acc [(keyword table-name) :indexes]
                                       (fnil conj [])
                                       {:index-name index-name
                                        :columns (map #(get % "name") columns)
                                        :unique (get create-index "unique" false)
                                        :changeset-id (get changeset "id")
                                        :source "liquibase-migration"
                                        :identification-method "deterministic"}))
                          inner-acc))
                      acc
                      changes)))
          {}
          changesets))

(defn merge-migration-changes
  "Merge different types of migration changes into comprehensive table schemas"
  [create-tables add-columns modify-columns constraints indexes]
  (let [all-table-keys (distinct (concat (keys create-tables)
                                        (keys add-columns)
                                        (keys modify-columns)
                                        (keys constraints)
                                        (keys indexes)))]
    (reduce (fn [acc table-key]
              (let [base-table (get create-tables table-key)
                    added-cols (get-in add-columns [table-key :added-columns] [])
                    modified-cols (get-in modify-columns [table-key :modified-columns] [])
                    table-constraints (get-in constraints [table-key :constraints] [])
                    table-indexes (get-in indexes [table-key :indexes] [])]

                (assoc acc table-key
                       (merge base-table
                              {:added-columns added-cols
                               :modified-columns modified-cols
                               :constraints table-constraints
                               :indexes table-indexes}))))
            {}
            all-table-keys)))

(defn analyze-migration-file
  "Analyze a single migration file"
  [file-path]
  (let [parsed (parse-yaml-migration file-path)]
    (if (:error parsed)
      parsed
      (let [changesets (:changesets parsed)
            create-tables (extract-create-table-changes changesets)
            add-columns (extract-add-column-changes changesets)
            modify-columns (extract-modify-column-changes changesets)
            constraints (extract-constraint-changes changesets)
            indexes (extract-index-changes changesets)]

        {:file-path file-path
         :changeset-count (count changesets)
         :create-tables create-tables
         :add-columns add-columns
         :modify-columns modify-columns
         :constraints constraints
         :indexes indexes}))))

(defn merge-migration-analysis
  "Merge analysis from multiple migration files"
  [analyses]
  (let [valid-analyses (filter #(not (:error %)) analyses)
        error-analyses (filter :error analyses)]

    (reduce (fn [acc analysis]
              (-> acc
                  (update :create-tables merge (:create-tables analysis))
                  (update :add-columns merge (:add-columns analysis))
                  (update :modify-columns merge (:modify-columns analysis))
                  (update :constraints merge (:constraints analysis))
                  (update :indexes merge (:indexes analysis))
                  (update :analyzed-files conj (:file-path analysis))
                  (update :total-changesets + (:changeset-count analysis))))
            {:create-tables {}
             :add-columns {}
             :modify-columns {}
             :constraints {}
             :indexes {}
             :analyzed-files []
             :total-changesets 0
             :errors error-analyses}
            valid-analyses)))

(defn convert-to-table-schema
  "Convert Liquibase migration analysis to table schema format"
  [merged-analysis]
  (let [merged-tables (merge-migration-changes
                        (:create-tables merged-analysis)
                        (:add-columns merged-analysis)
                        (:modify-columns merged-analysis)
                        (:constraints merged-analysis)
                        (:indexes merged-analysis))]

    {:tables merged-tables
     :metadata {:source "liquibase-migrations"
                :analyzed-files (:analyzed-files merged-analysis)
                :total-changesets (:total-changesets merged-analysis)
                :table-count (count merged-tables)
                :errors (:errors merged-analysis)}}))

(defn extract-liquibase-schema
  "Main entry point for extracting Liquibase migration schemas"
  [repo-path]
  (try
    (let [migration-files (find-migration-files repo-path)
          analyses (map analyze-migration-file migration-files)
          merged-analysis (merge-migration-analysis analyses)]

      (convert-to-table-schema merged-analysis))

    (catch Exception e
      (println "Warning: Failed to extract Liquibase schema from" repo-path ":" (.getMessage e))
      {:tables {}
       :metadata {:source "liquibase-migrations"
                  :error (.getMessage e)
                  :extraction-failed true}})))