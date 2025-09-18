(ns metabase-appdb-validator.schema.sql-init-scripts
  "Extract schema definitions from SQL initialization scripts"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(defn find-sql-init-files
  "Find SQL initialization scripts"
  [repo-path]
  (let [migrations-path (io/file repo-path "resources" "migrations" "initialization")]
    (if (.exists migrations-path)
      (->> (file-seq migrations-path)
           (filter #(and (.isFile %) (str/ends-with? (.getName %) ".sql")))
           (filter #(str/includes? (.getName %) "postgres"))  ; Focus on PostgreSQL
           (map #(.getAbsolutePath %)))
      [])))

(defn parse-sql-statements
  "Parse SQL DDL statements from SQL content"
  [sql-content]
  (let [;; Split by semicolons but be careful with quoted strings
        statements (->> (str/split sql-content #";")
                       (map str/trim)
                       (filter #(not (str/blank? %)))
                       (filter #(not (str/starts-with? % "--"))))]
    statements))

(defn extract-create-table-statements
  "Extract CREATE TABLE statements"
  [statements]
  (filter #(re-matches #"(?i)^\s*CREATE\s+TABLE.*" %) statements))

(defn split-column-lines
  "Split column section into individual column/constraint lines"
  [column-section]
  (let [lines (str/split column-section #",")
        ;; Simple approach - in production would need more sophisticated parsing
        ;; to handle nested parentheses in CHECK constraints, etc.
        cleaned-lines (map str/trim lines)]
    (filter #(not (str/blank? %)) cleaned-lines)))

(defn parse-postgresql-type
  "Parse PostgreSQL data type and normalize it"
  [type-str]
  (let [type-upper (str/upper-case (str/trim type-str))]
    (cond
      (str/starts-with? type-upper "VARCHAR") "varchar"
      (str/starts-with? type-upper "TEXT") "text"
      (str/starts-with? type-upper "INTEGER") "integer"
      (str/starts-with? type-upper "BIGINT") "bigint"
      (str/starts-with? type-upper "SMALLINT") "smallint"
      (str/starts-with? type-upper "BOOLEAN") "boolean"
      (str/starts-with? type-upper "TIMESTAMP") "timestamp"
      (str/starts-with? type-upper "DATE") "date"
      (str/starts-with? type-upper "TIME") "time"
      (str/starts-with? type-upper "DECIMAL") "decimal"
      (str/starts-with? type-upper "NUMERIC") "numeric"
      (str/starts-with? type-upper "REAL") "real"
      (str/starts-with? type-upper "DOUBLE") "double"
      (str/starts-with? type-upper "BYTEA") "bytea"
      (str/starts-with? type-upper "UUID") "uuid"
      (str/starts-with? type-upper "JSON") "json"
      (str/starts-with? type-upper "JSONB") "jsonb"
      :else (str/lower-case type-str))))

(defn extract-default-value
  "Extract default value from constraint tokens"
  [constraints]
  (let [constraint-str (str/join " " constraints)
        default-pattern #"(?i)DEFAULT\s+([^,\s]+)"
        match (re-find default-pattern constraint-str)]
    (when match
      (second match))))

(defn parse-single-column-definition
  "Parse a single column definition line"
  [line]
  (let [line (str/trim line)]
    (cond
      ;; Skip constraint definitions
      (re-matches #"(?i)^\s*(?:CONSTRAINT|PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE|CHECK).*" line)
      nil

      ;; Parse column definition
      :else
      (let [;; Basic pattern: column_name data_type [constraints...]
            parts (str/split line #"\s+")
            column-name (first parts)
            data-type (second parts)
            constraints (drop 2 parts)]

        (when (and column-name data-type)
          {:column-name (str/trim column-name "\"")
           :data-type (parse-postgresql-type data-type)
           :nullable (not (some #(= (str/upper-case %) "NOT NULL")
                                (map str/join (partition 2 1 constraints))))
           :primary-key (some #(str/includes? (str/upper-case %) "PRIMARY KEY") constraints)
           :unique (some #(str/includes? (str/upper-case %) "UNIQUE") constraints)
           :default-value (extract-default-value constraints)
           :source "sql-init"
           :identification-method "deterministic"})))))

(defn parse-column-definitions
  "Parse column definitions from CREATE TABLE statement"
  [column-section]
  (let [;; Split by commas, but be careful with function calls and constraints
        column-lines (split-column-lines column-section)]

    (reduce (fn [acc line]
              (let [parsed-col (parse-single-column-definition line)]
                (if parsed-col
                  (assoc acc (keyword (:column-name parsed-col)) parsed-col)
                  acc)))
            {}
            column-lines)))

(defn parse-create-table
  "Parse a CREATE TABLE statement"
  [statement]
  (try
    (let [;; Extract table name
          table-name-pattern #"(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\"?(\w+)\"?|\`?(\w+)\`?)"
          table-match (re-find table-name-pattern statement)
          table-name (or (nth table-match 1) (nth table-match 2))

          ;; Extract column definitions between parentheses
          column-section-pattern #"(?s)\(\s*(.*?)\s*\)(?:\s*;)?\s*$"
          column-section (second (re-find column-section-pattern statement))

          ;; Parse individual column definitions
          columns (when column-section
                    (parse-column-definitions column-section))]

      (when (and table-name columns)
        {:table-name table-name
         :columns columns
         :source "sql-init"
         :identification-method "deterministic"
         :sql-statement statement}))

    (catch Exception e
      (println "Warning: Failed to parse CREATE TABLE statement:" (.getMessage e))
      nil)))

(defn extract-table-constraints
  "Extract table-level constraints from CREATE TABLE statements"
  [statements]
  (reduce (fn [acc statement]
            (let [table-name-pattern #"(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\"?(\w+)\"?|\`?(\w+)\`?)"
                  table-match (re-find table-name-pattern statement)
                  table-name (or (nth table-match 1) (nth table-match 2))

                  ;; Extract constraints from the statement
                  constraint-patterns {:primary-key #"(?i)PRIMARY\s+KEY\s*\(\s*([^)]+)\s*\)"
                                      :foreign-key #"(?i)FOREIGN\s+KEY\s*\(\s*([^)]+)\s*\)\s+REFERENCES\s+(\w+)\s*\(\s*([^)]+)\s*\)"
                                      :unique #"(?i)UNIQUE\s*\(\s*([^)]+)\s*\)"
                                      :check #"(?i)CHECK\s*\(\s*([^)]+)\s*\)"}

                  constraints (reduce (fn [inner-acc [constraint-type pattern]]
                                       (let [matches (re-seq pattern statement)]
                                         (if (seq matches)
                                           (assoc inner-acc constraint-type matches)
                                           inner-acc)))
                                     {}
                                     constraint-patterns)]

              (if (and table-name (seq constraints))
                (assoc acc (keyword table-name) constraints)
                acc)))
          {}
          statements))

(defn analyze-sql-init-file
  "Analyze a single SQL initialization file"
  [file-path]
  (try
    (let [content (slurp file-path)
          statements (parse-sql-statements content)
          create-table-stmts (extract-create-table-statements statements)
          tables (keep parse-create-table create-table-stmts)
          table-constraints (extract-table-constraints create-table-stmts)]

      {:file-path file-path
       :statement-count (count statements)
       :create-table-count (count create-table-stmts)
       :tables (reduce (fn [acc table]
                        (assoc acc (keyword (:table-name table)) table))
                      {}
                      tables)
       :table-constraints table-constraints})

    (catch Exception e
      (println "Warning: Failed to analyze SQL file" file-path ":" (.getMessage e))
      {:file-path file-path
       :error (.getMessage e)})))

(defn merge-sql-analysis
  "Merge analysis from multiple SQL files"
  [analyses]
  (let [valid-analyses (filter #(not (:error %)) analyses)
        error-analyses (filter :error analyses)]

    (reduce (fn [acc analysis]
              (-> acc
                  (update :tables merge (:tables analysis))
                  (update :table-constraints merge (:table-constraints analysis))
                  (update :analyzed-files conj (:file-path analysis))
                  (update :total-statements + (:statement-count analysis 0))
                  (update :total-create-tables + (:create-table-count analysis 0))))
            {:tables {}
             :table-constraints {}
             :analyzed-files []
             :total-statements 0
             :total-create-tables 0
             :errors error-analyses}
            valid-analyses)))

(defn convert-to-table-schema
  "Convert SQL analysis to table schema format"
  [merged-analysis]
  (let [tables (:tables merged-analysis)
        constraints (:table-constraints merged-analysis)

        ;; Merge table-level constraints into table definitions
        enhanced-tables (reduce (fn [acc [table-key table-def]]
                                 (let [table-constraints (get constraints table-key {})]
                                   (assoc acc table-key
                                          (assoc table-def :table-constraints table-constraints))))
                               {}
                               tables)]

    {:tables enhanced-tables
     :metadata {:source "sql-init-scripts"
                :analyzed-files (:analyzed-files merged-analysis)
                :total-statements (:total-statements merged-analysis)
                :total-create-tables (:total-create-tables merged-analysis)
                :table-count (count enhanced-tables)
                :errors (:errors merged-analysis)}}))

(defn extract-sql-init-schema
  "Main entry point for extracting SQL initialization schemas"
  [repo-path]
  (try
    (let [sql-files (find-sql-init-files repo-path)]
      (if (seq sql-files)
        (let [analyses (map analyze-sql-init-file sql-files)
              merged-analysis (merge-sql-analysis analyses)]
          (convert-to-table-schema merged-analysis))
        {:tables {}
         :metadata {:source "sql-init-scripts"
                    :message "No PostgreSQL initialization scripts found"
                    :table-count 0}}))

    (catch Exception e
      (println "Warning: Failed to extract SQL init schema from" repo-path ":" (.getMessage e))
      {:tables {}
       :metadata {:source "sql-init-scripts"
                  :error (.getMessage e)
                  :extraction-failed true}})))