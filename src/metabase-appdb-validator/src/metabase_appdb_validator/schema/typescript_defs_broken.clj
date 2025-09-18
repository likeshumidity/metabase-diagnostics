(ns metabase-appdb-validator.schema.typescript-defs
  "Extract schema definitions from TypeScript definition files"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [babashka.fs :as fs]
            [cheshire.core :as json]))

(defn find-typescript-files
  "Find TypeScript definition files in the frontend source"
  [repo-path]
  (let [frontend-path (fs/path repo-path "frontend" "src")
        types-path (fs/path repo-path "frontend" "src" "metabase-types")]
    (concat
      ;; Look for .d.ts files
      (when (fs/exists? types-path)
        (->> (fs/glob types-path "**/*.d.ts")
             (map str)))
      ;; Look for TypeScript files with type definitions
      (when (fs/exists? frontend-path)
        (->> (fs/glob frontend-path "**/*.ts")
             (filter #(or (str/includes? (str %) "types")
                         (str/includes? (str %) "schema")))
             (map str))))))

(defn extract-interface-properties
  "Extract properties from interface body"
  [interface-body]
  (let [property-pattern #"(\w+)(\??):\s*([^;,\n]+)"
        matches (re-seq property-pattern interface-body)]
    (reduce (fn [acc [_ prop-name optional type-def]]
              (assoc acc (keyword prop-name)
                     {:name prop-name
                      :type (str/trim type-def)
                      :optional (= optional "?")
                      :source "typescript-def"
                      :identification-method "deterministic"}))
            {}
            matches)))

(defn extract-interface-definitions
  "Extract interface definitions from TypeScript content"
  [content]
  (let [interface-pattern #"(?s)interface\s+(\w+)\s*\{([^}]+)\}"
        matches (re-seq interface-pattern content)]
    (reduce (fn [acc [_ interface-name interface-body]]
              (let [properties (extract-interface-properties interface-body)]
                (assoc acc (keyword interface-name)
                       {:name interface-name
                        :properties properties
                        :source "typescript-def"
                        :identification-method "deterministic"})))
            {}
            matches)))

(defn extract-type-aliases
  "Extract type alias definitions"
  [content]
  (let [type-pattern #"type\s+(\w+)\s*=\s*([^;]+);"
        matches (re-seq type-pattern content)]
    (reduce (fn [acc [_ type-name type-def]]
              (assoc acc (keyword type-name)
                     {:name type-name
                      :definition (str/trim type-def)
                      :source "typescript-def"
                      :identification-method "deterministic"}))
            {}
            matches)))

(defn extract-enum-definitions
  "Extract enum definitions"
  [content]
  (let [enum-pattern #"(?s)enum\s+(\w+)\s*\{([^}]+)\}"
        matches (re-seq enum-pattern content)]
    (reduce (fn [acc [_ enum-name enum-body]]
              (let [values (extract-enum-values enum-body)]
                (assoc acc (keyword enum-name)
                       {:name enum-name
                        :values values
                        :source "typescript-def"
                        :identification-method "deterministic"})))
            {}
            matches)))

(defn extract-enum-values
  "Extract enum values from enum body"
  [enum-body]
  (->> (str/split enum-body #",")
       (map str/trim)
       (filter #(not (str/blank? %)))
       (map #(first (str/split % #"=")))
       (map str/trim)
       (remove str/blank?)))

(defn map-typescript-to-database
  "Map TypeScript types to database schema concepts"
  [ts-definitions]
  (let [interfaces (:interfaces ts-definitions)
        type-aliases (:type-aliases ts-definitions)
        enums (:enums ts-definitions)]

    ;; Look for interfaces that might represent database entities
    (reduce (fn [acc [interface-key interface-def]]
              (let [interface-name (:name interface-def)
                    properties (:properties interface-def)]

                ;; Heuristic: interfaces with 'id' property might be database entities
                (if (contains? properties :id)
                  (let [table-name (str/lower-case (str/replace interface-name #"(?<!^)([A-Z])" "_$1"))
                        columns (map-properties-to-columns properties)]
                    (assoc acc (keyword table-name)
                           {:table-name table-name
                            :typescript-interface interface-name
                            :columns columns
                            :source "typescript-def"
                            :identification-method "heuristic"}))
                  acc)))
            {}
            interfaces)))

(defn map-properties-to-columns
  "Map TypeScript interface properties to database columns"
  [properties]
  (reduce (fn [acc [prop-key prop-def]]
            (let [prop-name (:name prop-def)
                  ts-type (:type prop-def)
                  db-type (typescript-to-db-type ts-type)
                  nullable (:optional prop-def)]

              (assoc acc prop-key
                     {:column-name prop-name
                      :data-type db-type
                      :typescript-type ts-type
                      :nullable nullable
                      :source "typescript-def"
                      :identification-method "deterministic"})))
          {}
          properties))

(defn typescript-to-db-type
  "Convert TypeScript types to database types"
  [ts-type]
  (cond
    (str/includes? ts-type "string") "text"
    (str/includes? ts-type "number") "integer"
    (str/includes? ts-type "boolean") "boolean"
    (str/includes? ts-type "Date") "timestamp"
    (str/includes? ts-type "[]") "array"
    (str/includes? ts-type "Record") "json"
    (str/includes? ts-type "object") "json"
    :else "text"))

(defn extract-visualization-settings
  "Extract visualization settings schemas from TypeScript files"
  [content]
  (let [viz-settings-pattern #"(?s)(?:VizSettings|VisualizationSettings|ChartSettings)\s*[=:]\s*\{([^}]+)\}"
        matches (re-seq viz-settings-pattern content)]
    (map (fn [[_ settings-body]]
           {:type "visualization-settings"
            :definition settings-body
            :source "typescript-def"
            :identification-method "deterministic"})
         matches)))

(defn analyze-typescript-file
  "Analyze a single TypeScript file for schema definitions"
  [file-path]
  (try
    (let [content (slurp file-path)
          interfaces (extract-interface-definitions content)
          type-aliases (extract-type-aliases content)
          enums (extract-enum-definitions content)
          viz-settings (extract-visualization-settings content)]

      {:file-path file-path
       :interfaces interfaces
       :type-aliases type-aliases
       :enums enums
       :visualization-settings viz-settings})

    (catch Exception e
      (println "Warning: Failed to analyze TypeScript file" file-path ":" (.getMessage e))
      {:file-path file-path
       :error (.getMessage e)})))

(defn merge-typescript-analysis
  "Merge analysis from multiple TypeScript files"
  [analyses]
  (reduce (fn [acc analysis]
            (if (:error analysis)
              (update acc :errors conj analysis)
              (-> acc
                  (update :interfaces merge (:interfaces analysis))
                  (update :type-aliases merge (:type-aliases analysis))
                  (update :enums merge (:enums analysis))
                  (update :visualization-settings concat (:visualization-settings analysis))
                  (update :analyzed-files conj (:file-path analysis)))))
          {:interfaces {}
           :type-aliases {}
           :enums {}
           :visualization-settings []
           :analyzed-files []
           :errors []}
          analyses))

(defn convert-to-table-schema
  "Convert TypeScript definitions to table schema format"
  [merged-analysis]
  (let [database-tables (map-typescript-to-database merged-analysis)]

    {:tables database-tables
     :metadata {:source "typescript-definitions"
                :analyzed-files (:analyzed-files merged-analysis)
                :interfaces (count (:interfaces merged-analysis))
                :type-aliases (count (:type-aliases merged-analysis))
                :enums (count (:enums merged-analysis))
                :visualization-settings (count (:visualization-settings merged-analysis))
                :errors (:errors merged-analysis)}}))

(defn extract-typescript-definitions
  "Main entry point for extracting TypeScript definition schemas"
  [repo-path]
  (try
    (let [ts-files (find-typescript-files repo-path)
          analyses (map analyze-typescript-file ts-files)
          merged-analysis (merge-typescript-analysis analyses)]

      (convert-to-table-schema merged-analysis))

    (catch Exception e
      (println "Warning: Failed to extract TypeScript definitions from" repo-path ":" (.getMessage e))
      {:tables {}
       :metadata {:source "typescript-definitions"
                  :error (.getMessage e)
                  :extraction-failed true}})))