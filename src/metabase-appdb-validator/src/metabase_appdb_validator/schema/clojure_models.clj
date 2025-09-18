(ns metabase-appdb-validator.schema.clojure-models
  "Extract schema definitions from Clojure model files"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [rewrite-clj.zip :as z]
            [rewrite-clj.node :as n]
))

(defn find-model-files
  "Find all Clojure model files in the Metabase source"
  [repo-path]
  (let [src-path (io/file repo-path "src")
        models-path (io/file repo-path "src" "metabase" "models")]
    (if (.exists models-path)
      (->> (file-seq models-path)
           (filter #(and (.isFile %) (str/ends-with? (.getName %) ".clj")))
           (map #(.getAbsolutePath %))
           (sort))
      [])))

(defn parse-clojure-file
  "Parse a Clojure file and extract relevant forms"
  [file-path]
  (try
    (let [content (slurp file-path)
          zloc (z/of-string content)]
      (loop [loc zloc
             forms []]
        (if (z/end? loc)
          forms
          (let [node (z/node loc)]
            (if (and (= :list (n/tag node))
                     (let [first-child (first (n/children node))]
                       (and (= :token (n/tag first-child))
                            (contains? #{'defmodel 'deftable 't2/deftransforms 'methodical/defmethod}
                                      (n/sexpr first-child)))))
              (recur (z/next loc) (conj forms (z/sexpr loc)))
              (recur (z/next loc) forms))))))
    (catch Exception e
      (println "Warning: Failed to parse" file-path ":" (.getMessage e))
      [])))

(defn extract-table-name
  "Extract table name from model definitions"
  [forms]
  (reduce (fn [acc form]
            (cond
              ;; (methodical/defmethod t2/table-name :model/ApiKey [_model] :api_key)
              (and (list? form)
                   (= 'methodical/defmethod (first form))
                   (= 't2/table-name (second form)))
              (let [model-keyword (nth form 2 nil)
                    table-name (last form)]
                (if (and model-keyword table-name)
                  (assoc acc model-keyword table-name)
                  acc))

              ;; (defmodel Database :metabase_database ...)
              (and (list? form)
                   (= 'defmodel (first form)))
              (let [model-name (nth form 1 nil)
                    table-name (nth form 2 nil)]
                (if (and model-name table-name)
                  (assoc acc (keyword (str "model/" (name model-name))) table-name)
                  acc))

              :else acc))
          {}
          forms))

(defn extract-model-validations
  "Extract validation rules from model definitions"
  [forms]
  (reduce (fn [acc form]
            (cond
              ;; (t2/deftransforms :model/Database {:details mi/transform-encrypted-json})
              (and (list? form)
                   (= 't2/deftransforms (first form)))
              (let [model-keyword (nth form 1 nil)
                    transforms (nth form 2 nil)]
                (if (and model-keyword transforms)
                  (assoc acc model-keyword {:transforms transforms
                                           :source "clojure-model"
                                           :identification-method "deterministic"})
                  acc))

              :else acc))
          {}
          forms))

(defn extract-model-relationships
  "Extract relationship definitions from model forms"
  [forms]
  (reduce (fn [acc form]
            (when (and (list? form)
                       (or (str/includes? (str form) "belongs-to")
                           (str/includes? (str form) "has-many")
                           (str/includes? (str form) "has-one")))
              ;; This is a simplified extraction - would need more sophisticated parsing
              ;; for complete relationship mapping
              (conj acc {:type "relationship"
                        :form (take 3 form)
                        :source "clojure-model"
                        :identification-method "heuristic"}))
            acc)
          []
          forms))

(defn extract-mbql-schemas
  "Extract MBQL and query object schemas from model files"
  [forms]
  (reduce (fn [acc form]
            (when (and (list? form)
                       (or (str/includes? (str form) "mbql")
                           (str/includes? (str form) "query")
                           (str/includes? (str form) "field")
                           (str/includes? (str form) "aggregation")))
              ;; Look for schema definitions in MBQL-related forms
              (conj acc {:type "mbql-schema"
                        :definition form
                        :source "clojure-model"
                        :identification-method "deterministic"}))
            acc)
          []
          forms))

(defn analyze-model-file
  "Analyze a single model file and extract schema information"
  [file-path]
  (let [forms (parse-clojure-file file-path)
        table-names (extract-table-name forms)
        validations (extract-model-validations forms)
        relationships (extract-model-relationships forms)
        mbql-schemas (extract-mbql-schemas forms)]

    {:file-path file-path
     :table-names table-names
     :validations validations
     :relationships relationships
     :mbql-schemas mbql-schemas
     :forms-count (count forms)}))

(defn merge-model-analysis
  "Merge analysis from multiple model files"
  [analyses]
  (reduce (fn [acc analysis]
            (-> acc
                (update :table-names merge (:table-names analysis))
                (update :validations merge (:validations analysis))
                (update :relationships concat (:relationships analysis))
                (update :mbql-schemas concat (:mbql-schemas analysis))
                (update :analyzed-files conj (:file-path analysis))))
          {:table-names {}
           :validations {}
           :relationships []
           :mbql-schemas []
           :analyzed-files []}
          analyses))

(defn extract-column-info-from-transforms
  "Extract column information from transform definitions"
  [transforms]
  (reduce (fn [acc [column-key transform-fn]]
            (let [column-name (name column-key)
                  column-type (cond
                                (str/includes? (str transform-fn) "json") "json"
                                (str/includes? (str transform-fn) "keyword") "keyword"
                                (str/includes? (str transform-fn) "encrypt") "encrypted"
                                (str/includes? (str transform-fn) "boolean") "boolean"
                                (str/includes? (str transform-fn) "timestamp") "timestamp"
                                :else "unknown")]

              (assoc acc (keyword column-name)
                     {:column-name column-name
                      :data-type column-type
                      :transform transform-fn
                      :source "clojure-model"
                      :identification-method "deterministic"})))
          {}
          transforms))

(defn convert-to-table-schema
  "Convert extracted model information to table schema format"
  [merged-analysis]
  (let [table-names (:table-names merged-analysis)
        validations (:validations merged-analysis)]

    {:tables
     (reduce (fn [acc [model-keyword table-name]]
               (let [table-key (keyword (name table-name))
                     validation-info (get validations model-keyword {})
                     transforms (:transforms validation-info {})]

                 (assoc acc table-key
                        {:table-name (name table-name)
                         :model-keyword model-keyword
                         :columns (extract-column-info-from-transforms transforms)
                         :validations validation-info
                         :source "clojure-model"
                         :identification-method "deterministic"})))
             {}
             table-names)

     :metadata {:source "clojure-models"
                :analyzed-files (:analyzed-files merged-analysis)
                :table-count (count table-names)
                :relationships (:relationships merged-analysis)
                :mbql-schemas (:mbql-schemas merged-analysis)}}))

(defn extract-clojure-models
  "Main entry point for extracting Clojure model schemas"
  [repo-path]
  (try
    (let [model-files (find-model-files repo-path)
          analyses (map analyze-model-file model-files)
          merged-analysis (merge-model-analysis analyses)]

      (convert-to-table-schema merged-analysis))

    (catch Exception e
      (println "Warning: Failed to extract Clojure models from" repo-path ":" (.getMessage e))
      {:tables {}
       :metadata {:source "clojure-models"
                  :error (.getMessage e)
                  :extraction-failed true}})))