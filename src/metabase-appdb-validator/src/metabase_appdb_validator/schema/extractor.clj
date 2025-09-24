(ns metabase-appdb-validator.schema.extractor
  "Schema extraction from Metabase codebase across versions"
  (:require [clojure.java.shell :as shell]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [metabase-appdb-validator.schema.clojure-models :as clj-models]
            [metabase-appdb-validator.schema.typescript-defs :as ts-defs]
            [metabase-appdb-validator.schema.liquibase-migrations :as liquibase]
            [metabase-appdb-validator.schema.sql-init-scripts :as sql-init]))

(def METABASE-VERSION-PATTERN #"^v(\d+)\.(\d+)\.(\d+)\.(\d+)$")

(defn get-git-tags
  "Get all git tags from Metabase repository"
  [repo-path]
  (let [result (shell/sh "git" "tag" "--list" :dir repo-path)]
    (when (zero? (:exit result))
      (str/split-lines (:out result)))))

(defn filter-major-versions
  "Filter tags to get major version initials (license.major.minor.0) and latest patches"
  [tags]
  (let [version-pattern METABASE-VERSION-PATTERN
        parse-version (fn [tag]
                        (when-let [match (re-matches version-pattern tag)]
                          {:tag tag
                           :license (Integer/parseInt (nth match 1))
                           :major (Integer/parseInt (nth match 2))
                           :minor (Integer/parseInt (nth match 3))
                           :patch (Integer/parseInt (nth match 4))}))

        parsed-versions (->> tags
                            (map parse-version)
                            (filter some?)
                            (sort-by (juxt :license :major :minor :patch)))

        ;; Get major version initials (license.major.minor.0)
        major-initials (->> parsed-versions
                           (filter #(= 0 (:patch %)))
                           (map :tag))

        ;; Get latest patch for each license.major.minor
        latest-patches (->> parsed-versions
                           (group-by (juxt :license :major :minor))
                           (map (fn [[_ versions]]
                                  (->> versions
                                       (sort-by :patch)
                                       last
                                       :tag))))]

    (distinct (concat major-initials latest-patches))))

(defn list-supported-versions
  "List all supported Metabase versions for schema extraction"
  [repo-path]
  (let [all-tags (get-git-tags repo-path)]
    ;; Return all version tags that start with v0 or v1 and end with a digit
    ;; This matches the same logic as the list-versions command
    (->> all-tags
         (filter #(re-matches #"^v[01].*\d$" %))
         (sort))))

(defn checkout-version
  "Checkout specific git version in repository"
  [repo-path version]
  (let [result (shell/sh "git" "checkout" version :dir repo-path)]
    (when-not (zero? (:exit result))
      (throw (ex-info (str "Failed to checkout version " version)
                      {:version version
                       :error (:err result)
                       :exit-code (:exit result)})))))

(defn merge-schema-sources
  "Merge schema definitions from multiple sources with priority order"
  [clojure-schema typescript-schema liquibase-schema sql-init-schema]

  ;; Priority order: Clojure models > TypeScript defs > Liquibase > SQL init
  (let [all-tables (distinct (concat
                               (keys (:tables clojure-schema {}))
                               (keys (:tables typescript-schema {}))
                               (keys (:tables liquibase-schema {}))
                               (keys (:tables sql-init-schema {}))))]

    {:tables
     (reduce (fn [acc table-name]
               (let [;; Get table definition from each source
                     clj-table (get-in clojure-schema [:tables table-name])
                     ts-table (get-in typescript-schema [:tables table-name])
                     liquibase-table (get-in liquibase-schema [:tables table-name])
                     sql-table (get-in sql-init-schema [:tables table-name])

                     ;; Merge with priority
                     merged-table (merge
                                    (when sql-table
                                      (assoc sql-table :source "sql-init" :identification-method "deterministic"))
                                    (when liquibase-table
                                      (assoc liquibase-table :source "liquibase-migration" :identification-method "deterministic"))
                                    (when ts-table
                                      (assoc ts-table :source "typescript-def" :identification-method "deterministic"))
                                    (when clj-table
                                      (assoc clj-table :source "clojure-model" :identification-method "deterministic")))

                     ;; Merge column definitions with same priority
                     all-columns (distinct (concat
                                             (keys (:columns clj-table {}))
                                             (keys (:columns ts-table {}))
                                             (keys (:columns liquibase-table {}))
                                             (keys (:columns sql-table {}))))

                     merged-columns (reduce (fn [col-acc col-name]
                                              (let [clj-col (get-in clj-table [:columns col-name])
                                                    ts-col (get-in ts-table [:columns col-name])
                                                    liquibase-col (get-in liquibase-table [:columns col-name])
                                                    sql-col (get-in sql-table [:columns col-name])

                                                    merged-col (merge
                                                                 (when sql-col
                                                                   (assoc sql-col :source "sql-init" :identification-method "deterministic"))
                                                                 (when liquibase-col
                                                                   (assoc liquibase-col :source "liquibase-migration" :identification-method "deterministic"))
                                                                 (when ts-col
                                                                   (assoc ts-col :source "typescript-def" :identification-method "deterministic"))
                                                                 (when clj-col
                                                                   (assoc clj-col :source "clojure-model" :identification-method "deterministic")))]

                                                (assoc col-acc col-name merged-col)))
                                            {}
                                            all-columns)]

                 (assoc acc table-name (assoc merged-table :columns merged-columns))))
             {}
             all-tables)

     :metadata {:extraction-sources [:clojure-models :typescript-definitions :liquibase-migrations :sql-init-scripts]
                :priority-order ["clojure-model" "typescript-def" "liquibase-migration" "sql-init"]
                :merge-strategy "highest-priority-wins"}}))

(defn extract-schema-from-version
  "Extract schema definitions from a specific Metabase version"
  [repo-path version]
  (try
    ;; Checkout the specific version
    (checkout-version repo-path version)

    ;; Extract schemas from different sources
    (let [clojure-schema (clj-models/extract-clojure-models repo-path)
          typescript-schema (ts-defs/extract-typescript-definitions repo-path)
          liquibase-schema (liquibase/extract-liquibase-schema repo-path)
          sql-init-schema (sql-init/extract-sql-init-schema repo-path)]

      ;; Merge schemas with priority order
      {:version version
       :extraction-timestamp (java.time.Instant/now)
       :sources {:clojure-models clojure-schema
                 :typescript-definitions typescript-schema
                 :liquibase-migrations liquibase-schema
                 :sql-init-scripts sql-init-schema}
       :unified-schema (merge-schema-sources
                         clojure-schema
                         typescript-schema
                         liquibase-schema
                         sql-init-schema)})

    (catch Exception e
      (throw (ex-info (str "Failed to extract schema for version " version)
                      {:version version
                       :error (.getMessage e)
                       :cause e})))

    (finally
      ;; Always return to main/master branch
      (try
        (shell/sh "git" "checkout" "main" :dir repo-path)
        (catch Exception _
          (shell/sh "git" "checkout" "master" :dir repo-path))))))

(defn extract-schema
  "Main entry point for schema extraction"
  [version repo-path]
  (cond
    (= version "latest")
    ;; For latest, extract from current HEAD
    (let [clojure-schema (clj-models/extract-clojure-models repo-path)
          typescript-schema (ts-defs/extract-typescript-definitions repo-path)
          liquibase-schema (liquibase/extract-liquibase-schema repo-path)
          sql-init-schema (sql-init/extract-sql-init-schema repo-path)]

      {:version "latest"
       :extraction-timestamp (java.time.Instant/now)
       :sources {:clojure-models clojure-schema
                 :typescript-definitions typescript-schema
                 :liquibase-migrations liquibase-schema
                 :sql-init-scripts sql-init-schema}
       :unified-schema (merge-schema-sources
                         clojure-schema
                         typescript-schema
                         liquibase-schema
                         sql-init-schema)})

    (some #(= version %) (list-supported-versions repo-path))
    ;; For tagged versions, checkout and extract
    (extract-schema-from-version repo-path version)

    :else
    (throw (ex-info (str "Unsupported version: " version)
                    {:version version
                     :supported-versions (list-supported-versions repo-path)}))))

(defn cache-schema
  "Cache extracted schema to avoid repeated extraction"
  [version schema cache-dir]
  (let [cache-file (io/file cache-dir (str "schema-" version ".edn"))]
    (.mkdirs (.getParentFile cache-file))
    (spit cache-file (pr-str schema))))

(defn load-cached-schema
  "Load schema from cache if available"
  [version cache-dir]
  (let [cache-file (io/file cache-dir (str "schema-" version ".edn"))]
    (when (.exists cache-file)
      (try
        (read-string (slurp cache-file))
        (catch Exception _
          nil)))))

(defn extract-schema-with-cache
  "Extract schema with caching support"
  [version repo-path & [{:keys [cache-dir force-refresh]}]]
  (let [cache-dir (or cache-dir (io/file (System/getProperty "java.io.tmpdir") "metabase-schema-cache"))]
    (if-let [cached (and (not force-refresh)
                         (load-cached-schema version cache-dir))]
      ;; Return cached version
      cached
      ;; Extract and cache
      (let [schema (extract-schema version repo-path)]
        (cache-schema version schema cache-dir)
        schema))))
