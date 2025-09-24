(ns metabase-appdb-validator.core
  "Main entry point for Metabase Application Database Validator"
  (:require [clojure.tools.cli :as cli]
            [clojure.string :as str]
            [metabase-appdb-validator.cli :as app-cli]
            [metabase-appdb-validator.schema.extractor :as schema-extractor]
            [metabase-appdb-validator.validation.runner :as validation-runner]
            [metabase-appdb-validator.database.connection :as db-conn]
            [metabase-appdb-validator.output.csv :as csv-output])
  (:gen-class))

(def VERSION "1.0.0")

(defn generate-timestamped-filename
  "Generate a filename with ISO timestamp (no punctuation/letters)"
  [base-name extension]
  (let [now (java.time.Instant/now)
        formatter (java.time.format.DateTimeFormatter/ofPattern "yyyyMMddHHmmss")
        timestamp (.format formatter (.atZone now (java.time.ZoneId/systemDefault)))]
    (str base-name "-" timestamp "." extension)))

(defn print-banner []
  (println "")
  (println "███╗   ███╗███████╗████████╗ █████╗ ██████╗  █████╗ ███████╗███████╗")
  (println "████╗ ████║██╔════╝╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔════╝")
  (println "██╔████╔██║█████╗     ██║   ███████║██████╔╝███████║███████╗█████╗  ")
  (println "██║╚██╔╝██║██╔══╝     ██║   ██╔══██║██╔══██╗██╔══██║╚════██║██╔══╝  ")
  (println "██║ ╚═╝ ██║███████╗   ██║   ██║  ██║██████╔╝██║  ██║███████║███████╗")
  (println "╚═╝     ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═════╝ ╚═╝  ╚═╝╚══════╝╚══════╝")
  (println "")
  (println "        Application Database Validator v" VERSION)
  (println "        Comprehensive schema validation for Metabase databases")
  (println ""))

(defn validate-database
  "Main validation function that coordinates all validation scopes"
  [config]
  (try
    (when (:verbose config)
      (println "🔍 Starting Metabase Application Database Validation")
      (println "📊 Database:" (:db-host config) ":" (:db-port config) "/" (:db-name config))
      (println "🏷️  Version:" (:metabase-version config))
      (println "🎯 Scopes:" (str/join ", " (:validation-scopes config))))

    ;; Validate database connection and type
    (let [db-info (db-conn/validate-connection config)]
      (when (:verbose config)
        (println "✅ Database connection validated")
        (println "🐘 Database type:" (:db-type db-info))
        (println "📋 Database version:" (:db-version db-info)))

      ;; Extract schema for specified version
      (when (:verbose config)
        (println "🔬 Extracting schema definitions..."))

      (let [schema-def (schema-extractor/extract-schema
                         (:metabase-version config)
                         (:metabase-repo-path config "/Users/jacobjoseph/dev/metabase/metabase"))]

        (when (:verbose config)
          (println "✅ Schema extracted successfully")
          (printf "📋 Found %d tables, %d total columns\n"
                  (count (:tables schema-def))
                  (reduce + (map #(count (:columns %)) (:tables schema-def)))))

        ;; Run validation
        (when (:verbose config)
          (println "🧪 Running validation scopes..."))

        (let [validation-results (validation-runner/run-validation
                                   config
                                   db-info
                                   schema-def)]

          (when (:verbose config)
            (printf "✅ Validation completed: %d checks performed\n"
                    (count validation-results))
            (let [failed (count (filter #(not (:check-passed %)) validation-results))
                  passed (- (count validation-results) failed)]
              (printf "✅ Passed: %d, ❌ Failed: %d\n" passed failed)))

          ;; Output results
          (let [output-file (or (:output-file config)
                                (generate-timestamped-filename "metabase-validation-results" "csv"))]
            (csv-output/write-validation-results validation-results output-file)
            (println "📄 Results written to:" output-file)
            (System/exit 0)

            ;; Return summary for programmatic use (unreachable but kept for API compatibility)
            {:total-checks (count validation-results)
             :passed-checks (count (filter :check-passed validation-results))
             :failed-checks (count (filter #(not (:check-passed %)) validation-results))
             :output-file output-file
             :validation-results validation-results})))

    (catch Exception e
      (println "❌ Validation failed with error:")
      (println (.getMessage e))
      (when (:verbose config)
        (.printStackTrace e))
      (System/exit 1))))

(defn extract-schemas
  "Extract schemas for all or specific versions"
  [config]
  (try
    (when (:verbose config)
      (println "🔬 Extracting Metabase schemas..."))

    (let [versions (if (:version config)
                     [(:version config)]
                     (schema-extractor/list-supported-versions
                       (:metabase-repo-path config "/Users/jacobjoseph/dev/metabase/metabase")))]

      (doseq [version versions]
        (when (:verbose config)
          (println "📋 Extracting schema for version:" version))

        (let [schema (schema-extractor/extract-schema
                       version
                       (:metabase-repo-path config "/Users/jacobjoseph/dev/metabase/metabase"))]
          (when (:verbose config)
            (printf "  ✅ %d tables extracted\n" (count (:tables schema))))))

      (when (:verbose config)
        (println "✅ Schema extraction completed for" (count versions) "versions"))
      (System/exit 0))

    (catch Exception e
      (println "❌ Schema extraction failed:")
      (println (.getMessage e))
      (when (:verbose config)
        (.printStackTrace e))
      (System/exit 1))))

(defn sort-versions
  "Sort versions by license, major, minor, patch in descending order"
  [versions]
  (let [four-part-pattern #"^v(\d+)\.(\d+)\.(\d+)\.(\d+)$"
        three-part-pattern #"^v(\d+)\.(\d+)\.(\d+)$"
        parse-version (fn [version]
                        (cond
                          ;; Try 4-part version first (license.major.minor.patch)
                          (re-matches four-part-pattern version)
                          (let [match (re-matches four-part-pattern version)]
                            {:version version
                             :license (Integer/parseInt (nth match 1))
                             :major (Integer/parseInt (nth match 2))
                             :minor (Integer/parseInt (nth match 3))
                             :patch (Integer/parseInt (nth match 4))})

                          ;; Try 3-part version (major.minor.patch, treat as license=0)
                          (re-matches three-part-pattern version)
                          (let [match (re-matches three-part-pattern version)]
                            {:version version
                             :license (Integer/parseInt (nth match 1))
                             :major (Integer/parseInt (nth match 2))
                             :minor (Integer/parseInt (nth match 3))
                             :patch 0})

                          ;; Return nil for unrecognized formats
                          :else nil))

        parsed-versions (->> versions
                            (map parse-version)
                            (filter some?))]

    (->> parsed-versions
         (sort-by (juxt :license :major :minor :patch))
         (reverse)
         (map :version))))

(defn get-all-version-tags
  "Get all version tags that start with v0 or v1 and end with a digit"
  [repo-path]
  (let [all-tags (schema-extractor/get-git-tags repo-path)]
    (->> all-tags
         (filter #(re-matches #"^v[01].*\d$" %))
         (sort))))

(defn list-versions
  "List all available Metabase versions for schema extraction"
  [config]
  (try
    (let [versions (get-all-version-tags
                     (:metabase-repo-path config "/Users/jacobjoseph/dev/metabase/metabase"))
          sorted-versions (sort-versions versions)]

      (println "📋 Available Metabase versions for schema validation:")
      (println "")
      (doseq [version sorted-versions]
        (println "  " version))
      (println "")
      (printf "Total: %d versions supported\n" (count versions))
      (System/exit 0))

    (catch Exception e
      (println "❌ Failed to list versions:")
      (println (.getMessage e))
      (when (:verbose config)
        (.printStackTrace e))
      (System/exit 1))))

(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args app-cli/cli-options)]
    (cond
      ;; Help requested
      (:help options)
      (do
        (print-banner)
        (println "Available commands:")
        (println "  validate        - Validate a Metabase application database")
        (println "  extract-schemas - Extract schemas from Metabase versions")
        (println "  list-versions   - List available Metabase versions")
        (println "")
        (println "Usage Examples:")
        (println "")
        (println "  # Validate database with all options:")
        (println "  ./metabase-appdb-validator validate \\")
        (println "    --db-host localhost \\")
        (println "    --db-port 5432 \\")
        (println "    --db-name metabase \\")
        (println "    --db-user metabase \\")
        (println "    --db-password secret \\")
        (println "    --metabase-version v0.55.15 \\")
        (println "    --metabase-repo-path /path/to/metabase \\")
        (println "    --scope structural,business-rules,data-integrity \\")
        (println "    --output my-validation-results.csv \\")
        (println "    --config config.edn \\")
        (println "    --verbose")
        (println "")
        (println "  # Extract schemas with all options:")
        (println "  ./metabase-appdb-validator extract-schemas \\")
        (println "    --metabase-repo-path /path/to/metabase \\")
        (println "    --metabase-version v0.55.15 \\")
        (println "    --verbose")
        (println "")
        (println "  # List versions with all options:")
        (println "  ./metabase-appdb-validator list-versions \\")
        (println "    --metabase-repo-path /path/to/metabase \\")
        (println "    --verbose")
        (println "")
        (println "Options:")
        (println summary)
        (System/exit 0))

      ;; Version requested
      (:version options)
      (do
        (println "metabase-appdb-validator version" VERSION)
        (System/exit 0))

      ;; Errors in CLI parsing
      errors
      (do
        (println "❌ Error parsing command line arguments:")
        (doseq [error errors]
          (println "  " error))
        (println "")
        (println "Use --help for usage information")
        (System/exit 1))

      ;; No command provided
      (empty? arguments)
      (do
        (print-banner)
        (println "Available commands:")
        (println "  validate        - Validate a Metabase application database")
        (println "  extract-schemas - Extract schemas from Metabase versions")
        (println "  list-versions   - List available Metabase versions")
        (println "")
        (println "Use --help for detailed usage information")
        (System/exit 1))

      ;; Execute command
      :else
      (let [command (first arguments)
            config (merge options {:verbose (:verbose options)})]

        (case command
          "validate"
          (do
            (when-not (:help options) (print-banner))
            (validate-database config))

          "extract-schemas"
          (extract-schemas config)

          "list-versions"
          (list-versions config)

          ;; Unknown command
          (do
            (println "❌ Unknown command:" command)
            (println "Available commands: validate, extract-schemas, list-versions")
            (println "Use --help for usage information")
            (System/exit 1)))))))
