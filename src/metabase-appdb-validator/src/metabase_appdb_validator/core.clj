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
          (let [output-file (or (:output-file config) "metabase-validation-results.csv")]
            (csv-output/write-validation-results validation-results output-file)
            (println "📄 Results written to:" output-file)

            ;; Return summary for programmatic use
            {:total-checks (count validation-results)
             :passed-checks (count (filter :check-passed validation-results))
             :failed-checks (count (filter #(not (:check-passed %)) validation-results))
             :output-file output-file
             :validation-results validation-results}))))

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
        (println "✅ Schema extraction completed for" (count versions) "versions")))

    (catch Exception e
      (println "❌ Schema extraction failed:")
      (println (.getMessage e))
      (when (:verbose config)
        (.printStackTrace e))
      (System/exit 1))))

(defn list-versions
  "List all available Metabase versions for schema extraction"
  [config]
  (try
    (let [versions (schema-extractor/list-supported-versions
                     (:metabase-repo-path config "/Users/jacobjoseph/dev/metabase/metabase"))]
      (println "📋 Available Metabase versions for schema validation:")
      (println "")
      (println "Major Version Initials:")
      (doseq [version (filter #(re-matches #"v\d+\.\d+\.0" %) versions)]
        (println "  " version))
      (println "")
      (println "Latest Patch Versions:")
      (doseq [version (remove #(re-matches #"v\d+\.\d+\.0" %) versions)]
        (println "  " version))
      (println "")
      (printf "Total: %d versions supported\n" (count versions)))

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