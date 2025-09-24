(ns metabase-diagnostics.cli
  "Main CLI orchestrator for Metabase diagnostic tools"
  (:require [clojure.tools.cli :as cli]
            [clojure.string :as str]
            [metabase-diagnostics.logging.core :as log]
            [metabase-diagnostics.output.formatter :as fmt])
  (:gen-class))

;; CLI Configuration Constants
(def CLI_NAME "metabase-diagnostics")
(def CLI_VERSION "0.1.0")
(def DEFAULT_LOG_LEVEL "info")
(def AVAILABLE_FORMATS ["json" "edn" "table" "csv" "summary"])

;; Available Tools Registry
(def AVAILABLE_TOOLS
  {"api-diagnostics" {:description "Run API diagnostics and health checks"
                      :namespace 'metabase-diagnostics.tools.api-diagnostics
                      :main-fn 'run-validation}})
   ; "appdb-validator" {:description "Validate Metabase application database"
   ;                    :namespace 'metabase-diagnostics.tools.appdb-validator
   ;                    :main-fn 'run-validation}
   ; "dependency-tracer" {:description "Trace dependencies in dashboards/questions/models"
   ;                      :namespace 'metabase-diagnostics.tools.dependency-tracer
   ;                      :main-fn 'run-tracing}})

;; Global CLI options
(def GLOBAL_OPTIONS
  [["-h" "--help" "Show help"]
   ["-v" "--version" "Show version"]
   ["-V" "--verbose" "Enable verbose logging"]
   [nil "--log-level LEVEL" (str "Set log level (" (str/join "|" ["trace" "debug" "info" "warn" "error"]) ")")
    :default DEFAULT_LOG_LEVEL
    :validate [#{"trace" "debug" "info" "warn" "error"} "Must be a valid log level"]]
   [nil "--format FORMAT" (str "Output format (" (str/join "|" AVAILABLE_FORMATS) ")")
    :default "summary"
    :validate [(set AVAILABLE_FORMATS) "Must be a valid format"]]
   ["-q" "--quiet" "Suppress non-essential output"]])

(defn show-version
  "Display version information"
  []
  (println (str CLI_NAME " " CLI_VERSION)))

(defn show-tool-help
  "Display help for a specific tool"
  [tool-name]
  (if-let [tool-config (get AVAILABLE_TOOLS tool-name)]
    (do
      (println (str CLI_NAME " " tool-name " - " (:description tool-config)))
      (println)
      (println "Usage:")
      (println (str "  " CLI_NAME " [global-options] " tool-name " [tool-options]"))
      (println)
      (println "Tool-specific help:")
      (try
        (require (:namespace tool-config))
        (if-let [help-fn (resolve (symbol (str (:namespace tool-config)) "show-help"))]
          (help-fn)
          (println "  No specific help available for this tool."))
        (catch Exception e
          (println "  Error loading tool help:" (ex-message e)))))
    (do
      (println "Unknown tool:" tool-name)
      (println "Available tools:" (str/join ", " (keys AVAILABLE_TOOLS))))))

(defn show-help
  "Display help information"
  [& {:keys [subcommand]}]
  (if subcommand
    (show-tool-help subcommand)
    (do
      (println (str CLI_NAME " - Metabase Diagnostic Tools"))
      (println)
      (println "Usage:")
      (println (str "  " CLI_NAME " [global-options] <command> [command-options]"))
      (println)
      (println "Global Options:")
      (println (:summary (cli/parse-opts [] GLOBAL_OPTIONS)))
      (println)
      (println "Available Commands:")
      (doseq [[tool-name tool-config] AVAILABLE_TOOLS]
        (println (str "  " (format "%-20s" tool-name) (:description tool-config))))
      (println)
      (println "Use '" CLI_NAME " <command> --help' for command-specific help."))))

(defn list-tools
  "List all available diagnostic tools"
  []
  (println "Available Metabase Diagnostic Tools:")
  (println)
  (doseq [[tool-name tool-config] AVAILABLE_TOOLS]
    (println (str "  " tool-name ": " (:description tool-config)))))

(defn setup-logging
  "Setup logging configuration based on CLI options"
  [{:keys [log-level verbose quiet]}]
  (let [effective-level (cond
                          verbose "debug"
                          quiet "warn"
                          :else log-level)]
    (log/set-log-level! (keyword effective-level))
    (log/info "Logging configured at level: %s" effective-level)))

(defn run-tool
  "Load and run a specific diagnostic tool"
  [tool-name args global-opts]
  (log/with-log-context {:tool tool-name}
    (fn []
      (log/info "Starting tool: %s" tool-name)
      (if-let [tool-config (get AVAILABLE_TOOLS tool-name)]
        (try
          (require (:namespace tool-config))
          (let [tool-main (resolve (symbol (str (:namespace tool-config)) (str (:main-fn tool-config))))
                tool-args {:tool-args args :global-options global-opts}]
            (if tool-main
              (do
                (log/debug "Executing tool function: %s" (:main-fn tool-config))
                (tool-main tool-args))
              (throw (ex-info "Tool main function not found"
                              {:tool tool-name
                               :expected-fn (:main-fn tool-config)}))))
          (catch Exception e
            (log/error-with-exception e "Tool execution failed: %s" tool-name)
            {:error (ex-message e)
             :type :tool-execution-error}))
        (do
          (log/error "Unknown tool: %s" tool-name)
          {:error (str "Unknown tool: " tool-name)
           :available-tools (keys AVAILABLE_TOOLS)
           :type :unknown-tool})))))

(defn parse-args
  "Parse command line arguments"
  [args]
  (let [parsed (cli/parse-opts args GLOBAL_OPTIONS :in-order true)
        global-opts (:options parsed)
        remaining-args (:arguments parsed)
        errors (:errors parsed)]
    (cond
      errors
      {:error (str "Invalid arguments: " (str/join ", " errors))
       :type :parse-error}

      (:help global-opts)
      {:action :show-help :subcommand (first remaining-args)}

      (:version global-opts)
      {:action :show-version}

      (empty? remaining-args)
      {:action :show-help}

      :else
      (let [tool-name (first remaining-args)
            tool-args (rest remaining-args)]
        {:action :run-tool
         :tool-name tool-name
         :tool-args tool-args
         :global-opts global-opts}))))

(defn format-result
  "Format tool execution result"
  [result global-opts]
  (if (:error result)
    (fmt/format-error result :include-stacktrace (:verbose global-opts))
    (fmt/format-diagnostic-result result
                                  :format (:format global-opts)
                                  :timestamp (java.time.Instant/now))))

(defn -main
  "Main entry point for the CLI"
  [& args]
  (let [parsed (parse-args args)]
    (cond
      (:error parsed)
      (do
        (println (fmt/format-error parsed))
        (System/exit 1))

      (= (:action parsed) :show-version)
      (show-version)

      (= (:action parsed) :show-help)
      (show-help :subcommand (:subcommand parsed))

      (= (:action parsed) :run-tool)
      (let [{:keys [tool-name tool-args global-opts]} parsed]
        (setup-logging global-opts)
        (let [result (run-tool tool-name tool-args global-opts)]
          (when-not (:quiet global-opts)
            (println (format-result result global-opts)))
          (when (:error result)
            (System/exit 1)))))))
