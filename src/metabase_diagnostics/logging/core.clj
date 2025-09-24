(ns metabase-diagnostics.logging.core
  "Logging utilities for diagnostic tools"
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]))

;; Log Level Constants
(def LOG_LEVEL_TRACE :trace)
(def LOG_LEVEL_DEBUG :debug)
(def LOG_LEVEL_INFO :info)
(def LOG_LEVEL_WARN :warn)
(def LOG_LEVEL_ERROR :error)
(def LOG_LEVEL_FATAL :fatal)

;; Default Configuration
(def DEFAULT_LOG_LEVEL LOG_LEVEL_INFO)
(def MAX_MESSAGE_LENGTH 1000)
(def TRUNCATION_SUFFIX "...")

(def ^:dynamic *current-log-level* DEFAULT_LOG_LEVEL)
(def ^:dynamic *log-context* {})

(defn set-log-level!
  "Set the current log level"
  [level]
  {:pre [(#{LOG_LEVEL_TRACE LOG_LEVEL_DEBUG LOG_LEVEL_INFO
            LOG_LEVEL_WARN LOG_LEVEL_ERROR LOG_LEVEL_FATAL} level)]}
  (alter-var-root #'*current-log-level* (constantly level)))

(defn with-log-context
  "Execute function with additional log context"
  [context f]
  (binding [*log-context* (merge *log-context* context)]
    (f)))

(defn- format-context
  "Format log context as a string"
  [context]
  (when (seq context)
    (str "[" (str/join ", " (map (fn [[k v]] (str (name k) "=" v)) context)) "] ")))

(defn- truncate-message
  "Truncate long log messages"
  [message]
  (if (> (count message) MAX_MESSAGE_LENGTH)
    (str (subs message 0 (- MAX_MESSAGE_LENGTH (count TRUNCATION_SUFFIX))) TRUNCATION_SUFFIX)
    message))

(defn- format-message
  "Format log message with context and truncation"
  [message & args]
  (let [formatted-msg (if (seq args)
                        (apply format message args)
                        (str message))
        context-str (format-context *log-context*)]
    (truncate-message (str context-str formatted-msg))))

(defn trace
  "Log at TRACE level"
  [message & args]
  (log/trace (apply format-message message args)))

(defn debug
  "Log at DEBUG level"
  [message & args]
  (log/debug (apply format-message message args)))

(defn info
  "Log at INFO level"
  [message & args]
  (log/info (apply format-message message args)))

(defn warn
  "Log at WARN level"
  [message & args]
  (log/warn (apply format-message message args)))

(defn error
  "Log at ERROR level"
  [message & args]
  (log/error (apply format-message message args)))

(defn error-with-exception
  "Log at ERROR level with exception"
  [exception message & args]
  (log/error exception (apply format-message message args)))

(defn fatal
  "Log at FATAL level"
  [message & args]
  (log/fatal (apply format-message message args)))

(defn log-execution-time
  "Log execution time of a function"
  [description f]
  (let [start-time (System/currentTimeMillis)
        result (f)
        end-time (System/currentTimeMillis)
        duration (- end-time start-time)]
    (info "%s completed in %d ms" description duration)
    result))

(defmacro with-execution-time
  "Macro to log execution time of code block"
  [description & body]
  `(log-execution-time ~description (fn [] ~@body)))

(defn log-diagnostic-start
  "Log the start of a diagnostic operation"
  [tool-name operation & {:keys [target]}]
  (let [context (cond-> {:tool tool-name :operation operation}
                  target (assoc :target target))]
    (with-log-context context
      (info "Starting diagnostic operation: %s" operation))))

(defn log-diagnostic-end
  "Log the end of a diagnostic operation"
  [tool-name operation result & {:keys [target]}]
  (let [context (cond-> {:tool tool-name :operation operation}
                  target (assoc :target target))]
    (with-log-context context
      (info "Completed diagnostic operation: %s (result: %s)" operation (if result "success" "failure")))))

(defn log-api-call
  "Log API call details"
  [method url status-code duration-ms]
  (let [context {:api-method method :api-url url :status-code status-code :duration-ms duration-ms}]
    (with-log-context context
      (if (<= 200 status-code 299)
        (debug "API call successful: %s %s (%d ms)" method url duration-ms)
        (warn "API call failed: %s %s - Status %d (%d ms)" method url status-code duration-ms)))))

(defn log-database-operation
  "Log database operation details"
  [operation query-or-description duration-ms & {:keys [affected-rows]}]
  (let [context (cond-> {:db-operation operation :duration-ms duration-ms}
                  affected-rows (assoc :affected-rows affected-rows))]
    (with-log-context context
      (info "Database %s completed in %d ms%s: %s"
            operation
            duration-ms
            (if affected-rows (str " (affected " affected-rows " rows)") "")
            query-or-description))))