(ns metabase-diagnostics.output.formatter
  "Output formatting utilities for diagnostic tools"
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.pprint :as pprint]))

;; Output Format Constants
(def FORMAT_JSON "json")
(def FORMAT_EDN "edn")
(def FORMAT_TABLE "table")
(def FORMAT_CSV "csv")
(def FORMAT_SUMMARY "summary")

;; Table Formatting Constants
(def DEFAULT_COLUMN_WIDTH 20)
(def MIN_COLUMN_WIDTH 10)
(def MAX_COLUMN_WIDTH 50)
(def TABLE_SEPARATOR_CHAR "-")
(def TABLE_PADDING_CHAR " ")
(def CSV_SEPARATOR ",")

;; Output Configuration
(def DEFAULT_FORMAT FORMAT_SUMMARY)
(def JSON_PRETTY_PRINT true)

(defn- truncate-string
  "Truncate string to specified width"
  [s width]
  (if (<= (count s) width)
    s
    (str (subs s 0 (- width 3)) "...")))

(defn- pad-string
  "Pad string to specified width"
  [s width]
  (let [padding-needed (- width (count s))]
    (if (<= padding-needed 0)
      s
      (str s (apply str (repeat padding-needed TABLE_PADDING_CHAR))))))

(defn- calculate-column-widths
  "Calculate optimal column widths for table display"
  [headers rows]
  (let [all-rows (cons headers rows)
        column-count (count headers)]
    (for [col-idx (range column-count)]
      (let [column-values (map #(str (nth % col-idx "")) all-rows)
            max-width (apply max (map count column-values))]
        (max MIN_COLUMN_WIDTH (min MAX_COLUMN_WIDTH max-width))))))

(defn format-as-json
  "Format data as JSON"
  [data & {:keys [pretty] :or {pretty JSON_PRETTY_PRINT}}]
  (json/write-str data))

(defn format-as-edn
  "Format data as EDN"
  [data & {:keys [pretty] :or {pretty true}}]
  (if pretty
    (with-out-str (pprint/pprint data))
    (pr-str data)))

(defn format-as-csv
  "Format tabular data as CSV"
  [headers rows & {:keys [separator] :or {separator CSV_SEPARATOR}}]
  (let [format-row (fn [row] (str/join separator (map #(str "\"" (str/replace (str %) "\"" "\"\"") "\"") row)))
        header-line (format-row headers)
        data-lines (map format-row rows)]
    (str/join "\n" (cons header-line data-lines))))

(defn format-as-table
  "Format tabular data as ASCII table"
  [headers rows & {:keys [max-width] :or {max-width MAX_COLUMN_WIDTH}}]
  (if (empty? rows)
    (str "No data to display\n")
    (let [column-widths (calculate-column-widths headers rows)
          format-row (fn [row]
                       (str "| "
                            (str/join " | "
                                      (map-indexed (fn [idx val]
                                                     (-> (str val)
                                                         (truncate-string (nth column-widths idx))
                                                         (pad-string (nth column-widths idx))))
                                                   row))
                            " |"))
          separator-line (str "+"
                              (str/join "+"
                                        (map #(apply str (repeat (+ % 2) TABLE_SEPARATOR_CHAR))
                                             column-widths))
                              "+")
          header-line (format-row headers)
          data-lines (map format-row rows)]
      (str/join "\n"
                (concat [separator-line header-line separator-line]
                        data-lines
                        [separator-line ""])))))

(defn format-summary-stats
  "Format summary statistics"
  [stats]
  (let [format-stat (fn [[key value]]
                      (str "  " (name key) ": " value))]
    (str "Summary:\n"
         (str/join "\n" (map format-stat stats))
         "\n")))

(defn format-diagnostic-result
  "Format diagnostic result with metadata"
  [result & {:keys [format tool-name operation timestamp]
             :or {format DEFAULT_FORMAT}}]
  (let [header (when (or tool-name operation timestamp)
                 (str "=== Diagnostic Result ===\n"
                      (when tool-name (str "Tool: " tool-name "\n"))
                      (when operation (str "Operation: " operation "\n"))
                      (when timestamp (str "Timestamp: " timestamp "\n"))
                      "\n"))
        formatted-data (case format
                         "json" (format-as-json result)
                         "edn" (format-as-edn result)
                         "table" (if (and (map? result)
                                          (contains? result :headers)
                                          (contains? result :rows))
                                   (format-as-table (:headers result) (:rows result))
                                   (format-as-edn result))
                         "csv" (if (and (map? result)
                                        (contains? result :headers)
                                        (contains? result :rows))
                                 (format-as-csv (:headers result) (:rows result))
                                 (format-as-json result))
                         "summary" (if (map? result)
                                     (format-summary-stats result)
                                     (format-as-edn result))
                         (format-as-edn result))]
    (str header formatted-data)))

(defn format-error
  "Format error message"
  [error & {:keys [include-stacktrace] :or {include-stacktrace false}}]
  (let [error-msg (cond
                    (instance? Exception error) (ex-message error)
                    (map? error) (str (:type error "Error") ": " (:message error (str error)))
                    :else (str error))
        stacktrace (when (and include-stacktrace (instance? Exception error))
                     (with-out-str (.printStackTrace error)))]
    (str "ERROR: " error-msg
         (when stacktrace (str "\n\nStacktrace:\n" stacktrace)))))

(defn format-progress
  "Format progress indicator"
  [current total & {:keys [description]}]
  (let [percentage (if (> total 0) (int (* 100 (/ current total))) 0)
        progress-bar-width 20
        filled-chars (int (* progress-bar-width (/ current total)))
        empty-chars (- progress-bar-width filled-chars)
        bar (str "["
                 (apply str (repeat filled-chars "="))
                 (apply str (repeat empty-chars " "))
                 "]")]
    (str bar " " percentage "% (" current "/" total ")"
         (when description (str " - " description)))))

(defn create-table-data
  "Create table data structure from maps"
  [maps & {:keys [columns]}]
  (if (empty? maps)
    {:headers [] :rows []}
    (let [all-keys (if columns
                     columns
                     (distinct (mapcat keys maps)))
          headers (map name all-keys)
          rows (map (fn [m] (map #(get m %) all-keys)) maps)]
      {:headers headers :rows rows})))

(defn print-formatted
  "Print formatted output to stdout"
  [data & options]
  (println (apply format-diagnostic-result data options)))