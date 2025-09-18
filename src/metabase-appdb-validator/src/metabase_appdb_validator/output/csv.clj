(ns metabase-appdb-validator.output.csv
  "CSV output formatting for validation results"
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def csv-headers
  ["table_name"
   "column_name"
   "validation_scope"
   "check_passed"
   "error_message"
   "schema_source"
   "identification_method"
   "metabase_version"
   "validation_timestamp"])

(defn format-validation-result
  "Format a single validation result for CSV output"
  [result]
  [(or (:table-name result) "")
   (or (:column-name result) "")
   (or (:validation-scope result) "")
   (str (:check-passed result))
   (or (:error-message result) "")
   (or (:schema-source result) "")
   (or (:identification-method result) "")
   (or (:metabase-version result) "")
   (str (:validation-timestamp result))])

(defn validation-results-to-csv-data
  "Convert validation results to CSV data format"
  [validation-results]
  (cons csv-headers
        (map format-validation-result validation-results)))

(defn write-validation-results
  "Write validation results to CSV file"
  [validation-results output-file]
  (try
    (with-open [writer (io/writer output-file)]
      (csv/write-csv writer (validation-results-to-csv-data validation-results)))

    (println (str "✅ Validation results written to: " output-file))
    {:success true
     :file output-file
     :record-count (count validation-results)}

    (catch Exception e
      (println (str "❌ Failed to write CSV file: " (.getMessage e)))
      {:success false
       :error (.getMessage e)
       :file output-file})))

(defn create-summary-csv
  "Create a summary CSV with aggregated results"
  [validation-results output-file]
  (try
    (let [;; Group by table and scope
          by-table-scope (group-by (fn [result]
                                    [(:table-name result) (:validation-scope result)])
                                  validation-results)

          summary-data (map (fn [[[table scope] results]]
                             (let [total (count results)
                                   passed (count (filter :check-passed results))
                                   failed (- total passed)
                                   pass-rate (if (> total 0) (/ passed total) 0.0)]
                               [table
                                scope
                                total
                                passed
                                failed
                                (format "%.1f%%" (* 100.0 pass-rate))]))
                           by-table-scope)

          summary-headers ["table_name" "validation_scope" "total_checks" "passed_checks" "failed_checks" "pass_rate"]
          csv-data (cons summary-headers summary-data)]

      (with-open [writer (io/writer output-file)]
        (csv/write-csv writer csv-data))

      (println (str "✅ Summary CSV written to: " output-file))
      {:success true
       :file output-file
       :summary-count (count summary-data)})

    (catch Exception e
      (println (str "❌ Failed to write summary CSV: " (.getMessage e)))
      {:success false
       :error (.getMessage e)
       :file output-file})))

(defn write-detailed-report
  "Write a detailed report with multiple CSV files"
  [validation-results base-filename]
  (let [base-name (str/replace base-filename #"\.csv$" "")
        detailed-file (str base-name "-detailed.csv")
        summary-file (str base-name "-summary.csv")
        failed-only-file (str base-name "-failed-only.csv")

        failed-results (filter #(not (:check-passed %)) validation-results)]

    (println "📊 Writing detailed validation report...")

    ;; Write detailed results
    (write-validation-results validation-results detailed-file)

    ;; Write summary
    (create-summary-csv validation-results summary-file)

    ;; Write failed checks only
    (if (seq failed-results)
      (write-validation-results failed-results failed-only-file)
      (println "✅ No failed checks - skipping failed-only CSV"))

    {:detailed-file detailed-file
     :summary-file summary-file
     :failed-only-file (when (seq failed-results) failed-only-file)
     :total-results (count validation-results)
     :failed-results (count failed-results)}))

(defn format-for-excel
  "Format CSV data to be Excel-compatible"
  [csv-data]
  ;; Excel-specific formatting adjustments
  (map (fn [row]
         (map (fn [cell]
                (cond
                  ;; Handle potential CSV injection
                  (and (string? cell)
                       (some #(str/starts-with? cell %) ["=" "-" "+" "@"]))
                  (str "'" cell)

                  ;; Handle newlines in error messages
                  (and (string? cell) (str/includes? cell "\n"))
                  (str/replace cell "\n" " ")

                  ;; Handle commas in text
                  (and (string? cell) (str/includes? cell ","))
                  (str "\"" cell "\"")

                  :else cell))
              row))
       csv-data))

(defn write-excel-compatible-csv
  "Write Excel-compatible CSV with proper formatting"
  [validation-results output-file]
  (try
    (let [csv-data (validation-results-to-csv-data validation-results)
          excel-formatted (format-for-excel csv-data)]

      (with-open [writer (io/writer output-file :encoding "UTF-8")]
        ;; Write BOM for Excel UTF-8 recognition
        (.write writer "\uFEFF")
        (csv/write-csv writer excel-formatted))

      (println (str "✅ Excel-compatible CSV written to: " output-file))
      {:success true
       :file output-file
       :record-count (count validation-results)
       :excel-compatible true})

    (catch Exception e
      (println (str "❌ Failed to write Excel-compatible CSV: " (.getMessage e)))
      {:success false
       :error (.getMessage e)
       :file output-file})))

(defn print-csv-stats
  "Print statistics about the CSV output"
  [validation-results output-file]
  (let [total (count validation-results)
        passed (count (filter :check-passed validation-results))
        failed (- total passed)
        by-scope (group-by :validation-scope validation-results)
        by-source (group-by :schema-source validation-results)]

    (println "")
    (println "📄 CSV Output Statistics")
    (println "========================")
    (printf "File: %s\n" output-file)
    (printf "Total records: %d\n" total)
    (printf "Passed checks: %d (%.1f%%)\n" passed (* 100.0 (/ passed total)))
    (printf "Failed checks: %d (%.1f%%)\n" failed (* 100.0 (/ failed total)))

    (println "\nBy validation scope:")
    (doseq [[scope results] by-scope]
      (printf "  %s: %d records\n" scope (count results)))

    (println "\nBy schema source:")
    (doseq [[source results] by-source]
      (printf "  %s: %d records\n" source (count results)))

    (println "")))