(ns metabase-appdb-validator.validation.runner
  "Main validation runner that coordinates all validation scopes"
  (:require [metabase-appdb-validator.validation.structural :as structural]
            [metabase-appdb-validator.validation.business-rules :as business-rules]
            [metabase-appdb-validator.validation.data-integrity :as data-integrity]
            [metabase-appdb-validator.validation.migration-state :as migration-state]
            [metabase-appdb-validator.database.connection :as db-conn]
            [clojure.string :as str]))

(defn run-validation-scope
  "Run a specific validation scope"
  [scope config db-info schema-def]
  (try
    (case scope
      :structural
      (structural/validate-structural config db-info schema-def)

      :business-rules
      (business-rules/validate-business-rules config db-info schema-def)

      :data-integrity
      (data-integrity/validate-data-integrity config db-info schema-def)

      :migration-state
      (migration-state/validate-migration-state config db-info schema-def)

      ;; Unknown scope
      [{:table-name "validation-runner"
        :column-name nil
        :validation-scope (name scope)
        :check-passed false
        :error-message (str "Unknown validation scope: " scope)
        :schema-source "validation-runner"
        :identification-method "deterministic"
        :metabase-version (:metabase-version config "unknown")
        :validation-timestamp (java.time.Instant/now)}])

    (catch Exception e
      (println "Error running validation scope" scope ":" (.getMessage e))
      [{:table-name "validation-runner"
        :column-name nil
        :validation-scope (name scope)
        :check-passed false
        :error-message (str "Validation scope failed: " (.getMessage e))
        :schema-source "validation-runner"
        :identification-method "deterministic"
        :metabase-version (:metabase-version config "unknown")
        :validation-timestamp (java.time.Instant/now)}])))

(defn run-validation
  "Run all requested validation scopes and return combined results"
  [config db-info schema-def]
  (let [scopes (:validation-scopes config [:structural :business-rules :data-integrity :migration-state])
        conn-spec (:connection db-info)]

    (when (:verbose config)
      (println "🔍 Running validation scopes:" (str/join ", " (map name scopes))))

    ;; Run each validation scope and collect results
    (reduce (fn [acc scope]
              (when (:verbose config)
                (println "  🧪" (str/capitalize (name scope)) "validation..."))

              (let [scope-results (run-validation-scope scope config db-info schema-def)]
                (when (:verbose config)
                  (let [passed (count (filter :check-passed scope-results))
                        total (count scope-results)]
                    (printf "    ✅ %d/%d checks passed\n" passed total)))

                (concat acc scope-results)))
            []
            scopes)))

(defn validate-configuration
  "Validate that the configuration is sufficient for validation"
  [config]
  (let [required-fields [:db-host :db-port :db-name :db-user]
        missing-fields (filter #(not (get config %)) required-fields)]

    (when (seq missing-fields)
      (throw (ex-info
               (str "Missing required configuration fields: " (str/join ", " missing-fields))
               {:missing-fields missing-fields
                :config config})))

    ;; Validate validation scopes
    (let [valid-scopes #{:structural :business-rules :data-integrity :migration-state}
          requested-scopes (:validation-scopes config)
          invalid-scopes (remove valid-scopes requested-scopes)]

      (when (seq invalid-scopes)
        (throw (ex-info
                 (str "Invalid validation scopes: " (str/join ", " invalid-scopes))
                 {:invalid-scopes invalid-scopes
                  :valid-scopes valid-scopes}))))

    config))

(defn create-validation-summary
  "Create a summary of validation results"
  [validation-results]
  (let [total-checks (count validation-results)
        passed-checks (count (filter :check-passed validation-results))
        failed-checks (- total-checks passed-checks)

        ;; Group by scope
        by-scope (group-by :validation-scope validation-results)
        scope-summaries (reduce (fn [acc [scope results]]
                                 (let [scope-total (count results)
                                       scope-passed (count (filter :check-passed results))
                                       scope-failed (- scope-total scope-passed)]
                                   (assoc acc scope
                                          {:total scope-total
                                           :passed scope-passed
                                           :failed scope-failed
                                           :pass-rate (if (> scope-total 0)
                                                       (/ scope-passed scope-total)
                                                       0.0)})))
                               {}
                               by-scope)

        ;; Group by table
        by-table (group-by :table-name validation-results)
        table-summaries (reduce (fn [acc [table results]]
                                 (let [table-total (count results)
                                       table-passed (count (filter :check-passed results))
                                       table-failed (- table-total table-passed)]
                                   (assoc acc table
                                          {:total table-total
                                           :passed table-passed
                                           :failed table-failed
                                           :pass-rate (if (> table-total 0)
                                                       (/ table-passed table-total)
                                                       0.0)})))
                               {}
                               by-table)]

    {:total-checks total-checks
     :passed-checks passed-checks
     :failed-checks failed-checks
     :overall-pass-rate (if (> total-checks 0)
                         (/ passed-checks total-checks)
                         0.0)
     :scope-summaries scope-summaries
     :table-summaries table-summaries
     :validation-timestamp (java.time.Instant/now)}))

(defn print-validation-summary
  "Print a human-readable validation summary"
  [validation-results config]
  (let [summary (create-validation-summary validation-results)]

    (println "")
    (println "📊 Validation Summary")
    (println "====================")
    (printf "Total Checks: %d\n" (:total-checks summary))
    (printf "✅ Passed: %d (%.1f%%)\n"
            (:passed-checks summary)
            (* 100.0 (:overall-pass-rate summary)))
    (printf "❌ Failed: %d (%.1f%%)\n"
            (:failed-checks summary)
            (* 100.0 (- 1.0 (:overall-pass-rate summary))))

    (println "")
    (println "By Validation Scope:")
    (doseq [[scope scope-summary] (:scope-summaries summary)]
      (printf "  %s: %d/%d passed (%.1f%%)\n"
              (str/capitalize scope)
              (:passed scope-summary)
              (:total scope-summary)
              (* 100.0 (:pass-rate scope-summary))))

    (when (and (:verbose config) (> (:failed-checks summary) 0))
      (println "")
      (println "Failed Checks by Table:")
      (doseq [[table table-summary] (:table-summaries summary)]
        (when (> (:failed table-summary) 0)
          (printf "  %s: %d/%d failed\n"
                  table
                  (:failed table-summary)
                  (:total table-summary)))))

    summary))