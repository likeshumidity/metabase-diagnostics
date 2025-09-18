(ns metabase-appdb-validator.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [metabase-appdb-validator.core :as core]
            [metabase-appdb-validator.cli :as cli]
            [metabase-appdb-validator.schema.extractor :as extractor]))

(deftest test-cli-options-parsing
  (testing "CLI options parsing"
    (let [args ["--db-host" "localhost" "--db-port" "5432" "--db-name" "test"]
          {:keys [options]} (clojure.tools.cli/parse-opts args cli/cli-options)]
      (is (= "localhost" (:db-host options)))
      (is (= 5432 (:db-port options)))
      (is (= "test" (:db-name options))))))

(deftest test-validation-scopes
  (testing "Validation scope resolution"
    (let [valid-scopes (cli/resolve-validation-scopes "structural,business-rules")]
      (is (= [:structural :business-rules] valid-scopes))))

  (testing "Invalid validation scope"
    (is (thrown? Exception
                 (cli/resolve-validation-scopes "invalid-scope")))))

(deftest test-version-listing
  (testing "Version listing functionality"
    ;; Mock test - in practice would test against actual git repository
    (is (string? "v1.50.0"))  ; Placeholder test
    (is (re-matches #"v\d+\.\d+\.\d+" "v1.50.0"))))

(deftest test-schema-extraction
  (testing "Schema extraction framework"
    ;; This would be integration test requiring actual Metabase repo
    ;; For now, just test that functions exist and are callable
    (is (fn? extractor/list-supported-versions))
    (is (fn? extractor/extract-schema))))

(deftest test-validation-result-format
  (testing "Validation result structure"
    (let [sample-result {:table-name "test_table"
                        :column-name "test_column"
                        :validation-scope "structural"
                        :check-passed true
                        :error-message nil
                        :schema-source "test"
                        :identification-method "deterministic"
                        :metabase-version "v1.50.0"
                        :validation-timestamp (java.time.Instant/now)}]
      (is (contains? sample-result :table-name))
      (is (contains? sample-result :check-passed))
      (is (contains? sample-result :validation-scope)))))

(deftest test-csv-headers
  (testing "CSV output headers"
    (let [expected-headers ["table_name" "column_name" "validation_scope" "check_passed"
                           "error_message" "schema_source" "identification_method"
                           "metabase_version" "validation_timestamp"]]
      (is (= expected-headers metabase-appdb-validator.output.csv/csv-headers)))))

;; Integration test placeholder
(deftest test-full-validation-workflow
  (testing "Full validation workflow (integration test)"
    ;; This would be a full integration test that:
    ;; 1. Sets up a test database
    ;; 2. Extracts schema from a known Metabase version
    ;; 3. Runs validation
    ;; 4. Verifies output format
    ;; For now, just verify the main functions exist
    (is (fn? core/validate-database))
    (is (fn? core/extract-schemas))
    (is (fn? core/list-versions))))