;; Simple test to verify the basic CLI works
(ns test-basic
  (:require [metabase-appdb-validator.core :as core]))

(println "Testing basic CLI functionality...")

;; Test that the main function exists and can be called
(try
  (core/-main "--help")
  (println "✅ CLI help works")
  (catch Exception e
    (println "❌ CLI error:" (.getMessage e))))

(println "Basic test completed.")