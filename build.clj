(ns build
  "Build configuration for metabase-diagnostics"
  (:require [clojure.tools.build.api :as b]
            [clojure.string :as str]))

;; Build Constants
(def LIB_NAME 'metabase-diagnostics/metabase-diagnostics)
(def VERSION "0.1.0")
(def CLASS_DIR "target/classes")
(def BASIS (b/create-basis {:project "deps.edn"}))
(def UBER_FILE (format "target/%s-%s-standalone.jar" (name LIB_NAME) VERSION))
(def JAR_FILE (format "target/%s-%s.jar" (name LIB_NAME) VERSION))

(defn clean
  "Delete the build target directory"
  [_]
  (b/delete {:path "target"}))

(defn prep
  "Prepare for building"
  [_]
  (b/write-pom {:class-dir CLASS_DIR
                :lib LIB_NAME
                :version VERSION
                :basis BASIS
                :src-dirs ["src/metabase_diagnostics"]}))

(defn jar
  "Build a JAR file"
  [_]
  (clean nil)
  (prep nil)
  (b/copy-dir {:src-dirs ["src/metabase_diagnostics"]
               :target-dir CLASS_DIR})
  (b/jar {:class-dir CLASS_DIR
          :jar-file JAR_FILE}))

(defn uber
  "Build an uberjar"
  [_]
  (clean nil)
  (prep nil)
  (b/copy-dir {:src-dirs ["src/metabase_diagnostics"]
               :target-dir CLASS_DIR})
  (b/compile-clj {:basis BASIS
                  :src-dirs ["src/metabase_diagnostics"]
                  :class-dir CLASS_DIR})
  (b/uber {:class-dir CLASS_DIR
           :uber-file UBER_FILE
           :basis BASIS
           :main 'metabase-diagnostics.cli}))
