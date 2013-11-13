(ns clj-elasticsearch-rest.core-test
  (:require [clojure.test :refer :all]
            [clj-elasticsearch-rest.core :refer :all]
            [clj-elasticsearch-native.core :as nat]
            [clojure.java.io :refer :all]))

(defn delete-dir
  [path]
  (let [d (file path)]
    (if (.isDirectory d)
      (do
        (doseq [f (seq (.listFiles d))]
          (delete-dir f)
          (.delete f))
        (.delete d))
      (.delete d))))

(defn es-fixture
  [f]
  (let [node (nat/make-node {:local-mode true :client-mode false})]
    (with-rest-client {}
      (f))
    (.close node)
    (delete-dir "data")))

(use-fixtures :each es-fixture)

(deftest es-client
  (is (:id (index-doc {:index "test" :type "tyu"
                       :source {:field1 ["toto" "tutu"] :field2 42
                                :field3 {:tyu {:foo "bar"}}}
                       :id "mid"}))))
