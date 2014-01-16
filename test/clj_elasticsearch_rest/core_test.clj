(ns clj-elasticsearch-rest.core-test
  (:require [clojure.test :refer :all]
            [clj-elasticsearch-rest.core :refer :all]
            [clj-elasticsearch-native.core :as nat]
            [clojure.java.io :refer :all]
            [clojure.core.async :refer [<!!]]))

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

(def match-all {:query {:match_all {}}})

(deftest es-client
  ;; TODO harmonize boolean k
  (is (false? (:exists? (exists-index {:index ["test"]}))))
  (is (:id (index-doc {:index "test" :type "tyu"
                       :source {:field1 ["toto" "tutu"] :field2 42
                                :field3 {:tyu {:foo "bar"}}}
                       :id "mid"})))
  (is (> (get-in (refresh-index {:indices ["test"]}) [:shards :successful]) 0))
  (is (true? (:exists? (exists-index {:indices ["test"]}))))
  ;; TODO harmonize (not KW)
  (is (:status (cluster-health {:indices ["test"]})))
  (is (= 1 (:count (count-docs {:indices ["test"] :type "tyu"}))))
  (let [c (atom nil)
        l (make-listener {:on-response (fn [r] (reset! c (:count r)))
                          :format :clj})]
    (count-docs {:indices ["test"] :listener l})
    (Thread/sleep 50)
    (is (= 1 @c)))
  (let [ft (count-docs {:indices ["test"] :async? true})]
    (is (= 1 (:count @ft)))
    (is (realized? ft)))
  (let [ft (count-docs {:indices ["test"] :chan? true})]
    (is (= 1 (:count (<!! ft)))))
  ;; normalize :num_docs
  (let [status (index-status {:indices ["test"]})]
    (is (=  1 (get-in status [:indices :test :docs :num-docs]))))
  (let [d (get-doc {:index "test" :type "tyu" :id "mid" :fields ["field1" "field2" :field3]})]
    (is (nil? (:source d)))
    (is (= {:tyu {:foo "bar"}} (get-in d [:fields :field3])))
    (is (= ["toto" "tutu"] (get-in d [:fields :field1]))))
  (let [d (get-doc {:index "test" :type "tyu" :id "mid"})]
    (is (nil? (:fields d)))
    (is (= {:tyu {:foo "bar"}} (get-in d [:source :field3])))
    (is (= ["toto" "tutu"] (get-in d [:source :field1]))))
  (is (get-in (first
               (get-in (search {:indices ["test"] :types ["tyu"]
                                :extra-source match-all :search-type :query-then-fetch})
                       [:hits :hits]))
              [:source :field1])
      ["toto" "tutu"])
  ;; bulk test
  (let [res (bulk-request {:indices ["test"]
                           :actions [[{:index {:_type "tyu" :_id "foo1"}} {:ktest "foo1"}]
                                     [{:index {:_type "tyu" :_id "foo2"}} {:ktest "foo2"}]]})]
    (is (every? true? (map #(-> % (vals) (first) (:ok)) (:items res))))
    (is (= 2 (count (:items res)))))
  ;; (is (re-find #":matches :version .* ?:type :index :id"
  ;;              (:doc (meta #'clj-elasticsearch-rest.core/index-doc))))
  )
