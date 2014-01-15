(defproject clj-elasticsearch-rest "0.5.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [cheshire "5.3.1"]
                 [clj-elasticsearch "0.5.0-SNAPSHOT"]
                 [http-kit "2.1.16"]
                 [org.clojure/core.async "0.1.267.0-0d7780-alpha"]]
  :profiles {:dev {:dependencies [[clj-elasticsearch-native "0.5.0-SNAPSHOT"]
                                  [org.elasticsearch/elasticsearch "0.20.5"]]}})
