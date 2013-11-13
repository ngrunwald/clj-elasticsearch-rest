(ns clj-elasticsearch-rest.core
  (:require [org.httpkit.client :as http]
            [clj-elasticsearch.specs :as specs]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [cheshire.core :as json]
            [clojure.stacktrace :as cst]
            [clojure.pprint :as pp]))

(def global-rest-specs
  (let [rs (io/resource "blueprint.edn")
        edn-str (slurp rs)]
    (edn/read-string edn-str)))

(defn- prepare-response
  [{:keys [opts body headers] :as resp}]
  (if (re-find #"json" (get headers "Content-Type"))
    (let [decoded (json/decode body)]
      (merge opts (assoc decoded :headers headers)))
    resp))

(defmethod specs/make-listener :rest
  [_
   {:keys [on-failure on-response]
    :or {format :clj
         on-failure (fn [e]
                      (binding [*out* *err*]
                        (println "Error in listener")
                        (pp/pprint (select-keys e [:opts :status]))))}}]
  (fn [{:keys [error] :as resp}]
    (if error
      (on-failure resp)
      (on-response (prepare-response resp)))))

(defn- build-url
  [rest-uri args rest-default]
  (let [interpolated (map #(if (string? %)
                             %
                             (get args % (get rest-default %))) args)]
    (->> interpolated
         (remove nil?)
         (str/join "/"))))

(defn kw->param
  [kw]
  (-> kw (name) (str/lower-case) (str/replace #"\?$" "") (.replace \- \_)))

(defn- make-requester
  [{:keys [symb constructor required
           rest-uri rest-method rest-default]
    :as spec}]
  (when (and rest-uri rest-method)
    (let [required-args (concat constructor required)
          uri-args (conj (filter keyword? rest-uri) :source :async? :listener)]
      (fn [client {:keys [source async? listener] :as args}]
        (let [expanded-url (build-url rest-uri args rest-default)
              full-uri (str (:base-url client) "/" expanded-url)
              args-left (apply dissoc args uri-args)
              merged-headers (merge (:headers client) (:headers args))
              http-opts (merge client {:url full-uri
                                       :headers merged-headers
                                       :query-params (zipmap (map kw->param (keys args-left))
                                                             (map str (vals args-left)))
                                       :method rest-method})
              http-with-body (if source
                               (assoc http-opts :body (if (string? source)
                                                        source
                                                        (json/encode source)))
                               http-opts)
              callback (fn [{:keys [opts status body headers error] :as resp}]
                         (if error
                           resp
                           (prepare-response resp)))]
          (cond
           async? (http/request http-with-body callback)
           listener (http/request http-with-body listener)
           :default (let [{:keys [error opts] :as resp} (deref (http/request http-with-body callback))]
                      (if error
                        (throw (ex-info "error in request" {:error error :opts opts}))
                        resp))))))))

(defn- make-implementation!
  [specs]
  (reduce (fn [acc [class-name {:keys [symb impl constructor required] :as spec}]]
            (if-let [req-fn (make-requester spec)]
              (let [name-kw (-> symb (name) (keyword))
                    symb-name (vary-meta symb merge (meta req-fn))]
                (intern 'clj-elasticsearch-rest.core symb-name req-fn)
                (assoc acc name-kw req-fn))
              acc))
          {} specs))

(defonce implementation (make-implementation! specs/global-specs))

(extend-protocol specs/PCallAPI
  clojure.lang.IPersistentMap
  (specs/make-api-call [this method-name options]
    (specs/make-api-call* implementation this method-name options)))

(defmethod specs/make-client :rest
  [_ spec]
  (let [options {:base-url "http://localhost:9200"
                 :user-agent "clj-elasticsearch"
                 :headers {"Content-Type" "application/json"}}]
    (merge options spec)))
