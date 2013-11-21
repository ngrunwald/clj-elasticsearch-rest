(ns clj-elasticsearch-rest.core
  (:require [org.httpkit.client :as http]
            [clj-elasticsearch.specs :as specs]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [cheshire.core :as json]
            [clojure.stacktrace :as cst]
            [clojure.pprint :as pp]))

(def ^{:dynamic true} *client*)

(def global-rest-specs
  (let [rs (io/resource "blueprint.edn")
        edn-str (slurp rs)]
    (edn/read-string edn-str)))

(defn parse-json-key
  [^String k]
  (-> k
      (str/replace #"^_" "")
      (.replace \_ \-)
      (keyword)))

(defn- prepare-response
  [{:keys [opts body headers] :as resp}]
  (if (re-find #"json" (get headers :content-type ""))
    (let [decoded (json/decode body parse-json-key)]
      (assoc decoded :headers headers))
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

(defn make-listener [arg] (specs/make-listener :rest arg))

(defn- build-url
  [rest-uri args rest-default]
  (let [interpolated (map #(if (string? %)
                             %
                             (get args % (get rest-default %))) rest-uri)]
    (->> interpolated
         (remove nil?)
         (map (fn [e] (if (coll? e) (str/join "," e) e)))
         (str/join "/"))))

(defn kw->param
  [kw]
  (-> kw (name) (str/lower-case) (str/replace #"\?$" "") (.replace \- \_)))

(defn request-error?
  [{:keys [status] :as resp}]
  (not (and (>= status 200) (< status 300))))

(defn map-query-val
  [v]
  (if (coll? v)
    (str/join "," (map name v))
    (if (keyword? v)
      (kw->param v)
      (str v))))

(defn- make-requester
  [{:keys [symb constructor required
           rest-uri rest-method rest-default
           on-success on-failure aliases]
    :as spec}]
  (let [swap-aliases (if aliases (fn [p] (get aliases p p)) identity)]
    (when (and rest-uri rest-method)
     (let [required-args (concat constructor required)
           uri-args (conj (filter keyword? rest-uri) :source :extra-source :async? :listener)]
       (fn self
         ([client {:keys [source extra-source async? listener] :as args}]
            (let [real-args (if aliases
                              (zipmap (map swap-aliases (keys args)) (vals args))
                              args)
                  expanded-url (build-url rest-uri real-args rest-default)
                  real-source (cond
                               (string? extra-source) extra-source
                               (and extra-source (string? source)) (json/encode extra-source)
                               (string? source) source
                               :else (json/encode (merge source extra-source)))
                  full-uri (str (:base-url client) "/" expanded-url)
                  args-left (apply dissoc real-args uri-args)
                  http-method (if (= rest-method :put/post)
                                (if (:id real-args) :put :post)
                                rest-method)
                  merged-headers (merge (:headers client) (:headers real-args))
                  http-opts (merge client {:url full-uri
                                           :headers merged-headers
                                           :query-params (zipmap (map kw->param (keys args-left))
                                                                 (map map-query-val (vals args-left)))
                                           :method http-method})
                  http-with-body (if source
                                   (assoc http-opts :body real-source)
                                   http-opts)
                  callback (fn [{:keys [opts status body headers error] :as resp}]
                             (if (request-error? resp)
                               (if on-failure
                                 (on-failure resp)
                                 resp)
                               (if on-success
                                 #spy/d (assoc (on-success resp) :http-response (dissoc resp :body))
                                 #spy/d (assoc (prepare-response resp)
                                          :http-response (dissoc resp :body)))))]
              (cond
               async? (http/request http-with-body callback)
               listener (http/request http-with-body listener)
               :default (let [{:keys [error opts] :as resp}
                              (deref (http/request http-with-body callback))]
                          (if error
                            (throw (ex-info (format "error in request with status %s" error)
                                            {:error error :opts opts}))
                            resp)))))
         ([args] (self *client* args)))))))

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

(def implementation (make-implementation! specs/global-specs))

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

(defmacro with-rest-client
  "opens a rest client with given spec"
  [server-spec & body]
  `(binding [*client* (specs/make-client :rest ~server-spec)]
     (do ~@body)))
