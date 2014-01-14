(ns clj-elasticsearch-rest.core
  (:require [org.httpkit.client :as http]
            [clj-elasticsearch.specs :as specs]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [cheshire.core :as json]
            [clojure.stacktrace :as cst]
            [clojure.pprint :as pp])
  (:use [clojure.pprint :only [cl-format]]))

(def ^{:dynamic true} *client*)

(def preprocess-handlers
  {:bulk-request-body
   (fn [{:keys [actions]}]
     (str
      (str/join "\n"
                (mapcat
                 (fn [[action content]]
                   [(json/encode action) (json/encode content)])
                 actions))
      "\n"))})

(def other-routes
  {:bulk-request
   {:params {:types [:string-or-list-of-strings "String or seq of Strings"]
             :indices [:string-or-list-of-strings "String or seq of Strings"]
             :actions [:list "list of requests of the form {:action-type {:params ...}}"]}}})

(def global-rest-specs
  (let [rs (io/resource "blueprint.edn")
        edn-str (slurp rs)]
    (merge (edn/read-string edn-str)
           other-routes)))

(defn- prepare-response
  [{:keys [opts body headers] :as resp}]
  (if (re-find #"json" (get headers :content-type ""))
    (let [decoded (json/decode body specs/parse-json-key)]
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

(defn- format-params-doc
  [method]
  (cl-format false "~2TParams keys:~%~:{~4T~vA=> ~A~%~}" method))

(defn prepare-rest-doc
  [{:keys [params] :as dynaspec}
   required-args
   default]
  (let [all-required (select-keys params required-args)
        with-default (map (fn [[k [t d]]]
                            (let [default-val (if (k default)
                                                (k default)
                                                "all")]
                              [k [t (str d " (default: " default-val ")")]]))
                          (select-keys all-required (keys default)))
        required (map (fn [[k [t d]]]
                        [k [t (str d " (required)")]])
                      (apply dissoc all-required (keys default)))
        optional (apply dissoc params required-args)
        max-length (apply max
                          (mapcat (fn [coll]
                                    (map (fn [[k _]] (count (name k))) coll))
                                  [required with-default optional]))]
    (reduce (fn [acc mets]
              (reduce (fn [acc [k [typ doc]]]
                        ;; TODO check cl-format syntax
                        (str acc "\n";; (format-params-doc [max-length k doc])
                             ))
                      acc mets))
            "" (map #(sort-by first %) [required with-default optional]))))

(defn- make-requester
  [{:keys [symb constructor required
           rest-uri rest-method rest-default
           on-success on-failure aliases
           rest-preprocess]
    :as spec}]
  (let [swap-aliases (if aliases (fn [p] (get aliases p p)) identity)
        preprocess-fn (when rest-preprocess (preprocess-handlers rest-preprocess))]
    (when (and rest-uri rest-method)
     (let [required-args (concat constructor required)
           kw-method-name (-> symb (name) (keyword))
           dynaspec (get global-rest-specs kw-method-name)
           params-doc (prepare-rest-doc dynaspec required-args rest-default)
           uri-args (conj (filter keyword? rest-uri) :source :extra-source :async? :listener :actions :headers)]
       (vary-meta
        (fn make-request
          ([client {:keys [source extra-source async? listener] :as args}]
             (let [real-args (if aliases
                               (zipmap (map swap-aliases (keys args)) (vals args))
                               args)
                   expanded-url (build-url rest-uri real-args rest-default)
                   real-source (cond
                                rest-preprocess (preprocess-fn args)
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
                   http-with-body (if (or source rest-preprocess)
                                    (assoc http-opts :body real-source)
                                    http-opts)
                   callback (fn [{:keys [opts status body headers error] :as resp}]
                              (if (request-error? resp)
                                (if on-failure
                                  (on-failure resp)
                                  resp)
                                (if on-success
                                  (assoc (on-success resp) :http-response (dissoc resp :body))
                                  (assoc (prepare-response resp)
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
          ([args] (make-request *client* args)))
        assoc :doc params-doc)))))

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
