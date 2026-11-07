(ns jepsen.ozzy.remote
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [jepsen.control :as c]
            [jepsen.util :as util]))

(def root "/opt/ozzy-jepsen")
(def ^:dynamic *command-timeout-ms* 360000)
(defn script! [& args]
  ;; A guest's blocked disk I/O can keep an authenticated SSH channel open
  ;; indefinitely. Bound the controller's wait even when the guest cannot exit.
  (util/timeout *command-timeout-ms*
    (throw (ex-info "Node command timed out" {:type ::command-timeout :args args}))
    (apply c/exec (str root "/node") args)))
(defn on! [test nodes & args]
  (c/on-nodes test nodes (fn [_ _] (apply script! args))))
(defn status! [] (json/parse-string (script! "status") true))
(defn wait! [predicate message]
  (util/await-fn #(or (predicate) (throw (ex-info message {})))
                 {:timeout 300000 :retry-interval 100 :log-interval 10000
                  :log-message message}))
(defn command! [command]
  (c/exec :bash :-c command))
(defn quote-sh [s] (str "'" (str/replace (str s) "'" "'\\''") "'"))
