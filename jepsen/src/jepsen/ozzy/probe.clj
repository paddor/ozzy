(ns jepsen.ozzy.probe
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [jepsen.ozzy.client :as sdk]
            [jepsen.ozzy.data :as data]))

(defn -main [inventory record]
  (let [test (edn/read-string (slurp inventory))
        record (json/parse-string record true)
        state (atom {:client (sdk/connect test) :session "minority"})]
    ;; Fresh producers open partitions lazily. Confirm a warmup so the
    ;; minority attempt exercises APPEND on an established producer epoch.
    (let [warmup (data/event (:id record) "probe-warmup" 0 0)
          result (sdk/send-record test state {:f :witness :record warmup})]
      (when-not (= :ok (:type result)) (throw (ex-info "Probe warmup failed" result)))
      (println "OZZY_READY" (json/generate-string (:receipt result))))
    (flush)
    (when-not (= "go" (read-line)) (System/exit 2))
    (let [result (sdk/send-record test state {:f :witness :record record})]
      (println "OZZY_RESULT" (json/generate-string (select-keys result [:type :receipt :error])))
      (flush)
      ;; This process models a client crash; do not wait for an unconfirmed
      ;; producer to drain while its quorum is deliberately unavailable.
      (System/exit 0))))
