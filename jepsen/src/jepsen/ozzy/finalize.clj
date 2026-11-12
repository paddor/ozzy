(ns jepsen.ozzy.finalize
  (:require [jepsen.ozzy.client :as sdk]
            [jepsen.ozzy.data :as data]
            [jepsen.ozzy.remote :as remote])
  (:import (java.util UUID)))

(defn append-evidence! [events operations]
  (if-let [emit! (::emit! (meta events))]
    (do (reset! events (vec operations)) (emit! @events))
    (swap! events into operations)))

(defn evidence! [events op completion]
  (append-evidence! events [(assoc op :type :invoke) completion]))

(defn send-timed! [test state op]
  ;; Wall-clock bounds place final and witness writes beside node samples.
  (let [start (System/currentTimeMillis)
        result (sdk/send-record test state op)]
    (assoc result :start-ms start :end-ms (System/currentTimeMillis))))

(defn close! [progress]
  (when-let [reader (:reader @progress)]
    (swap! progress dissoc :reader)
    (sdk/close-state! reader)))

(defn failed! [progress error events]
  (swap! progress assoc :error (str error))
  (close! progress)
  {:type :info :ok? false :scan-complete? true :error (str error) :evidence @events})

(defn begin! [test progress round]
  (let [events (atom []) state (atom {})]
    (try
      (when (:error @progress) (throw (ex-info "Earlier final phase failed" {})))
      (close! progress)
      (case round
        :warm
        (do
          (remote/on! test (:nodes test) "heal")
          (remote/on! test (:nodes test) "start")
          (reset! state {:client (sdk/connect test) :session (str (UUID/randomUUID))})
          (let [markers (mapv #(data/event (:run-id test) "marker" % %) (range data/keys-count))]
            (doseq [r markers]
              (let [op (assoc (data/operation r) :f :marker)
                    result (send-timed!
                             (assoc test :confirmation-timeout-ms (* 1000 (:recovery-timeout test 300)))
                             state op)]
                (evidence! events op result)
                (when-not (= :ok (:type result))
                  (throw (ex-info "Marker could not confirm after healing" result)))))
            (swap! progress assoc :markers markers)))
        :cold
        (do
          (when-not (contains? (:completed @progress) :warm)
            (throw (ex-info "Warm scan did not complete" {})))
          (remote/on! test (:nodes test) (if (:lazyfs test) "power-loss" "kill"))
          (remote/on! test (:nodes test) "start")))
      (swap! progress assoc :round round
             :remaining (set (map :id (:markers @progress)))
             :deadline (+ (System/nanoTime) (* (:recovery-timeout test 300) 1000000000))
             :reader (atom {:client (sdk/connect test) :session (str (UUID/randomUUID))}))
      {:type :info :ok? true :evidence @events}
      (catch Throwable error
        (failed! progress error events))
      (finally (when (:client @state) (sdk/close-state! state))))))

(defn step! [test progress round]
  (let [events (atom [])]
    (try
      (when (or (:error @progress) (not= round (:round @progress)) (not (:reader @progress)))
        (throw (ex-info "Final reader is not active" {:round round})))
      (when (> (System/nanoTime) (:deadline @progress))
        (throw (ex-info "Final scan did not reach every marker" {:remaining (:remaining @progress)})))
      ;; Each Jepsen result holds at most two bounded SDK polls. The history
      ;; writer can persist them as the scan advances instead of retaining
      ;; both complete partition scans inside one nemesis completion.
      (doseq [topic data/topics]
        (let [op {:f :poll :topic topic :round round}
              result (sdk/poll-records (:reader @progress) op)
              ;; Opening the reader can time out while the brokers recover
              ;; from the cold restart. No consumer was kept, so the next step
              ;; opens it again from the start; the scan deadline bounds this.
              ;; Any other failure, or one of an open reader, ends the scan.
              opening? (and (= :info (:type result))
                            (re-find #"timed out" (str (:error result)))
                            (nil? (get-in @(:reader @progress) [:consumers topic])))]
          (when (or (seq (:records result)) (not= :ok (:type result)))
            (evidence! events op result))
          (when-not (or (= :ok (:type result)) opening?)
            (throw (ex-info "Final reader failed" result)))
          (swap! progress update :remaining #(reduce disj % (map :id (:records result))))
          (when (empty? (:records result)) (Thread/sleep 10))))
      (let [done? (empty? (:remaining @progress))]
        (when done?
          (close! progress)
          (swap! progress update :completed (fnil conj #{}) round))
        {:type :info :ok? true :scan-complete? done? :evidence @events})
      (catch Throwable error
        (failed! progress error events)))))

(defn complete! [progress]
  (close! progress)
  {:type :info :f :finalize
   :ok? (and (not (:error @progress)) (= #{:warm :cold} (:completed @progress)))
   :error (:error @progress)})
