(ns jepsen.ozzy.client
  (:refer-clojure :exclude [await])
  (:require [jepsen.client :as client]
            [jepsen.ozzy.data :as data])
  (:import (io.ozzy BrokerAddress Client ClientConfig ClientConfig$Capacity ClientConfig$Limits
                    PendingRecord Producer Receipt Record ReaderOptions SendOptions)
           (java.nio ByteBuffer)
           (java.time Duration)
           (java.util UUID)
           (java.util.concurrent CompletableFuture TimeUnit)))

(defn await [^CompletableFuture future millis]
  (try (.get future (long millis) TimeUnit/MILLISECONDS)
       (catch Throwable error
         ;; Cancellation releases an observer, never an admitted record.
         (.cancel future true)
         (when (instance? InterruptedException error) (.interrupt (Thread/currentThread)))
         (throw error))))

(defn receipt [^Receipt r]
  {:topic-id (str (.topicId r)) :incarnation (str (.incarnation r))
   :partition (.partition r) :offset (.offset r) :id (str (.messageId r))
   :producer-id (str (.producerId r)) :producer-epoch (.producerEpoch r)
   :owner-epoch (.ownerEpoch r) :sequence (.sequence r)
   :disposition (str (.disposition r)) :policy (str (.policy r))})

(defn record [topic ^Record r]
  {:topic-id (str (.topicId r)) :incarnation (str (.incarnation r))
   :key (data/log-key topic (.partition r)) :offset (.offset r)
   :id (str (.messageId r))
   :parts (mapv #(data/encode (data/buffer-bytes %)) (.parts r))})

(defn connect [test]
  (let [addresses (mapv (fn [{:keys [id peer data-peer]}]
                          (BrokerAddress. (UUID/fromString id) peer data-peer))
                        (:addresses test))
        config (-> (ClientConfig. addresses)
                   (.withLimits (ClientConfig$Limits. 32768 4096 16 64 2048 2048))
                   (.withCapacity (ClientConfig$Capacity. 12 2 2 4))
                   (.withRequestTimeout (Duration/ofSeconds 5)))]
    (await (Client/connectAsync config) 10000)))

(defn producer [test state topic]
  (or (get-in @state [:producers topic])
      (let [p (await (.producerAsync ^Client (:client @state) topic) 10000)]
        (swap! state assoc-in [:producers topic] p)
        p)))

(defn consumer [state topic]
  (or (get-in @state [:consumers topic])
      (let [c (await (.consumerAsync ^Client (:client @state) topic) 10000)]
        (swap! state assoc-in [:consumers topic] c)
        c)))

(defn submit [^Producer p r]
  (let [options (.withId (SendOptions/partition (data/partition (:key r)))
                         (UUID/fromString (:id r)))
        parts (mapv #(ByteBuffer/wrap (data/decode %)) (:parts r))]
    (await (.sendAsync p ^java.util.List parts options) 5000)))

(defn send-record [test state op]
  (let [r (:record op)]
    (swap! state dissoc :pending)
    (try
      (let [p (producer test state (data/topic (:key r)))
            ^PendingRecord pending (submit p r)]
        (swap! state assoc :pending pending)
        (let [result (receipt (await (.confirmedAsync pending)
                                    (:confirmation-timeout-ms test 5000)))]
          (swap! state dissoc :pending)
          (assoc op :type :ok :receipt result
                 :value [[:send (:key r) [(:offset result) (:id r)]]])))
      (catch Throwable error
        (assoc op :type :info :error (str (class error) ": " (.getMessage error))
               :value [[:send (:key r) [nil (:id r)]]]
               :sequence (some-> ^PendingRecord (:pending @state) .sequence))))))

(defn send-burst [test state op]
  (let [deadline (+ (System/nanoTime) (* 1000000 (:confirmation-timeout-ms test 5000)))
        admitted (mapv (fn [r]
                         (try
                           {:record r :pending (submit (producer test state (data/topic (:key r))) r)}
                           (catch Throwable error {:record r :error (str error)})))
                       (:attempts op))
        items (mapv (fn [{:keys [record pending error]}]
                      (try
                        (when error (throw (ex-info error {})))
                        {:type :ok :f (:f op) :record record
                         :receipt (receipt (await (.confirmedAsync ^PendingRecord pending)
                                                  (max 1 (quot (- deadline (System/nanoTime)) 1000000))))}
                        (catch Throwable failure
                          {:type :info :f (:f op) :record record :error (str failure)
                           :sequence (some-> ^PendingRecord pending .sequence)}))) admitted)]
    (assoc op :type (if (every? #(= :ok (:type %)) items) :ok :info) :items items
           :value (mapv (fn [{:keys [record receipt]}]
                          [:send (:key record) [(:offset receipt) (:id record)]]) items))))

(defn poll-records [state op]
  (let [topic (:topic op)]
    (try
      (let [c (consumer state topic)
            deadline (+ (System/nanoTime) 100000000)
            records (loop [records [] bytes 0]
                      (let [ready (.tryNext ^io.ozzy.Consumer c)]
                        (if (.isPresent ready)
                          (let [r (record topic (.get ready))
                                records (conj records r)
                                bytes (long (+ bytes (reduce + (map count (:parts r)))))]
                            (if (or (>= (count records) 128) (>= bytes 1048576)
                                    (>= (System/nanoTime) deadline)) records
                                (recur records bytes)))
                          records)))
            values (into {} (for [[k rs] (group-by :key records)]
                              [k (mapv (juxt :offset :id) rs)]))]
        (assoc op :type :ok :session (:session @state) :records records
               :mode (keyword topic) :value [[:poll values]]))
      (catch Throwable error
        (assoc op :type :info :error (str error))))))

(defn close-resources! [closers]
  (let [failure (atom nil)]
    (doseq [close closers]
      (try
        (await (close) 300000)
        (catch Throwable error
          (if-let [first-error @failure]
            (when-not (identical? first-error error) (.addSuppressed ^Throwable first-error error))
            (reset! failure error)))))
    (when-let [error @failure] (throw error))))

(defn close-state! [state]
  (when-let [^Client c (:client @state)]
    (try
      (close-resources!
        (concat (map (fn [^io.ozzy.Consumer r] #(.closeAsync r)) (vals (:consumers @state)))
                (map (fn [^Producer p] #(.closeAsync p)) (vals (:producers @state)))
                [#(.closeAsync c)]))
      ;; Drain records the error. Retrying a failed handle in worker teardown
      ;; must not abort the interpreter before the saved history is checked.
      (finally (reset! state {})))))

(defrecord OzzyClient [state]
  client/Client
  (open! [this test _]
    (assoc this :state (atom {:client (connect test) :session (str (UUID/randomUUID))})))
  (setup! [this _] this)
  (invoke! [_ test op]
    (case (:f op)
      (:send :dedup) (if (:attempts op) (send-burst test state op) (send-record test state op))
      :poll (poll-records state op)
      :drain (try (close-state! state) (assoc op :type :ok)
                  (catch Throwable error (assoc op :type :fail :error (str error))))))
  (teardown! [_ _])
  (close! [_ _] (close-state! state))
  client/Reusable
  ;; SDK owners keep admitted records and reconnect across an indeterminate
  ;; operation. Replacing them here can block the generator's quorum heal.
  (reusable? [_ _] true))

(defn client [] (->OzzyClient nil))
