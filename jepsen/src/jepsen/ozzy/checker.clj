(ns jepsen.ozzy.checker
  (:require [clojure.set :as set]
            [cheshire.core :as json]
            [jepsen.checker :as checker]
            [jepsen.history :as h]
            [jepsen.store :as store]
            [jepsen.ozzy.data :as data]
            [jepsen.tests.kafka :as kafka]))

(def write-functions #{:send :dedup :marker :witness})
(defn completed-write? [op]
  (and (= :ok (:type op)) (write-functions (:f op)) (:receipt op)))
(defn coordinate [r] [(:topic-id r) (:incarnation r) (:key r) (:offset r)])
(defn identity-value [r] (select-keys r [:id :parts]))
(defn expanded [history]
  (eduction
    (mapcat (fn [op]
              (cond
                (:evidence op) (cons op (:evidence op))
                (and (= :invoke (:type op)) (:attempts op))
                (map #(assoc (dissoc op :attempts :items) :record %) (:attempts op))
                (:items op) (:items op)
                (:attempts op) (map #(assoc (dissoc op :attempts) :record %) (:attempts op))
                :else [op])))
    history))

(defn kafka-operations
  "A pipeline is overlapping single-record calls, not an application transaction."
  [history]
  (eduction
    (filter #(and (#{:send :poll} (:f %)) (not (:round %))
                  (not= "dedup" (:topic %))))
    (mapcat
      (fn [op]
        (let [process (* 17 (long (or (:process op) 0)))
              invoke? (= :invoke (:type op))]
          (if (:attempts op)
            (map-indexed
              (fn [slot item]
                (let [r (if invoke? item (:record item))
                      result (if invoke? (:id r) [(get-in item [:receipt :offset]) (:id r)])]
                  (merge (select-keys op [:time :f])
                         (select-keys (if invoke? op item) [:type :error])
                         {:process (+ process slot 1) :value [[:send (:key r) result]]})))
              (if invoke? (:attempts op) (:items op)))
            [(assoc (select-keys op [:time :type :f :value :error]) :process process)]))))
    history))

(defn kafka-analysis [history]
  (try
    (kafka/analysis
      (h/history (kafka-operations history))
      {:ww-deps false})
    (catch Exception error
      {:errors {:checker-exception {:class (str (class error))
                                   :message (.getMessage error)}}})))

(defn analyze
  "Independent record oracle. Never uses broker status or on-disk codecs."
  [test history]
  (let [history (expanded history)
        invocations (eduction (filter #(and (= :invoke (:type %))
                                           (write-functions (:f %))))
                              (map #(select-keys % [:record :forbidden?])) history)
        candidates (group-by (comp :id :record) invocations)
        writes (eduction (filter completed-write?) history)
        records (eduction (mapcat :records) history)
        errors (atom [])
        error-count (atom 0)
        bad! (fn [error]
               (swap! error-count inc)
               (swap! errors #(if (< (count %) 32) (conj % error) %)))
        offsets (atom {})
        identities (atom {})
        streams (atom {})
        retries (atom {})]
    (doseq [[identity ops] (group-by #(vector (get-in % [:record :key])
                                             (get-in % [:record :id])) writes)
            :when (>= (first identity) 6)
            :let [stored (->> ops (map :receipt)
                              (filter #(= "STORED" (:disposition %)))
                              (map #(mapv % [:producer-id :producer-epoch :sequence])) set)]
            :when (> (count stored) 1)]
      (bad! {:type :dedup-disposition :identity identity :stored-attempts stored}))
    (doseq [[producer ops] (group-by #(mapv (:receipt %) [:topic-id :incarnation
                                                        :producer-id :producer-epoch :partition]) writes)
            :let [stored (->> ops (map :receipt) (filter #(= "STORED" (:disposition %)))
                              (sort-by :sequence) (partition 2 1))]
            [a b] stored
            :when (and (< (:sequence a) (:sequence b)) (>= (:offset a) (:offset b)))]
      (bad! {:type :producer-order :producer producer :receipts [a b]}))
    (doseq [op writes]
      (let [r (:receipt op)
            retry-key (mapv r [:topic-id :incarnation :producer-id
                              :producer-epoch :partition :sequence])
            result (select-keys r [:offset :id :disposition])]
        (when-not (= "QUORUM_DURABLE" (:policy r))
          (bad! {:type :wrong-durability :op op}))
        (when-not (and (= (:id (:record op)) (:id r))
                       (= (data/partition (:key (:record op))) (:partition r)))
          (bad! {:type :wrong-receipt :op op}))
        (when-let [prior (get @retries retry-key)]
          (when-not (= prior result)
            (bad! {:type :changed-retry :prior prior :op op})))
        (swap! retries assoc retry-key result)))
    (doseq [r (concat (map #(assoc (data/stored (:record %) (:receipt %))
                                  :attempt-only true
                                  :disposition (get-in % [:receipt :disposition])) writes)
                            records)]
      (let [c (coordinate r) value (identity-value r) ik [(:key r) (:id r)]]
        ;; A dedup receipt describes the attempted payload, not the first winner's.
        ;; Only actual reads establish the winning bytes for a duplicate receipt.
        (when-not (and (:attempt-only r) (= "DUPLICATE" (:disposition r)))
          (when-let [prior (get @offsets c)]
            (when-not (= prior value)
              (bad! {:type :conflicting-offset :coordinate c :prior prior :record r})))
          (swap! offsets assoc c value))
        (when-let [prior (get @identities ik)]
          (when-not (= prior c)
            (bad! {:type :duplicate-storage :id ik :coordinates [prior c]})))
        (swap! identities assoc ik c)))
    (doseq [op writes
            :let [r (data/stored (:record op) (:receipt op))
                  observed (get @offsets (coordinate r))]
            :when (and observed (not= (:id r) (:id observed)))]
      (bad! {:type :wrong-dedup-target :record r :observed observed}))
    (doseq [op history r (:records op)]
      (let [stream [(:session op) (:key r)]
            expected (get @streams stream 0)
            possible (get candidates (:id r))]
        (when (some #(and (:forbidden? %) (= (:key r) (get-in % [:record :key]))) possible)
          (bad! {:type :fenced-record-visible :record r}))
        (when-not (= expected (:offset r))
          (bad! {:type :reader-order :stream stream :expected expected :record r}))
        (swap! streams assoc stream (inc (:offset r)))
        (when-not (some #(and (= (:key r) (get-in % [:record :key]))
                             (= (:parts r) (get-in % [:record :parts]))) possible)
          (bad! {:type :uninvoked-or-changed-record :record r}))))
    (doseq [round (:required-rounds test [:warm :cold])]
      (let [observed (into #{} (comp (filter #(= round (:round %)))
                                    (mapcat :records) (map coordinate)) history)
            marker-keys (into #{} (comp (filter #(and (completed-write? %) (= :marker (:f %))))
                                        (map (comp :key :record))) history)]
        (when-not (= (set (range data/keys-count)) marker-keys)
          (bad! {:type :missing-markers :round round :keys marker-keys}))
        (doseq [op writes
                :let [c (coordinate (data/stored (:record op) (:receipt op)))]]
          (when-not (observed c)
            (bad! {:type :lost-acknowledged-record :round round :coordinate c
                   :id (get-in op [:record :id])})))))
    (doseq [op history :when (and (= :ok (:type op)) (= :quorum-probe (:f op)))]
      (when (seq (:confirmed op))
        (bad! {:type :minority-confirmation :op op})))
    (let [counts (frequencies (map (comp :key :record)
                                   (filter #(and (completed-write? %)
                                                 (#{:send :dedup} (:f %))) history)))
          missing (set/difference (set (range data/keys-count)) (set (keys counts)))]
      {:valid? (and (empty? @errors) (empty? missing))
       :safety {:valid? (zero? @error-count) :count @error-count :errors @errors}
       :coverage {:valid? (empty? missing) :missing-write-keys missing :writes counts}})))

(defn fault-analysis [test history]
  ;; Jepsen records both the nemesis request and completion as :info. Its
  ;; single nemesis worker serializes them, including exception completions.
  (let [results (for [[request completion] (partition-all 2 (filter #(= :fault (:f %)) history))]
                  (if (and completion
                           (= (select-keys (:value request) [:family :turn])
                              (select-keys (:value completion) [:family :turn])))
                    completion
                    (assoc request :error :incomplete-or-mismatched-fault)))
        injected (filter #(and (not (:error %)) (true? (get-in % [:value :effect]))) results)
        failed (remove #(and (not (:error %)) (true? (get-in % [:value :effect]))) results)]
    {:valid? (and (empty? failed) (or (= :none (:fault test)) (boolean (seq injected))))
     :injections (count injected) :failed (take 8 failed)}))

(defn check []
  (reify checker/Checker
    (check [_ test history _]
      (let [records (analyze test history)
            ;; No rendering: repository charts require an explicit request.
            external (kafka-analysis history)
            bad (apply dissoc (:errors external)
                       (conj (kafka/allowed-error-types {:ww-deps false :sub-via #{:assign}})
                             :unseen))
            available? (and (boolean (some #(and (= :finalize (:f %)) (:ok? %)) history))
                            (not-any? #(and (= :drain (:f %)) (= :fail (:type %))) history))
            witness? (or (not (:witnesses test))
                         (boolean (some #(and (= :witnesses (:f %)) (:ok? %)) history)))
            faults (fault-analysis test history)
            result (assoc records :valid? (boolean (and (:valid? records) (empty? bad)
                                                        available? witness? (:valid? faults)))
               :availability {:valid? available?}
               :witnesses {:valid? witness?}
               :faults faults
               :kafka {:valid? (empty? bad) :errors bad
                       :observed-error-types (keys (:errors external))})]
        (when (:start-time test)
          (spit (store/path! test "results.json") (json/generate-string result)))
        result))))
