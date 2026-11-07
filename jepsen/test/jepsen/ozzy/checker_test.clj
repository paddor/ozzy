(ns jepsen.ozzy.checker-test
  (:require [clojure.test :refer [deftest is testing]]
            [cheshire.core :as json]
            [jepsen.checker :as jc]
            [jepsen.client :as jclient]
            [jepsen.core :as jepsen]
            [jepsen.history :as h]
            [jepsen.ozzy.checker :as checker]
            [jepsen.ozzy.client :as sdk]
            [jepsen.ozzy.core :as core]
            [jepsen.ozzy.data :as data]
            [jepsen.ozzy.finalize :as finalize]
            [jepsen.ozzy.remote :as remote]
            [jepsen.ozzy.witness :as witness])
  (:import (java.util.concurrent CompletableFuture)))

(deftest cleanup-attempts-every-resource-and-preserves-errors
  (let [calls (atom [])
        failure (ex-info "consumer close failed" {})
        second-failure (ex-info "client close failed" {})
        closer (fn [role error]
                 (fn []
                   (swap! calls conj role)
                   (if error (throw error) (CompletableFuture/completedFuture nil))))
        caught (try
                 (sdk/close-resources! [(closer :consumer failure)
                                       (closer :producer nil) (closer :client second-failure)])
                 (catch Throwable error error))]
    (is (= [:consumer :producer :client] @calls))
    (is (identical? failure caught))
    (is (= [second-failure] (vec (.getSuppressed ^Throwable caught))))))

(defn append-pair [key offset marker?]
  (let [r (data/event "fixture" (if marker? "marker" "writer") key key)
        op (cond-> (data/operation r) marker? (assoc :f :marker))
        receipt {:topic-id (data/topic key) :incarnation (str "partition-" key)
                 :partition (data/partition key) :offset offset :id (:id r)
                 :producer-id (:writer r) :producer-epoch 1 :sequence offset
                 :disposition "STORED" :policy "QUORUM_DURABLE"}]
    [op (assoc op :type :ok :receipt receipt
               :value [[:send key [offset (:id r)]]])]))

(defn fixture []
  (let [writes (mapcat #(append-pair % 0 false) (range 12))
        markers (mapcat #(append-pair % 1 true) (range 12))
        stored (map #(data/stored (:record %) (:receipt %))
                    (filter checker/completed-write? (concat writes markers)))
        reads (for [round [:warm :cold]]
                {:f :poll :type :ok :round round :session round
                 :records (sort-by (juxt :key :offset) stored)})]
    (vec (concat writes markers reads))))

(defn errors [history] (->> (checker/analyze {} history) :safety :errors (map :type) set))

(deftest complete-history-passes
  (is (:valid? (checker/analyze {} (fixture)))))

(deftest offline-analysis-must-pass-before-returning-success
  (with-redefs [jepsen/log-results identity]
    (let [test {:results {:valid? true}}]
      (is (= test ((core/checked-analysis (constantly test)) {}))))
    (doseq [valid? [false :unknown nil]]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"History did not pass"
                           ((core/checked-analysis (constantly {:results {:valid? valid?}})) {}))))))

(deftest scans-retain-records-without-retaining-empty-polls
  (doseq [scan! [(fn [events marker]
                  (let [progress (atom {:reader (atom {}) :round :cold
                                        :remaining #{(:id marker)}
                                        :deadline (+ (System/nanoTime) 1000000000)})]
                    (loop []
                      (let [result (finalize/step! {} progress :cold)]
                        (swap! events into (:evidence result))
                        (when-not (:scan-complete? result) (recur))))))
                (fn [events marker]
                  (witness/read-until! {} events [(:id marker)] "fixture"))]]
    (let [events (atom [])
          polls (atom 0)
          marker {:id "marker"}]
      (with-redefs [sdk/connect (constantly nil)
                    sdk/close-state! (constantly nil)
                    sdk/poll-records (fn [_ op]
                                       (assoc op :type :ok
                                              :records (if (= 4 (swap! polls inc)) [marker] [])))]
        (scan! events marker))
      (is (= 4 @polls))
      (is (= 2 (count @events)))
      (is (= [marker] (:records (last @events)))))))

(deftest final-reader-failure-ends-the-scan-and-fails-finalization
  (let [closed (atom 0)
        progress (atom {:reader (atom {}) :round :cold :completed #{:warm}
                        :remaining #{"missing"} :deadline Long/MAX_VALUE})]
    (with-redefs [sdk/poll-records (fn [_ op] (assoc op :type :fail :error :broken))
                  sdk/close-state! (fn [_] (swap! closed inc))]
      (let [result (finalize/step! {} progress :cold)]
        (is (:scan-complete? result))
        (is (false? (:ok? result)))
        (is (= :broken (:error (last (:evidence result)))))
        (is (false? (:ok? (finalize/complete! progress))))
        (is (= 1 @closed))))))

(deftest minority-precondition-waits-for-a-common-ready-view
  (let [calls (atom 0)
        row {:topic "log" :partition 0 :leader "current" :view 9
             :state "Normal" :ready true}
        reports (fn [rows]
                  (zipmap ["n0" "n1" "n2"]
                          (map #(json/generate-string {:partitions [%]}) rows)))]
    (with-redefs [remote/on! (fn [& _]
                              (case (swap! calls inc)
                                1 (reports [row row (assoc row :leader "old" :view 8)])
                                2 (reports [row row (assoc row :ready false)])
                                (reports [row row row])))]
      (is (= "current" (witness/common-leader! {:nodes ["n0" "n1" "n2"]}))))
    (is (= 3 @calls))))

(deftest acknowledged-tail-loss-fails
  (let [history (mapv #(if (= :cold (:round %))
                        (update % :records (partial remove (fn [r] (= 1 (:offset r))))) %)
                     (fixture))]
    (is ((errors history) :lost-acknowledged-record))))

(deftest skipped-and-reordered-records-fail
  (doseq [transform [reverse rest]]
    (let [history (mapv #(if (= :cold (:round %)) (update % :records transform) %) (fixture))]
      (is ((errors history) :reader-order)))))

(deftest payload-and-part-boundaries-are-checked
  (let [history (mapv #(if (= :warm (:round %))
                        (update % :records
                                (fn [rs] (cons (assoc (first rs) :parts ["YQ==" ""])
                                               (rest rs)))) %) (fixture))]
    (is ((errors history) :conflicting-offset))
    (is ((errors history) :uninvoked-or-changed-record))))

(deftest duplicated-storage-fails
  (let [history (mapv #(if (= :cold (:round %))
                        (update % :records
                                (fn [rs] (concat rs [(assoc (first rs) :offset 2)]))) %) (fixture))]
    (is ((errors history) :duplicate-storage))))

(deftest local-durable-receipts-fail
  (is ((errors (update-in (fixture) [1 :receipt] assoc :policy "LOCAL_DURABLE"))
       :wrong-durability)))

(deftest retry-result-cannot-change
  (let [history (fixture)
        retry (update-in (nth history 1) [:receipt] assoc :offset 55)]
    (is ((errors (conj history retry)) :changed-retry))))

(deftest unknown-outcome-may-be-present-or-absent
  (let [[invoke complete] (append-pair 0 2 false)
        r (data/event "fixture" "unknown" 100 0)
        invoke (assoc invoke :record r)
        unknown (assoc invoke :type :info :error :timeout)
        stored (data/stored r (assoc (:receipt complete) :offset 2))
        history (vec (concat [invoke unknown] (fixture)))]
    (is (:valid? (checker/analyze {} history)))
    (is (:valid? (checker/analyze {}
                  (mapv #(if (:round %) (update % :records concat [stored]) %) history))))))

(deftest duplicate-receipt-keeps-first-wins-payload
  (let [history (fixture)
        original (nth history 13)
        r (assoc (:record original) :parts ["ZGlmZmVyZW50"])
        duplicate (-> original (assoc :record r)
                      (assoc-in [:receipt :disposition] "DUPLICATE")
                      (assoc-in [:receipt :sequence] 5))]
    (is (:valid? (checker/analyze {} (conj history (assoc duplicate :type :invoke) duplicate))))))

(deftest a-new-dedup-sequence-cannot-claim-stored-again
  (let [history (fixture)
        duplicate (assoc-in (nth history 13) [:receipt :sequence] 50)]
    (is ((errors (conj history duplicate)) :dedup-disposition))))

(deftest empty-history-and-incomplete-coverage-fail
  (is (false? (:valid? (checker/analyze {} []))))
  (is (false? (:valid? (checker/analyze {} (drop 2 (fixture)))))))

(deftest pipelined-partial-successes-are-not-lost
  (let [history (fixture)
        [invoke completion] (take 2 history)
        batch [(assoc invoke :attempts [(:record invoke)])
               (assoc completion :type :info :items [completion])]]
    (is (:valid? (checker/analyze {} (concat batch (drop 2 history))))))
  (let [history (fixture)
        forbidden (assoc (first history) :forbidden? true)]
    (is ((errors (cons forbidden history)) :fenced-record-visible))))

(deftest pipelines-have-independent-operation-intervals
  (let [[a a-ok] (append-pair 0 0 false)
        [b b-ok] (append-pair 1 0 false)
        invocation {:f :send :type :invoke :process 0 :attempts [(:record a) (:record b)]}
        completion (assoc invocation :type :info :items [a-ok (assoc b-ok :type :info)])
        projected (vec (checker/kafka-operations [invocation completion]))]
    (is (= [1 2 1 2] (mapv :process projected)))
    (is (= [:invoke :invoke :ok :info] (mapv :type projected)))
    (is (every? #(= 1 (count (:value %))) projected))))

(defn external-history [corrupt?]
  (let [history (fixture)
        records (->> history (filter checker/completed-write?)
                     (filter #(= :send (:f %)))
                     (map #(data/stored (:record %) (:receipt %))))
        op {:f :poll :topic "log" :session "online" :value [[:poll]]}
        values (into {} (map (fn [r] [(:key r) [[(:offset r) (:id r)]]]) records))
        values (cond-> values corrupt? (assoc-in [0 0 1] (get-in values [1 0 1])))]
    (h/history
      (map #(assoc % :process 0)
           (concat history [(assoc op :type :invoke)
                            (assoc op :type :ok :records records :value [[:poll values]])
                            {:f :finalize :type :info :ok? true}])))))

(deftest nemesis-info-requests-are-paired-with-completions
  (let [request {:type :info :process :nemesis :f :fault
                 :value {:family :recovery :turn 0}}
        complete (assoc-in request [:value :effect] true)]
    (is (= {:valid? true :injections 1 :failed []}
           (checker/fault-analysis {:fault :recovery} [request complete])))
    (is (:valid? (jc/check (checker/check) {:fault :recovery}
                          (h/history (map #(dissoc % :index)
                                          (concat (external-history false) [request complete]))) {})))
    (doseq [history [[request]
                    [complete]
                    [request (assoc complete :error "Injection failed")]
                    [request (assoc-in complete [:value :effect] false)]
                    [request (assoc-in complete [:value :turn] 1)]
                    [request (assoc request :error "indeterminate")]]]
      (is (false? (:valid? (checker/fault-analysis {:fault :recovery} history)))))))

(deftest upstream-kafka-analysis-is-exercised
  (let [result (jc/check (checker/check) {:fault :none} (external-history false) {})]
    (is (:valid? result) (pr-str (dissoc result :coverage))))
  (let [result (jc/check (checker/check) {:fault :none} (external-history true) {})]
    (is (false? (:valid? result)))
    (is (seq (get-in result [:kafka :errors])))))

(deftest failed-drain-survives-worker-teardown-and-still-invalidates-the-run
  (let [state (atom {:client :fixture})
        owner (sdk/->OzzyClient state)
        failure (ex-info "reader close timed out" {})
        drain (with-redefs [sdk/close-resources! (fn [_] (throw failure))]
                (jclient/invoke! owner {} {:f :drain :type :invoke}))
        history (h/history (map #(dissoc % :index)
                                (concat (external-history false) [(assoc drain :process 0)])))
        result (jc/check (checker/check) {:fault :none} history {})]
    (is (= :fail (:type drain)))
    (is (re-find #"reader close timed out" (:error drain)))
    (is (empty? @state))
    (is (nil? (jclient/close! owner {})))
    (is (true? (get-in result [:safety :valid?])))
    (is (false? (get-in result [:availability :valid?])))
    (is (false? (:valid? result)))))
