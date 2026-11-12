(ns jepsen.ozzy.witness-test
  (:require [clojure.test :refer :all]
            [jepsen.ozzy.finalize :as final]
            [jepsen.ozzy.witness :as witness]))

(deftest evidence-can-be-emitted-without-retaining-the-whole-witness
  (let [observed (atom [])
        events (atom [] :meta {::final/emit! #(swap! observed into %)})]
    (doseq [n (range 1000)]
      (final/evidence! events {:f :poll :ordinal n} {:f :poll :type :ok :ordinal n}))
    (is (= 2000 (count @observed)))
    (is (= (mapcat #(repeat 2 %) (range 1000)) (map :ordinal @observed)))
    (is (= [:invoke :ok] (map :type @events)))
    (is (= [999 999] (map :ordinal @events)))))

(deftest witness-observations-arrive-as-bounded-history-results
  (let [progress (atom nil)]
    (with-redefs [witness/execute!
                  (fn [_ events]
                    (doseq [n (range 1000)]
                      (final/evidence! events {:f :poll :ordinal n}
                                       {:f :poll :type :ok :ordinal n}))
                    {:type :info :f :witnesses :ok? true})]
      (try
        (let [results (loop [results []]
                        (let [result (witness/step! {} progress)
                              results (conj results result)]
                          (if (:scan-complete? result) results (recur results))))]
          (is (every? #(<= (count (:evidence %)) 2) results))
          (is (= (mapcat #(repeat 2 %) (range 1000))
                 (map :ordinal (mapcat :evidence results))))
          (is (not-any? :ok? (butlast results)))
          (is (true? (:ok? (last results)))))
        (finally (witness/close! progress))))))

(deftest a-witness-failure-cannot-report-early-success
  (let [progress (atom nil) executed (atom 0)]
    (with-redefs [witness/execute!
                  (fn [_ events]
                    (swap! executed inc)
                    (final/evidence! events {:f :poll} {:f :poll :type :ok})
                    (throw (ex-info "lost witness" {})))]
      (try
        (is (not (:ok? (witness/step! {} progress))))
        (let [result (witness/step! {} progress)]
          (is (false? (:ok? result)))
          (is (:scan-complete? result))
          (is (re-find #"lost witness" (:error result)))
          (is (= result (witness/step! {} progress)))
          (is (= 1 @executed)))
        (finally (witness/close! progress))))))

(deftest cancellation-stops-a-witness-blocked-on-history-pressure
  (let [progress (atom nil) stopped (promise)]
    (with-redefs [witness/execute!
                  (fn [_ events]
                    (try
                      (dotimes [n 100000]
                        (final/evidence! events {:f :poll :ordinal n}
                                         {:f :poll :type :ok :ordinal n}))
                      (finally (deliver stopped true))))]
      (try
        (is (seq (:evidence (witness/step! {} progress))))
        (witness/close! progress)
        (is (= true (deref stopped 1000 :did-not-stop)))
        (is (nil? @progress))
        (finally (witness/close! progress))))))
