(ns jepsen.ozzy.client-test
  (:require [clojure.test :refer :all]
            [jepsen.generator.interpreter :as interpreter]
            [jepsen.ozzy.client :as sdk]))

(deftest an-indeterminate-write-keeps-the-reconnecting-sdk-session
  (let [opened (atom 0)
        closed (atom 0)
        states (atom [])
        worker (interpreter/->ClientWorker "n0" nil nil #{})]
    (with-redefs [sdk/connect (fn [_] (swap! opened inc) (Object.))
                  sdk/send-record (fn [_ state op]
                                    (swap! states conj state)
                                    (assoc op :type (if (zero? (:process op)) :info :ok)))
                  sdk/close-state! (fn [_] (swap! closed inc))]
      (try
        (is (= :info (:type (interpreter/invoke! worker {:client (sdk/client)}
                                               {:process 0 :f :send}))))
        ;; The interpreter assigns a fresh logical process after :info.
        ;; Closing the old producer here can wait for the quorum's heal,
        ;; while that blocked worker prevents the generator from healing.
        (is (= :ok (:type (interpreter/invoke! worker {:client (sdk/client)}
                                             {:process 12 :f :send}))))
        (is (= 1 @opened))
        (is (zero? @closed))
        (is (identical? (first @states) (second @states)))
        (finally (interpreter/close! worker {})))
      (is (= 1 @closed)))))
