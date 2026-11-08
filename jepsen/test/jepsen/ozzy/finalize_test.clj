(ns jepsen.ozzy.finalize-test
  (:require [clojure.test :refer :all]
            [jepsen.ozzy.client :as sdk]
            [jepsen.ozzy.finalize :as finalize]
            [jepsen.ozzy.remote :as remote]))

(defn warm-progress []
  (atom {:completed #{:warm} :markers [{:id "confirmed-marker"}]}))

(deftest cold-replay-uses-the-qualified-cache-loss-operation
  (doseq [[lazyfs expected] [[true "power-loss"] [false "kill"]]]
    (let [actions (atom []) progress (warm-progress)]
      (with-redefs [remote/on! (fn [_ nodes command]
                                (is (= ["n0" "n1" "n2"] nodes))
                                (swap! actions conj command))
                    sdk/connect (fn [_] (swap! actions conj :connect) (Object.))]
        (is (:ok? (finalize/begin! {:nodes ["n0" "n1" "n2"] :lazyfs lazyfs}
                                 progress :cold)))
        (is (= [expected "start" :connect] @actions))
        (is (= #{"confirmed-marker"} (:remaining @progress)))))))

(deftest an-unacknowledged-cache-loss-cannot-start-a-cold-reader
  (let [connected (atom false) progress (warm-progress)]
    (with-redefs [remote/on! (fn [_ _ command]
                              (when (= "power-loss" command)
                                (throw (ex-info "Cache loss did not acknowledge" {}))))
                  sdk/connect (fn [_] (reset! connected true) (Object.))]
      (let [result (finalize/begin! {:nodes ["n0" "n1" "n2"] :lazyfs true}
                                  progress :cold)]
        (is (false? (:ok? result)))
        (is (re-find #"Cache loss did not acknowledge" (str (:error result))))
        (is (false? @connected))))))
