(ns jepsen.ozzy.core-test
  (:require [clojure.test :refer :all]
            [jepsen.generator :as gen]
            [jepsen.ozzy.core :as core]))

(deftest quiet-after-heal-delays-only-the-next-fault
  (let [plain (take 5 (core/fault-generator :power 0))
        quiet (take 5 (core/fault-generator :power 75))]
    (is (= [:fault :heal] (keep :f (take 4 plain))))
    (is (= (gen/sleep 5) (nth plain 4)))
    (is (= [:fault :heal] (keep :f (take 4 quiet))))
    (is (= (gen/sleep 75) (nth quiet 4)))
    (is (= :fault (:f (nth (core/fault-generator :power 75) 6))))))
