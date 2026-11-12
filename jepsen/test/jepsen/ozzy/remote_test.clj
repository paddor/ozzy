(ns jepsen.ozzy.remote-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.control :as c]
            [jepsen.ozzy.remote :as remote])
  (:import (java.util.concurrent CountDownLatch)))

(deftest command-results-and-errors-survive-the-deadline-wrapper
  (with-redefs [c/exec (fn [& args] (vec args))]
    (is (= ["/opt/ozzy-jepsen/node" "status"] (remote/script! "status"))))
  (let [failure (ex-info "Remote failure" {:exit 1})]
    (with-redefs [c/exec (fn [& _] (throw failure))]
      (is (identical? failure (try (remote/script! "status")
                                  (catch Exception error error)))))))

(deftest a-stalled-command-times-out-and-interrupts-its-worker
  (let [blocked (CountDownLatch. 1)
        stopped (promise)]
    (with-redefs [c/exec (fn [& _]
                          (try (.await blocked)
                               (finally (deliver stopped true))))]
      (binding [remote/*command-timeout-ms* 200]
        (let [failure (try (remote/script! "heal")
                          (catch clojure.lang.ExceptionInfo error error))]
          (is (= {:type :jepsen.ozzy.remote/command-timeout :args '("heal")}
                 (ex-data failure))))))
    (is (= true (deref stopped 1000 :still-blocked)))))
