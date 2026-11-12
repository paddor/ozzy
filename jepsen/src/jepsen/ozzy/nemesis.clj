(ns jepsen.ozzy.nemesis
  (:require [jepsen.nemesis :as nemesis]
            [jepsen.ozzy.finalize :as finalize]
            [jepsen.ozzy.remote :as remote]
            [jepsen.ozzy.witness :as witness]))

(def families [:process :network :clock :storage :recovery])

(defn heal! [test]
  (remote/on! test (:nodes test) "heal")
  (remote/on! test (:nodes test) "start"))

(defn inject! [test {:keys [family turn]}]
  (let [nodes (:nodes test)
        node (nth nodes (mod turn 3))
        other (nth nodes (mod (inc turn) 3))
        ip #(get-in test [:test-ips %])]
    (case family
      :process (case (long (mod (quot turn 3) 3))
                 0 (remote/on! test [node] "kill-nowait")
                 1 (remote/on! test [node] "pause")
                 2 (remote/on! test nodes "kill-nowait"))
      :network (case (long (mod (quot turn 3) 4))
                 0 (doseq [a nodes b nodes :when (not= (= a node) (= b node))]
                     (remote/on! test [a] "cut" (ip b)))
                 1 (doseq [a nodes b nodes :when (not= a b)]
                     (remote/on! test [a] "cut" (ip b)))
                 2 (remote/on! test [node] "cut" (ip other))
                 3 (remote/on! test [node] "netem"))
      :clock (remote/on! test [node] "clock"
                         (str (nth [10 -10 3600 -3600] (mod (quot turn 3) 4))))
      :storage (remote/on! test [node] "stall")
      :recovery (let [partition (str (mod turn 6))]
                  (remote/on! test nodes "wait-normal")
                  ;; Hold back one donor so the replacement remains nonvoting.
                  ;; Interrupt it before the generator's next heal can finish
                  ;; recovery; a later fault could otherwise kill a normal copy.
                  (remote/on! test [node] "cut" (ip other))
                  (remote/on! test [other] "cut" (ip node))
                  (remote/on! test [node] "recover" "log" partition)
                  (remote/on! test [node] "wait-nonvoting" "log" partition)
                  (remote/on! test [node] "snapshot")
                  (case (long (mod (quot turn 3) 3))
                    0 (remote/on! test [node] "kill")
                    1 (do (remote/on! test [node] "resume-recovery")
                          (remote/on! test [node] "wait-nonvoting" "log" partition))
                    2 (remote/on! test [node] "pause")))
      :power (do
               (when-not (:lazyfs test) (throw (ex-info "LazyFS was not qualified" {})))
               (remote/on! test (case (long (mod (quot turn 3) 3))
                                 0 [node] 1 [node other] 2 nodes) "power-loss")))
    {:family family :turn turn :target node :effect true}))

(defrecord OzzyNemesis [progress witnesses]
  nemesis/Nemesis
  (setup! [this _] this)
  (invoke! [_ test op]
    (case (:f op)
      :fault (do (remote/on! test (:nodes test) "snapshot")
                 (let [result (inject! test (:value op))]
                   (remote/on! test (:nodes test) "snapshot")
                   (assoc op :type :info :value result)))
      :heal (do (heal! test) (assoc op :type :info :value :healed))
      :witnesses (merge op (witness/step! test witnesses))
      :final-begin (merge op (finalize/begin! test progress (:round op)))
      :final-scan (merge op (finalize/step! test progress (:round op)))
      :finalize (merge op (finalize/complete! progress))))
  (teardown! [_ test]
    (try (witness/close! witnesses)
         (finalize/close! progress)
         (finally (heal! test)))))

(defn nemesis [] (->OzzyNemesis (atom {}) (atom nil)))
