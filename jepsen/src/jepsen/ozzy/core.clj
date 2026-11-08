(ns jepsen.ozzy.core
  (:gen-class)
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [jepsen [cli :as cli] [core :as jepsen] [generator :as gen] [tests :as tests]]
            [jepsen.ozzy [checker :as checker] [client :as client] [data :as data]
             [db :as db] [nemesis :as nemesis]]
            [jepsen.store.format :as format])
  (:import (java.util UUID)))

(def options
  [[nil "--inventory FILE" "Controller inventory EDN" :default "inventory.edn"]
   [nil "--backend NAME" "aio or pool" :default :aio :parse-fn keyword
    :validate [#{:aio :pool} "Expected aio or pool"]]
   [nil "--fault NAME" "none, process, network, clock, storage, recovery, combined, power"
    :default :none :parse-fn keyword
    :validate [(set (concat nemesis/families [:none :combined :power])) "Unknown fault family"]]
   [nil "--rate NUMBER" "Total append operations per second" :default 100
    :parse-fn parse-double :validate [pos? "Must be positive"]]
   [nil "--lazyfs" "Require pool/LazyFS qualification before starting"]
   [nil "--witnesses" "Run scripted pair, minority, and recovery witnesses"]
   [nil "--inflight NUMBER" "Outstanding confirmations per writer, 1 through 16" :default 1
    :parse-fn parse-long :validate [#(<= 1 % 16) "Expected 1 through 16"]]
   [nil "--slow-readers" "Poll each live reader once per second"]
   [nil "--recovery-timeout SECONDS" "Deadline for each final confirmation/scan phase"
    :default 300 :parse-fn parse-long :validate [pos? "Must be positive"]]])

(defrecord Writes [run writer inflight ordinal]
  gen/Generator
  (update [this _ _ _] this)
  (op [this test ctx]
    (let [n ordinal k (mod (+ n writer) data/keys-count)
          ;; Adjacent writers periodically race the same dedup identity.
          shared? (and (>= k 6) (zero? (mod n 5)))
          r (data/event run (if shared? "shared" writer)
                        (if shared? (- n (mod n 60)) n) k)
          r (if (and shared? (odd? writer))
              (update r :parts conj (data/encode (.getBytes (str "attempt-" writer "-" n)
                                                            java.nio.charset.StandardCharsets/UTF_8))) r)
          operation (if (= 1 inflight)
                      (data/operation r)
                      (let [records (mapv #(data/event run writer (+ (* n inflight) %) k)
                                          (range inflight))]
                        (assoc (data/operation (first records)) :attempts records
                               :value (mapv (fn [r] [:send k (:id r)]) records))))]
      (when-let [[operation _] (gen/op operation test ctx)]
        [operation (if (= :pending operation) this (update this :ordinal inc))]))))

(defn writes [run writer inflight]
  ;; Jepsen retains the initial generator in the test map. A lazy sequence
  ;; there would keep every emitted payload reachable throughout analysis.
  (->Writes run writer inflight 0))

(defn fault-generator [fault]
  (when-not (= :none fault)
    (mapcat (fn [turn]
              (let [combined [:process :network :clock :storage]
                    family (if (= :combined fault) (nth combined (mod turn 4)) fault)]
                (concat
                  [(gen/sleep 5) {:type :info :f :fault :value {:family family :turn turn}}]
                  (when (= :combined fault)
                    [(gen/sleep 2) {:type :info :f :fault
                                    :value {:family (nth combined (mod (inc turn) 4))
                                            :turn (inc turn)}}])
                  [(gen/sleep 10) {:type :info :f :heal}])))
            (range))))

(defn test-map [opts]
  (let [inventory (edn/read-string (slurp (:inventory opts)))
        run (str (UUID/randomUUID))
        writers (gen/mix (map #(writes run % (:inflight opts 1)) (range 8)))
        clients (gen/reserve 8 (gen/stagger (/ (double (:inflight opts 1)) (:rate opts)) writers)
                             2 (gen/stagger (if (:slow-readers opts) 2 0.05)
                                            (repeat {:f :poll :topic "log" :value [[:poll]]}))
                             (gen/stagger (if (:slow-readers opts) 2 0.05)
                                          (repeat {:f :poll :topic "dedup" :value [[:poll]]})))]
    (when (and (:lazyfs opts) (not= :pool (:backend opts)))
      (throw (ex-info "LazyFS campaigns require --backend pool" {})))
    (when (and (= :power (:fault opts)) (not (:lazyfs opts)))
      (throw (ex-info "Power-loss faults require --lazyfs qualification" {})))
    (merge tests/noop-test opts inventory
           {:name (str "ozzy-" (name (:backend opts)) "-" (name (:fault opts)))
            :run-id run :concurrency 12 :db (db/db) :client (client/client)
            :nemesis (nemesis/nemesis) :checker (checker/check)
            :generator (gen/phases
                         (->> clients
                              (gen/nemesis (fault-generator (:fault opts)))
                              (gen/time-limit (:time-limit opts)))
                         (gen/nemesis (gen/once {:type :info :f :heal}))
                         (gen/clients (gen/each-thread (gen/once {:f :drain})))
                         (when (:witnesses opts)
                           (gen/nemesis
                             (gen/until :scan-complete?
                                        (repeat {:type :info :f :witnesses}))))
                         (gen/nemesis (gen/once {:type :info :f :final-begin :round :warm}))
                         (gen/nemesis
                           (gen/until :scan-complete?
                                      (repeat {:type :info :f :final-scan :round :warm})))
                         (gen/nemesis (gen/once {:type :info :f :final-begin :round :cold}))
                         (gen/nemesis
                           (gen/until :scan-complete?
                                      (repeat {:type :info :f :final-scan :round :cold})))
                         (gen/nemesis (gen/once {:type :info :f :finalize})))
            :required-rounds [:warm :cold]})))

(defn checked-analysis [analyze]
  (fn [options]
    (let [test (analyze options)]
      (jepsen/log-results test)
      ;; Upstream's analyze subcommand exits zero even for an invalid history.
      (when-not (true? (get-in test [:results :valid?]))
        (throw (ex-info "History did not pass analysis"
                        {:valid? (get-in test [:results :valid?])})))
      test)))

(defn -main [& args]
  ;; The upstream 16K-operation chunk can hold gigabytes of multipart reads.
  ;; Bound each encoded history chunk as well as each SDK poll.
  (with-redefs [format/big-vector-chunk-size 256]
    (cli/run! (update-in (cli/single-test-cmd {:opt-spec options :test-fn test-map})
                        ["analyze" :run] checked-analysis) args)))
