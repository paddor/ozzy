(ns jepsen.ozzy.witness
  (:refer-clojure :exclude [run!])
  (:require [cheshire.core :as json]
            [clojure.java.shell :as shell]
            [jepsen.ozzy.client :as sdk]
            [jepsen.ozzy.data :as data]
            [jepsen.ozzy.finalize :as final]
            [jepsen.ozzy.remote :as remote])
  (:import (java.io BufferedReader InputStreamReader)
           (java.util UUID)
           (java.util.concurrent ArrayBlockingQueue TimeUnit)
           (java.util.concurrent.atomic AtomicBoolean)))

(defn send! [test state events writer ordinal k]
  (let [r (data/event (:run-id test) writer ordinal k)
        op (assoc (data/operation r) :f :witness)
        result (sdk/send-record (assoc test :confirmation-timeout-ms 300000) state op)]
    (final/evidence! events op result)
    (when-not (= :ok (:type result)) (throw (ex-info "Quorum witness did not confirm" result)))
    r))

(defn read-until! [test events ids label]
  (let [state (atom {:client (sdk/connect test) :session label})
        remaining (atom (set ids))]
    (try
      (remote/wait!
        (fn []
          (doseq [topic data/topics]
            (let [op {:f :poll :topic topic}
                  result (sdk/poll-records state op)]
              (when (or (seq (:records result)) (not= :ok (:type result)))
                (final/evidence! events op result))
              (when-not (= :ok (:type result)) (throw (ex-info "Witness reader failed" result)))
              (swap! remaining #(reduce disj % (map :id (:records result))))
              (when (empty? (:records result)) (Thread/sleep 10))))
          (empty? @remaining))
        (str "Read acknowledged records through " label))
      (finally (sdk/close-state! state)))))

(defn pair-witnesses! [test events]
  (doseq [outsider (:nodes test)
          victim (remove #{outsider} (:nodes test))]
    (remote/on! test (:nodes test) "heal")
    (remote/on! test (:nodes test) "start")
    (remote/on! test [outsider] "pause")
    (let [label (str "pair-" outsider "-" victim)
          state (atom {:client (sdk/connect test) :session label})
          records (try
                    (mapv #(send! test state events label % %) (range 12))
                    (finally (sdk/close-state! state)))]
      (if (:lazyfs test)
        (let [survivor (first (remove #{outsider victim} (:nodes test)))]
          (remote/on! test [victim survivor] "power-loss")
          (remote/on! test [survivor] "start"))
        (remote/on! test [victim] "kill"))
      (remote/on! test [outsider] "resume")
      (read-until! test events (map :id records) label))
    (remote/on! test (:nodes test) "start")))

(defn local! [& args]
  (let [r (apply shell/sh args)]
    (when-not (zero? (:exit r)) (throw (ex-info "Controller probe setup failed" r)))
    r))

(defn recovering-outsider! [test events]
  (let [[_ recovering donor] (:nodes test)
        state (atom {})]
    (remote/on! test (:nodes test) "wait-normal")
    (remote/on! test [recovering] "cut" (get-in test [:test-ips donor]))
    (remote/on! test [donor] "cut" (get-in test [:test-ips recovering]))
    (try
      (remote/on! test [recovering] "recover" "log" "0")
      (remote/on! test [recovering] "wait-nonvoting" "log" "0")
      ;; Keep the SDK connected to all three reports, including the outsider.
      (reset! state {:client (sdk/connect test) :session "recovering-outsider"})
      (doseq [k (range 12)] (send! test state events "recovering-outsider" k k))
      (finally
        (remote/on! test (:nodes test) "heal")
        (remote/on! test (:nodes test) "start")
        (when (:client @state) (sdk/close-state! state))))))

(defn lifecycle! [test events]
  (doseq [k [0 6]]
    (let [topic (data/topic k)
          state (atom {:client (sdk/connect test) :session "lifecycle"})]
      (try
        (send! test state events (str "lifecycle-" k) 0 k)
        (let [^io.ozzy.Producer first-producer (get-in @state [:producers topic])
              identity (.identity first-producer)
              before (:receipt (last @events))]
          (sdk/await (.closeAsync first-producer) 300000)
          (let [^io.ozzy.Client client (:client @state)
                ^io.ozzy.Producer resumed (sdk/await (.producerAsync client topic identity false) 300000)]
            (swap! state assoc-in [:producers topic] resumed)
            (send! test state events (str "lifecycle-" k) 1 k)
            (let [after (:receipt (last @events))]
              (when-not (and (= (:producer-id before) (:producer-id after))
                             (= (:producer-epoch before) (:producer-epoch after))
                             (= (inc (:sequence before)) (:sequence after)))
                (throw (ex-info "Resume changed producer coordinates" {:before before :after after}))))
            (let [takeover (sdk/await (.producerAsync client topic identity true) 300000)]
              (swap! state assoc-in [:producers topic] takeover)
              (send! test state events (str "lifecycle-" k) 2 k)
              (when-not (> (get-in (last @events) [:receipt :producer-epoch])
                           (:producer-epoch before))
                (throw (ex-info "Takeover did not advance the producer epoch" {})))
              (let [r (data/event (:run-id test) (str "fenced-" k) 0 k)
                    op (assoc (data/operation r) :f :witness :forbidden? true)
                    old-state (atom {:client client :producers {topic resumed}})
                    result (sdk/send-record test old-state op)]
                (final/evidence! events op result)
                (when (= :ok (:type result))
                  (throw (ex-info "Fenced producer confirmed a fresh write" result))))
              (try (sdk/await (.closeAsync resumed) 10000)
                   (catch java.util.concurrent.ExecutionException _)))))
        (finally (sdk/close-state! state))))))

(defn line! [^BufferedReader reader prefix]
  (let [result (future
                 (loop []
                   (let [line (.readLine reader)]
                     (cond (nil? line) (throw (ex-info "Probe exited before reply" {}))
                           (.startsWith ^String line prefix) (subs line (count prefix))
                           :else (recur)))))
        value (deref result 60000 ::timeout)]
    (when (= ::timeout value) (throw (ex-info "Probe reply timed out" {:prefix prefix})))
    value))

(defn common-leader! [test]
  (remote/wait!
    (fn []
      (let [reports (remote/on! test (:nodes test) "status")
            rows (map (fn [[_ text]]
                        (->> (:partitions (json/parse-string text true))
                             (filter #(and (= "log" (:topic %)) (zero? (:partition %)))) first))
                      reports)]
        (when (and (= 3 (count rows))
                   (every? #(and (:ready %) (= "Normal" (:state %)) (:leader %)) rows)
                   (apply = (map :leader rows)) (apply = (map :view rows)))
          (:leader (first rows)))))
    "Wait for a common serving leader before the minority cut"))

(defn minority! [test events mode]
  (let [nodes (:nodes test)
        r (data/event (:run-id test) (str "minority-" mode) 0 0)
        warmup (data/event (:id r) "probe-warmup" 0 0)
        warmup-op {:type :invoke :f :witness :record warmup}]
    (local! "bash" "/opt/ozzy-jepsen/probe-net" "up")
    (final/append-evidence! events [warmup-op])
    (let [^java.util.List command
                           ["ip" "netns" "exec" "ozzy-jepsen-probe" "java"
                            "--enable-native-access=ALL-UNNAMED" "-Djava.awt.headless=true"
                            "-cp" (System/getProperty "java.class.path")
                            "clojure.main" "-m" "jepsen.ozzy.probe"
                            (:inventory test) (json/generate-string r)]
          process (.start (.redirectError (ProcessBuilder. command)
                                          java.lang.ProcessBuilder$Redirect/INHERIT))
          reader (BufferedReader. (InputStreamReader. (.getInputStream process)))]
      (try
        (let [receipt (json/parse-string (line! reader "OZZY_READY ") true)]
          (final/append-evidence! events [(assoc warmup-op :type :ok :receipt receipt)]))
        ;; Warmup can overlap recovery from the previous probe. Select the
        ;; current agreed leader only after the child has opened its handles.
        (let [leader (common-leader! test)
              target (some (fn [[node address]] (when (= leader (:id address)) node))
                           (map vector nodes (:addresses test)))
              ip (get-in test [:test-ips target])]
          (when-not target (throw (ex-info "Unknown minority leader" {:leader leader})))
          ;; Handles are open before the cut. The fresh append is submitted only
          ;; after both server and client restrictions have completed.
          (case mode
            :lone (remote/on! test (remove #{target} nodes) "kill")
            :minority (doseq [a nodes b nodes :when (not= (= a target) (= b target))]
                        (remote/on! test [a] "cut" (get-in test [:test-ips b])))
            :recovering
            (let [[recovering donor] (remove #{target} nodes)]
              (remote/on! test [recovering] "cut" (get-in test [:test-ips donor]))
              (remote/on! test [donor] "cut" (get-in test [:test-ips recovering]))
              (remote/on! test [recovering] "recover" "log" "0")
              (remote/on! test [recovering] "wait-nonvoting" "log" "0")
              (remote/on! test [donor] "kill")))
          (local! "bash" "/opt/ozzy-jepsen/probe-net" "restrict" ip)
          (let [input (.getOutputStream process)]
            (.write input (.getBytes "go\n")) (.flush input))
          (let [result (json/parse-string (line! reader "OZZY_RESULT ") true)
                op {:f :witness :record r}
                completion (assoc op :type (keyword (:type result)) :receipt (:receipt result))]
            (final/evidence! events op completion)
            (final/append-evidence! events [{:f :quorum-probe :type :ok :confirmed
                                             (if (= :ok (:type completion)) [(:receipt completion)] [])}])))
        (finally
          (.destroyForcibly process)
          (.waitFor process 10 TimeUnit/SECONDS)
          (local! "bash" "/opt/ozzy-jepsen/probe-net" "down")
          (remote/on! test nodes "heal")
          (remote/on! test nodes "start"))))))

(defn execute! [test events]
  (pair-witnesses! test events)
  (recovering-outsider! test events)
  (lifecycle! test events)
  (doseq [mode [:minority :lone :recovering]]
    (remote/on! test (:nodes test) "wait-normal")
    (minority! test events mode))
  {:type :info :f :witnesses :ok? true})

(defn start! [test]
  (let [queue (ArrayBlockingQueue. 1)
        closed (AtomicBoolean. false)
        events (atom [] :meta {::final/emit! #(.put queue %)})]
    {:queue queue :closed closed
     :worker (future
               (let [result (try (execute! test events)
                                 (catch Throwable error
                                   {:type :info :f :witnesses :ok? false :error (str error)}))]
                 (when-not (.get closed)
                   (.put queue {::complete result}))))}))

(defn close! [progress]
  (when-let [{:keys [queue closed worker]} @progress]
    (.set ^AtomicBoolean closed true)
    (future-cancel worker)
    (.clear ^ArrayBlockingQueue queue)
    (reset! progress nil)))

(defn step! [test progress]
  (when-not @progress (reset! progress (start! test)))
  (let [{:keys [queue result]} @progress]
    (if result
      (assoc result :scan-complete? true)
      (if-let [item (.poll ^ArrayBlockingQueue queue 360 TimeUnit/SECONDS)]
        (if-let [result (::complete item)]
          (do (swap! progress assoc :result result)
              (assoc result :scan-complete? true))
          {:type :info :f :witnesses :scan-complete? false :evidence item})
        (do (close! progress)
            {:type :info :f :witnesses :ok? false :scan-complete? true
             :error "Witness made no progress before its deadline"})))))
