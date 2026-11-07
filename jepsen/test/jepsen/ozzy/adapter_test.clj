(ns jepsen.ozzy.adapter-test
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.test :refer [deftest is]]
            [jepsen.checker :as jc]
            [jepsen.core :as jepsen]
            [jepsen.db :as jdb]
            [jepsen.history :as h]
            [jepsen.store :as store]
            [jepsen.ozzy.checker :as checker]
            [jepsen.ozzy.client :as sdk]
            [jepsen.ozzy.core :as core]
            [jepsen.ozzy.data :as data]
            [jepsen.ozzy.finalize :as finalize]
            [jepsen.ozzy.remote :as remote]
            [jepsen.ozzy.witness :as witness])
  (:import (java.net ServerSocket)
           (java.nio.file Files)
           (java.util UUID)
           (java.util.concurrent TimeUnit)))

(defn command! [binary config args]
  (let [process (.start (.redirectErrorStream
                         (ProcessBuilder. ^java.util.List (into [binary "--config" config] args)) true))
        output (future (slurp (.getInputStream process)))]
    (when-not (.waitFor process 60 TimeUnit/SECONDS)
      (.destroyForcibly process)
      (throw (ex-info "Provisioning command timed out" {:args args})))
    (when-not (zero? (.exitValue process))
      (throw (ex-info "Provisioning command failed" {:args args :output @output})))))

(defn fixture []
  (let [binary (or (System/getenv "OZZY_JEPSEN_BROKER")
                   (throw (ex-info "Set OZZY_JEPSEN_BROKER to the built broker" {})))
        root (.toFile (Files/createTempDirectory
                       (.toPath (io/file (or (System/getenv "OZZY_ARTIFACT_ROOT") "/tmp")))
                       "ozzy-jepsen-tcp-" (make-array java.nio.file.attribute.FileAttribute 0)))
        sockets (vec (repeatedly 12 #(ServerSocket. 0)))
        ports (mapv #(.getLocalPort ^ServerSocket %) sockets)
        ids (vec (repeatedly 3 #(str (UUID/randomUUID))))
        config (str (io/file root "deployment.toml"))
        args (fn [n] ["--broker" (str "n" n) "--identity" (str (io/file root "shared.identity"))
                      "--local-identity" (str (io/file root (str "n" n ".identity")))])
        text (str "[cluster]\nmode = \"three\"\n"
                  (apply str (for [topic data/topics]
                               (str "[topics." topic "]\npartitions = 6\nsegment_bytes = 2097152\n"
                                    "max_append_bytes = 1048576\n"
                                    (when (= "dedup" topic)
                                      "[topics.dedup.deduplication]\nwindow_millis = 86400000\nmax_entries = 4096\nmax_bytes = 8388608\n"))))
                  (apply str (for [n (range 3)]
                               (str "[brokers.n" n "]\nid = \"" (nth ids n) "\"\n"
                                    "[brokers.n" n ".endpoints]\n"
                                    (apply str (map-indexed
                                                 (fn [i name]
                                                   (str name " = \"tcp://127.0.0.1:"
                                                        (nth ports (+ (* n 4) i)) "\"\n"))
                                                 ["peer" "reader_pub" "follower_pub" "data_peer"]))
                                    "admin = \"ipc://@jepsen-" (nth ids n) "\"\n"
                                    "[brokers.n" n ".devices.data]\nroot = \"" (io/file root (str "data-" n)) "\"\n"
                                    "controller = \"data\"\n[brokers.n" n ".devices.data.workers]\nbackend = \"pool\"\nwrite_threads = 1\n"
                                    "[[brokers.n" n ".topology.shards]]\nid = 0\ndevice = \"data\"\n"
                                    "[[brokers.n" n ".topology.shards]]\nid = 1\ndevice = \"data\"\n"))))]
    (spit config text)
    (try
      (command! binary config ["init" "--identity" (str (io/file root "shared.identity"))])
      (doseq [n (range 3)]
        (.mkdir (io/file root (str "data-" n)))
        (doseq [step ["init-broker" "init-volumes" "format"]]
          (command! binary config (into [step] (args n)))))
      (finally (doseq [^ServerSocket socket sockets] (.close socket))))
    {:root root :binary binary :config config :args args
     :admin-addresses (mapv #(str "ipc://@jepsen-" %) ids)
     :addresses (mapv (fn [n] {:id (nth ids n)
                               :peer (str "tcp://127.0.0.1:" (nth ports (* n 4)))
                               :data-peer (str "tcp://127.0.0.1:" (nth ports (+ 3 (* n 4))))})
                      (range 3))}))

(defn start! [{:keys [binary config args root]} n]
  (.start (-> (ProcessBuilder. ^java.util.List
                               (into [binary "--config" config "serve"]
                                     (concat (args n) ["--trusted-transport"])))
              (.redirectErrorStream true)
              (.redirectOutput (java.lang.ProcessBuilder$Redirect/appendTo
                                 (io/file root (str "broker-" n ".log")))))))

(defn stop! [^Process p]
  (.destroyForcibly p)
  (when-not (.waitFor p 10 TimeUnit/SECONDS) (throw (ex-info "Broker did not stop" {}))))

(defn await-ready! [test processes]
  (let [probe (or (System/getenv "OZZY_JEPSEN_PROBE")
                  (throw (ex-info "Set OZZY_JEPSEN_PROBE to the built support probe" {})))
        deadline (+ (System/nanoTime) (* 300 1000000000))]
    (loop []
      (doseq [^Process process @processes]
        (when-not (.isAlive process)
          (throw (ex-info "Broker exited during startup; inspect fixture logs"
                          {:root (str (:root test)) :exit (.exitValue process)}))))
      (let [reports (mapv (fn [endpoint]
                            (let [result (shell/sh probe "admin" endpoint)]
                              (when (zero? (:exit result))
                                (json/parse-string (:out result) true))))
                          (:admin-addresses test))
            rows (mapcat :partitions reports)
            ready? (and (= 3 (count (filter some? reports)))
                        (every? #(and (zero? (:missing_shards %))
                                      (= 12 (count (:partitions %)))) reports)
                        (every? #(and (:ready %) (= "Normal" (:state %)) (:leader %)) rows)
                        (every? #(apply = (map (juxt :leader :view) %))
                                (vals (group-by (juxt :topic :partition) rows))))]
        (spit (io/file (:root test) "readiness.jsonl")
              (str (json/generate-string {:time (System/currentTimeMillis) :reports reports}) "\n")
              :append true)
        (when-not ready?
          (when (> (System/nanoTime) deadline)
            (throw (ex-info "Brokers did not reach a common ready view" {:reports reports})))
          (Thread/sleep 100)
          (recur))))))

(defn local-control [test processes]
  (fn [_ nodes command & _]
    (let [indices (map #(if (number? %) % (parse-long (subs % 1))) nodes)]
      (case command
        "heal" (doseq [^Process p @processes :when (.isAlive p)]
                 (shell/sh "kill" "-CONT" (str (.pid p))))
        "kill" (doseq [n indices] (stop! (nth @processes n)))
        "pause" (doseq [n indices]
                  (let [result (shell/sh "kill" "-STOP" (str (.pid ^Process (nth @processes n))))]
                    (when-not (zero? (:exit result)) (throw (ex-info "Pause failed" result)))))
        "resume" (doseq [n indices]
                   (let [result (shell/sh "kill" "-CONT" (str (.pid ^Process (nth @processes n))))]
                     (when-not (zero? (:exit result)) (throw (ex-info "Resume failed" result)))))
        "start" (doseq [n indices]
                  (when-not (.isAlive ^Process (nth @processes n))
                    (swap! processes assoc n (start! test n))))))))

(defn read-all [test count]
  (let [state (atom {:client (sdk/connect test) :session (str (UUID/randomUUID))})
        deadline (+ (System/nanoTime) (* 300 1000000000))]
    (try
      (loop [records []]
        (if (= count (clojure.core/count records)) records
            (do
              (when (> (System/nanoTime) deadline)
                (throw (ex-info "Adapter read deadline" {:count (clojure.core/count records)})))
              (let [results (mapv #(sdk/poll-records state {:f :poll :topic %}) data/topics)]
                (when-not (every? #(= :ok (:type %)) results)
                  (throw (ex-info "Adapter poll failed" {:results results})))
                (when (every? #(empty? (:records %)) results) (Thread/sleep 10))
                (recur (into records (mapcat :records results)))))))
      (finally (sdk/close-state! state)))))

(defn final-history! [test]
  (let [progress (atom {}) events (atom [])]
    (try
      (doseq [round [:warm :cold]]
        (swap! events conj (assoc (finalize/begin! test progress round) :f :final-begin))
        (loop []
          (let [result (assoc (finalize/step! test progress round) :f :final-scan)]
            (swap! events conj result)
            (when-not (:scan-complete? result) (recur)))))
      (conj @events (finalize/complete! progress))
      (finally (finalize/close! progress)))))

(deftest ^:integration java-adapter-over-real-dq-tcp
  (let [test (assoc (fixture) :run-id "adapter" :nodes [0 1 2])
        processes (atom (mapv #(start! test %) (range 3)))
        state (atom {})]
    (try
      (await-ready! test processes)
      (reset! state {:client (sdk/connect test) :session "writer"})
      (let [records (mapv #(data/event "adapter" "writer" % %) (range 12))
            results (mapv #(sdk/send-record test state (data/operation %)) records)
            burst-records (mapv #(data/event "adapter" "burst" (+ % 12) 0) (range 16))
            burst (sdk/send-burst test state (assoc (data/operation (first burst-records)) :attempts burst-records))
            expected (concat records burst-records)]
        (is (every? #(= :ok (:type %)) results) (pr-str (map #(select-keys % [:type :error]) results)))
        (is (= :ok (:type burst)) (pr-str (map #(select-keys % [:type :error]) (:items burst))))
        (is (every? #(= "QUORUM_DURABLE" (get-in % [:receipt :policy])) results))
        (let [duplicate (sdk/send-record test state (data/operation (nth records 6)))]
          (is (= "DUPLICATE" (get-in duplicate [:receipt :disposition])))
          (is (= (get-in (nth results 6) [:receipt :offset]) (get-in duplicate [:receipt :offset]))))
        (sdk/close-state! state)
        (is (= (set (map #(select-keys % [:id :key :parts]) expected))
               (set (map #(select-keys % [:id :key :parts]) (read-all test 28)))))
        (doseq [p @processes] (stop! p))
        (reset! processes (mapv #(start! test %) (range 3)))
        (await-ready! test processes)
        (is (= (set (map :id expected)) (set (map :id (read-all test 28)))))
        (let [events (atom (vec (concat (mapcat vector (map data/operation records) results)
                                       [(assoc burst :type :invoke) burst])))]
          (witness/lifecycle! test events)
          (let [finals (with-redefs [remote/on! (local-control test processes)]
                         (final-history! test))
                final (last finals)
                history (h/history (map #(assoc % :process 0) (into @events finals)))
                result (jc/check (checker/check) (assoc test :fault :none) history {})]
            (is (:ok? final) (:error final))
            (is (:valid? result) (pr-str (dissoc result :coverage))))))
      (finally
        (when (:client @state) (sdk/close-state! state))
        (doseq [p @processes] (stop! p))))))

(deftest ^:integration jepsen-generator-and-checker-over-real-dq-tcp
  (doseq [inflight [1 16]]
    (let [fixture (fixture)
          processes (atom (mapv #(start! fixture %) (range 3)))
          inventory (str (io/file (:root fixture) "inventory.edn"))]
      (spit inventory (pr-str {:nodes ["n0" "n1" "n2"]
                              :addresses (:addresses fixture) :ssh {:dummy? true}}))
      (try
        (await-ready! fixture processes)
        (let [result (with-redefs [remote/on! (local-control fixture processes)
                                  store/base-dir (str (io/file (:root fixture) "history"))]
                       (jepsen/run! (assoc (core/test-map {:inventory inventory :backend :pool
                                                          :rate (if (= inflight 1) 100 500)
                                                          :inflight inflight :time-limit 30 :fault :none})
                                           :db jdb/noop)))]
          (is (true? (get-in result [:results :valid?]))
              (pr-str (:results result))))
        (finally (doseq [p @processes] (stop! p)))))))

(deftest ^:integration acknowledged-records-survive-every-overlapping-pair
  (let [test (assoc (fixture) :run-id "pair-witness" :nodes [0 1 2])
        processes (atom (mapv #(start! test %) (range 3)))
        events (atom [])]
    (try
      (await-ready! test processes)
      (with-redefs [remote/on! (local-control test processes)]
        (witness/pair-witnesses! test events))
      (let [writes (filter checker/completed-write? @events)]
        (is (= 72 (count writes)))
        (is (every? #(= "QUORUM_DURABLE" (get-in % [:receipt :policy])) writes)))
      (finally (doseq [p @processes] (stop! p))))))
