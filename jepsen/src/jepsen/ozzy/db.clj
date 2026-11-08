(ns jepsen.ozzy.db
  (:require [jepsen.db :as db]
            [jepsen.ozzy.remote :as remote]))

(defrecord OzzyDB []
  db/DB
  (setup! [_ test node]
    (remote/script! "setup" (name (:backend test)) (str (:lazyfs test false)))
    ;; Each fresh store starts near the controller's clock. Fault/heal cycles
    ;; preserve fractional time instead of accumulating whole-second rounding.
    (remote/script! "clock-set" (str (* (System/currentTimeMillis) 1000000))))
  (teardown! [_ _ _]
    ;; Collection precedes teardown. Preserve stores for explicit collection/down.
    (remote/script! "heal")
    (remote/script! "kill"))
  db/Primary
  (primaries [_ test]
    (let [statuses (remote/on! test (:nodes test) "status")]
      (keep (fn [[node status]]
              (when (re-find #"\"ready\":true" status) node)) statuses)))
  (setup-primary! [_ test node]
    ;; This is called once, after every node has finished setup!.
    (remote/script! "init")
    (remote/on! test (remove #{node} (:nodes test)) "fetch-identity" node)
    (remote/on! test (:nodes test) "format")
    (remote/on! test (:nodes test) "start")
    (remote/on! test (:nodes test) "wait-normal"))
  db/Kill
  (kill! [_ _ _] (remote/script! "kill"))
  (start! [_ _ _] (remote/script! "start"))
  db/Pause
  (pause! [_ _ _] (remote/script! "pause"))
  (resume! [_ _ _] (remote/script! "resume"))
  db/LogFiles
  (log-files [_ _ _]
    {(str remote/root "/run/broker.log") "broker.log"
     (str remote/root "/run/events.jsonl") "events.jsonl"
     (str remote/root "/run/deployment.toml") "deployment.toml"
     (str remote/root "/run/shared.identity") "shared.identity"
     (str remote/root "/run/status.json") "status.json"
     (str remote/root "/run/snapshots.jsonl") "snapshots.jsonl"
     (str remote/root "/run/environment.txt") "environment.txt"
     (str remote/root "/run/qualification.json") "qualification.json"
     (str remote/root "/run/lazyfs.revision") "lazyfs.revision"
     (str remote/root "/run/lazyfs.patch") "lazyfs.patch"
     (str remote/root "/run/lazyfs.log") "lazyfs.log"}))

(defn db [] (->OzzyDB))
