(ns jepsen.ozzy.data
  (:refer-clojure :exclude [partition])
  (:import (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.util Base64 UUID)))

(def topics ["log" "dedup"])
(def keys-count 12)
(defn topic [k] (nth topics (quot k 6)))
(defn partition [k] (mod k 6))
(defn log-key [topic partition] (+ (* 6 (.indexOf ^java.util.List topics topic)) partition))
(defn encode [^bytes bytes] (.encodeToString (Base64/getEncoder) bytes))
(defn decode [^String text] (.decode (Base64/getDecoder) text))
(defn buffer-bytes [^ByteBuffer buffer]
  (let [copy (.duplicate buffer) bytes (byte-array (.remaining copy))]
    (.get copy bytes)
    bytes))

(defn event [run writer ordinal k]
  (let [id (str (UUID/nameUUIDFromBytes
                 (.getBytes (str run "/" writer "/" ordinal) StandardCharsets/UTF_8)))
        head (.getBytes (pr-str {:id id :writer writer :ordinal ordinal :key k})
                        StandardCharsets/UTF_8)
        padding (byte-array 1024 (byte (mod ordinal 127)))
        parts (if (zero? (mod ordinal 7)) [head (byte-array 0) padding]
                  [(byte-array (concat head padding))])]
    {:id id :writer writer :ordinal ordinal :key k :parts (mapv encode parts)}))

(defn operation [record]
  {:type :invoke :f (if (< (:key record) 6) :send :dedup)
   :record record :value [[:send (:key record) (:id record)]]})

(defn stored [record receipt]
  (merge record (select-keys receipt [:topic-id :incarnation :offset])))
