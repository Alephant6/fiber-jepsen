(ns jepsen.fiber.nonce
  "Checks MuSig2 nonce use, from the records that fnn builds instrumented with
  instrument/ (cargo feature nonce-oracle) append to /fiber/nonce-oracle.log. Each node's
  file is pooled across its restarts.

  A nonce is identified by a hash of its public nonce, a signing context by a hash of the
  aggregated key, the aggregate nonce and the message. Secret nonces are never recorded.
  With two-nonce MuSig2, partial signatures from one secret nonce over three different
  contexts reveal the signing key, so a nonce whose partial signature left the node in
  two different contexts is a violation: one more revealed signature would be enough.

  A clean result means as much as the run exercised, so each node also reports how many of
  its processes wrote records (restarts), how many channel reestablishments started and how
  often they resent a stored CommitmentSigned or RevokeAndAck verbatim (V2), how often V2
  returned a stored signature and how often it did so after a restart, how often a later
  process signed the same context again, and how often V2 refused a conflicting context."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.store :as store]))

(def log-name
  "The record file, as Jepsen downloads it into each node's store directory."
  "nonce-oracle.log")

(defn parse-line
  "\"NONCE-ORACLE Sent nonce=.. context=.. site=.. label=.. pid=.. boot=..\" ->
  {:kind \"Sent\", :nonce .., :context .., ...}, or nil for any other line."
  [line]
  (when-let [[_ kind fields] (re-find #"^NONCE-ORACLE (\w+) (.*)$" line)]
    (into {:kind kind}
          (for [field (str/split fields #" ")
                :let  [[k v] (str/split field #"=" 2)]
                :when v]
            [(keyword k) v]))))

(defn proc
  "The process that wrote a record. pid alone could repeat after a restart."
  [record]
  (str (:pid record) "/" (:boot record)))

(def signature-kinds #{"Sent" "Aggregated"})

(defn contexts
  "{context [record ...]} for one nonce's records of the given kind."
  [records kind]
  (group-by :context (filter #(= kind (:kind %)) records)))

(defn nonce-summary
  [nonce records]
  (let [sent (contexts records "Sent")
        agg  (contexts records "Aggregated")]
    {:nonce          nonce
     :sent           sent
     :agg            agg
     :violation?     (<= 2 (count sent))
     ; Three revealed contexts give the key: three sent, or two sent and a published one.
     :revealable?    (or (<= 3 (count sent))
                         (and (<= 2 (count sent)) (some #(not (contains? sent %)) (keys agg))))
     :procs          (set (map proc records))
     ; The same context signed again by a later process: the signature was not persisted,
     ; but the restarted node signed exactly what it had signed before.
     :resigned-after-restart? (some #(< 1 (count (set (map proc %)))) (vals sent))}))

(defn describe-violation
  [{:keys [nonce sent agg procs]}]
  {:nonce           nonce
   :across-restarts (< 1 (count procs))
   :sent            (vec (for [[ctx rs] sent]
                           {:context ctx
                            :sites   (into (sorted-set) (map :site rs))
                            :labels  (into (sorted-set) (map :label rs))
                            :procs   (into (sorted-set) (map proc rs))}))
   :aggregated      (vec (keys agg))})

(defn analyze
  "Summarises one node's records, in file order."
  [records]
  (let [records  (vec records)
        sigs     (filter #(signature-kinds (:kind %)) records)
        nonces   (->> sigs
                      (group-by :nonce)
                      (map (fn [[n rs]] (nonce-summary n rs))))
        by-nonce (into {} (map (juxt :nonce identity)) nonces)
        ; The process of the first signature made with each nonce.
        first-proc (reduce (fn [m r]
                             (if (contains? m (:nonce r)) m (assoc m (:nonce r) (proc r))))
                           {}
                           (filter #(= "Sent" (:kind %)) records))
        cached   (filter #(= "Cached" (:kind %)) records)
        cached-ctx-mismatch (filter #(let [s (get-in by-nonce [(:nonce %) :sent])]
                                       (and s (not (contains? s (:context %)))))
                                    cached)
        violations (filter :violation? nonces)]
    {:records                  (count records)
     :processes                (count (distinct (map proc records)))
     :signatures               (count sigs)
     :nonces                   (count nonces)
     :violation-count          (count violations)
     :revealable-count         (count (filter :revealable? nonces))
     :violations               (vec (take 10 (map describe-violation violations)))
     ; V1 signs each commitment nonce once for the peer and also aggregates its own
     ; commitment transaction with it; at most one aggregated transaction can be published.
     :sent-once-aggregated-elsewhere
     (count (filter #(and (= 1 (count (:sent %)))
                          (some (fn [c] (not (contains? (:sent %) c))) (keys (:agg %))))
                    nonces))
     :aggregated-2plus         (count (filter #(<= 2 (count (:agg %))) nonces))
     :cached                   (count cached)
     :cached-after-restart     (count (filter #(let [p (first-proc (:nonce %))]
                                                 (and p (not= p (proc %))))
                                              cached))
     :cached-without-signature (count (remove #(first-proc (:nonce %)) cached))
     :cached-context-mismatch  (count cached-ctx-mismatch)
     :resigned-after-restart   (count (filter :resigned-after-restart? nonces))
     :nonces-across-restarts   (count (filter #(< 1 (count (:procs %))) nonces))
     :conflicts                (count (filter #(= "Conflict" (:kind %)) records))
     :reestablish              (frequencies (map #(str "v" (:v %))
                                                 (filter #(= "Reestablish" (:kind %)) records)))
     :replayed-cs              (count (filter #(and (= "Replay" (:kind %)) (= "true" (:cs %))) records))
     :replayed-ack             (count (filter #(and (= "Replay" (:kind %)) (= "true" (:ack %))) records))}))

(defn read-records
  [file]
  (with-open [r (io/reader file)]
    (into [] (keep parse-line) (line-seq r))))

(defn checker
  "Fails the test if any node sent partial signatures for one nonce in two different
  contexts. Unknown if a node has no record file, i.e. fnn was not built with the
  nonce-oracle feature."
  []
  (reify checker/Checker
    (check [_ test _history _opts]
      (let [files   (into {} (for [node (:nodes test)]
                               [node (store/path test (name node) log-name)]))
            missing (vec (sort (for [[node f] files :when (not (.exists (io/file f)))] node)))
            nodes   (into (sorted-map)
                          (for [[node f] files :when (.exists (io/file f))]
                            [node (analyze (read-records f))]))
            total   (reduce + (map :violation-count (vals nodes)))]
        {:valid?           (cond (pos? total)    false
                                 (seq missing)   :unknown
                                 (zero? (reduce + (map :signatures (vals nodes)))) :unknown
                                 :else           true)
         :nonce-violations total
         :missing          missing
         :nodes            nodes}))))
