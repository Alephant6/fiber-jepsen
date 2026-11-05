(ns jepsen.fiber.faults.clock
  "The clock fault (C-14 of the chaos-test plan): fnn's wall clock jumps forward
  or back on one node or on all of them, and later jumps back to the real time.

  Fiber expires TLCs by wall-clock time, so a node whose clock is off may see a
  TLC as expired, or not yet, when its peer doesn't. Jepsen's own clock nemesis
  sets the host's clock, which every container shares, so it can't skew one
  node. Here fnn runs under libfaketime instead (jepsen.fiber.db adds the
  environment from `env` when this fault is in use): the library takes the
  offset from a file, which a :clock-jump op rewrites, and rereads it within a
  second. Only the wall clock (CLOCK_REALTIME) moves; monotonic clocks keep
  running, as when NTP steps a real clock. fnn measures its 30 s peer-response
  deadline with the wall clock (SystemTime), so a forward jump of more than
  30 s while a channel waits for an ack can make fnn force-close it. The offset
  outlives restarts: a killed fnn starts with it.

  After each change the op asks fnn for an invoice and compares the invoice's
  timestamp, from fnn's clock, with the control node's, so a run shows whether
  fnn's clock really moved; the checker fails the run when it didn't."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [warn]]
            [jepsen [checker :as checker]
                    [control :as c]
                    [nemesis :as n]
                    [util :as util]]
            [jepsen.fiber [rpc :as rpc]
                          [schedule :as schedule]])
  (:import (java.util SplittableRandom)))

(def offset-file
  "libfaketime's FAKETIME_TIMESTAMP_FILE on each node: an offset such as +300 or
  -60 (seconds). docker/node/Dockerfile creates it with +0."
  "/fiber/faketime")

(def library
  "libfaketime's multi-threaded build; docker/node/Dockerfile links it here."
  "/usr/local/lib/libfaketimeMT.so.1")

(defn env
  "The environment fnn starts with when this fault is in use."
  [_opts]
  {:LD_PRELOAD                   library
   :FAKETIME_TIMESTAMP_FILE      offset-file
   ; How often, in seconds, libfaketime rereads the file.
   :FAKETIME_CACHE_DURATION      "1"
   ; Leave CLOCK_MONOTONIC alone, as a real clock step does.
   :FAKETIME_DONT_FAKE_MONOTONIC "1"})

(def target-specs
  #{:one :all})

(defn parse-range
  "\"60-1800\" -> [60 1800]; \"300\" -> [300 300]. Seconds."
  [s]
  (let [[lo hi] (map parse-long (str/split s #"-" 2))]
    [lo (or hi lo)]))

(defn spec
  "The offset file's text for `offset-s` seconds."
  [offset-s]
  (str (if (neg? offset-s) "-" "+") (abs (long offset-s))))

(def settle-ms
  "How long a jump waits before checking fnn's clock: libfaketime rereads the
  offset file once a second."
  2500)

(defn set-offset!
  "Writes the offset on the current node, atomically, so that libfaketime never
  reads a half-written file."
  [offset-s]
  (c/su
    (c/exec :echo (spec offset-s) :> (str offset-file ".tmp"))
    (c/exec :mv (str offset-file ".tmp") offset-file)))

(defn invoice-timestamp-ms
  "The timestamp of a new_invoice result, in ms. Fiber's invoices carry ms since
  the epoch; an older build's seconds are scaled."
  [result]
  (when-let [t (some-> (get-in result [:invoice :data :timestamp]) rpc/hex->int long)]
    (if (< t 100000000000) (* 1000 t) t)))

(def check-timeout-ms
  "How long the clock check waits for fnn's invoice. A paused fnn never answers,
  and the nemesis does nothing else meanwhile, so with other faults a long wait
  stretches theirs."
  5000)

(defn fnn-offset-s
  "fnn's clock offset on `node`, in seconds to a tenth: an invoice's timestamp
  (fnn's clock) minus the control node's time halfway through the call. A
  string when fnn didn't answer."
  [node]
  (try
    (let [t0  (System/currentTimeMillis)
          res (rpc/call node "new_invoice"
                        {:amount      (rpc/int->hex 1000000)
                         :currency    "Fibd"
                         :description "fiber-jepsen clock check"
                         :expiry      (rpc/int->hex 3600)}
                        {:timeout check-timeout-ms})
          t1  (System/currentTimeMillis)
          ts  (invoice-timestamp-ms res)]
      (if ts
        (/ (Math/round (/ (- ts (/ (+ t0 t1) 2.0)) 100.0)) 10.0)
        "no invoice timestamp"))
    (catch Exception e
      (warn "clock nemesis: couldn't read fnn's clock on" node (.getMessage e))
      (str "failed: " (or (.getMessage e) (.getSimpleName (class e)))))))

(defn clock-nemesis
  []
  (reify
    n/Reflection
    (fs [_] #{:clock-jump :clock-reset})

    n/Nemesis
    (setup! [this test]
      (c/on-nodes test (fn [_ _] (set-offset! 0)))
      this)

    (invoke! [_ test op]
      (let [[nodes offset] (case (:f op)
                             :clock-jump  [(:nodes (:value op)) (:offset-s (:value op))]
                             :clock-reset [(:nodes test) 0])]
        (c/on-nodes test nodes (fn [_ _] (set-offset! offset)))
        (Thread/sleep settle-ms)
        (assoc op :value
               {:offset-s offset
                ; In parallel: a paused node makes its check wait check-timeout-ms.
                :fnn-offset-s (into (sorted-map) (util/real-pmap (fn [node] [node (fnn-offset-s node)]) nodes))})))

    (teardown! [_ test]
      (c/on-nodes test (fn [_ _] (set-offset! 0))))))

(defn package
  [opts]
  {:generator       nil
   :final-generator (when ((:faults opts) :clock) {:type :info, :f :clock-reset, :value nil})
   :nemesis         (clock-nemesis)
   :perf            #{{:name  "clock"
                       :start #{:clock-jump}
                       :stop  #{:clock-reset}
                       :color "#C4A0E9"}}})

(defn ops
  "Endless pairs of a :clock-jump, its targets and offset drawn from `r`, and a
  :clock-reset. Options: :clock-targets (specs to choose from) and :clock-jump
  ([min max] seconds; the size is drawn uniformly, the direction at random)."
  [^SplittableRandom r opts]
  (let [nodes   (vec (:nodes opts))
        specs   (vec (:clock-targets opts [:one]))
        [lo hi] (:clock-jump opts [60 1800])]
    (mapcat (fn [jump]
              [{:type :info, :f :clock-jump, :value jump}
               {:type :info, :f :clock-reset, :value nil}])
            (repeatedly
              (fn []
                (let [targets (case (schedule/pick r specs)
                                :one [(schedule/pick r nodes)]
                                :all nodes)
                      size    (+ lo (.nextInt r (int (inc (- hi lo)))))
                      sign    (if (.nextBoolean r) 1 -1)]
                  {:nodes targets, :offset-s (* sign size)}))))))

(def tolerance-s
  "How far fnn's measured offset may be from the one set: the invoice call's
  own time and libfaketime's one-second rereads."
  5)

(defn check
  "Every :clock-jump and :clock-reset completion in `history`, with fnn's offset
  as measured: valid when each node measured is within tolerance-s of the offset
  set. A node that didn't answer (down or paused, when combined with other
  faults) doesn't count; :unknown when no change was measured at all."
  [history]
  (let [changes  (for [op    history
                       :when (and (#{:clock-jump :clock-reset} (:f op)) (map? (:value op)))
                       [node m] (:fnn-offset-s (:value op))]
                   {:time (:time op), :f (:f op), :node node, :set (:offset-s (:value op)), :measured m})
        measured (filter (comp number? :measured) changes)
        off      (vec (remove #(<= (abs (- (:measured %) (:set %))) tolerance-s) measured))]
    {:valid?     (cond (empty? measured) :unknown
                       (seq off)         false
                       :else             true)
     :changes    (count changes)
     :measured   (count measured)
     :unmeasured (count (remove (comp number? :measured) changes))
     :offsets-s  (vec (sort (distinct (map :set (filter #(= :clock-jump (:f %)) changes)))))
     :not-taken  off}))

(defn checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (check history))))

(def fault
  {:faults  #{:clock}
   :stream  :clock
   :package package
   :ops     ops
   :env     env
   :checker (fn [_opts] {:clock (checker)})
   :fired   {:clock #{:clock-jump}}})
