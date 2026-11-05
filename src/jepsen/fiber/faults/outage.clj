(ns jepsen.fiber.faults.outage
  "The outage fault (C-03 of the chaos-test plan): a node stays down past the
  expiry of the TLCs its neighbours offered it, and they take them back on chain.

  Each :outage op is one outage of one node: with --outage-targets middle, a
  node that forwards payments (n2 in line); with one, any node. The op waits up
  to --outage-await seconds for a neighbour to hold a TLC offered to the target
  that the target has acknowledged and not removed, so that a payment is in
  flight when the node goes down. Then it kills the target's fnn (SIGKILL, or
  SIGTERM with --kill-signal term), keeps it down for a time drawn from
  --outage-duration, and starts it again.

  A release build of fnn makes a payment's TLC last hours: every route adds a
  random privacy delay of at least 4 hours to each hop's expiry (Fiber's
  graph.rs, rand_tlc_expiry_delta), so no outage of a run could outlast one. With
  --outage-jump SECONDS, an outage that caught a TLC in flight moves every
  clock forward by that much once the target is down: each fnn's, through
  libfaketime as the clock fault does (jepsen.fiber.faults.clock), and the CKB
  node's, through the offset file that the chain container's ckb reads
  (docker/chain/clockshift.c, in the chain-clock volume). The chain's clock
  has to move too: a node takes an expired TLC back with a transaction whose
  since is a timestamp, which CKB checks against the median time of the last
  37 blocks. Block times can't go back, so the clocks never jump back; each
  jump adds to the last, and the containers need recreating after a run
  (scripts/run.sh does).

  The op records the TLCs caught (offered to the target and still unresolved
  after the kill) and, after a jump, every clock's offset as measured: each
  fnn's from an invoice's timestamp, the chain's from its tip block's. The
  checker fails the run when a measured offset is more than 5 s off the one set,
  and is :unknown when --outage-jump was asked for but no outage caught a TLC."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [control :as c]
                    [db :as db]
                    [nemesis :as n]]
            [jepsen.fiber [chain :as chain]
                          [db :as fdb]
                          [rpc :as rpc]
                          [schedule :as schedule]
                          [topology :as topology]]
            [jepsen.fiber.faults [clock :as clock]
                                 [process :as process]])
  (:import (java.nio.file CopyOption Files StandardCopyOption)))

(def target-specs
  #{:one :middle})

(def chain-offset-file
  "The CKB node's clock offset file, as the control node sees it: the
  chain-clock volume that docker/docker-compose.yml mounts in the chain and
  control containers. docker/chain/entrypoint.sh writes +0 at each start."
  "/chain-clock/offset")

(defn read-chain-offset
  "The offset in the chain's offset file, in seconds; nil when there is no file."
  []
  (let [f (io/file chain-offset-file)]
    (when (.exists f)
      (parse-long (str/trim (slurp f))))))

(defn write-chain-offset!
  "Writes the chain's offset file atomically, so that ckb never reads a
  half-written file."
  [offset-s]
  (let [tmp (io/file (str chain-offset-file ".tmp"))]
    (spit tmp (str (clock/spec offset-s) "\n"))
    (Files/move (.toPath tmp) (.toPath (io/file chain-offset-file))
                (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE]))))

(defn outbound-state
  "The state of an outbound pending_tlcs entry (\"Committed\", ...), nil for an
  inbound one. Statuses are {:Outbound s}, or a string in older builds."
  [tlc]
  (let [s (:status tlc)]
    (if (map? s)
      (some-> (:Outbound s) name)
      (second (re-find #"^Outbound\((\w+)\)$" (str s))))))

(defn unresolved-offer?
  "Whether a neighbour's pending_tlcs entry is a TLC it offered that the peer has
  acknowledged and that isn't removed: one the neighbour force-closes the
  channel over when it nears expiry (fiber-types get_expired_offered_tlcs)."
  [tlc]
  (let [state (outbound-state tlc)]
    (boolean (and state (not (#{"LocalAnnounced" "RemoveAckConfirmed"} state))))))

(defn in-flight
  "The unresolved TLCs that `target`'s neighbours have offered it, read from each
  neighbour that answers: [{:from :channel :hash :amount :expiry :state}]."
  [test target]
  (let [pubkey (get @(:pubkeys test) target)]
    (vec (for [nb  (sort (get (topology/neighbours (topology/edges test)) target))
               ch  (try (:channels (rpc/call nb "list_channels" {:pubkey pubkey} {:timeout 2000}))
                        (catch Exception _ nil))
               tlc (:pending_tlcs ch)
               :when (unresolved-offer? tlc)]
           {:from    nb
            :channel (:channel_id ch)
            :hash    (:payment_hash tlc)
            :amount  (some-> (:amount tlc) rpc/hex->int long)
            :expiry  (some-> (:expiry tlc) rpc/hex->int long)
            :state   (outbound-state tlc)}))))

(defn await-in-flight
  "Polls `target`'s neighbours every 50 ms until one holds a TLC offered to the
  target, for up to `await-s` seconds. Returns how long it waited, in ms."
  [test target await-s]
  (let [t0       (System/currentTimeMillis)
        deadline (+ t0 (* 1000 await-s))]
    (loop []
      (let [now (System/currentTimeMillis)]
        (if (or (<= deadline now) (seq (in-flight test target)))
          (- now t0)
          (do (Thread/sleep 50)
              (recur)))))))

(defn chain-offset-s
  "The CKB node's clock offset in seconds, to a tenth: its tip block's timestamp
  minus the control node's time. With a block a second it reads up to about a
  second behind. A string when the chain didn't answer."
  []
  (try
    (let [now (System/currentTimeMillis)
          ts  (chain/tip-time-ms)]
      (/ (Math/round (/ (- ts now) 100.0)) 10.0))
    (catch Exception e
      (str "failed: " (or (.getMessage e) (.getSimpleName (class e)))))))

(defn await-chain-offset
  "Reads the chain's offset once a second, for up to 30 s, until it is within
  clock/tolerance-s of `offset-s`; returns the last reading. A block made from a
  template taken before the jump still carries the old time."
  [offset-s]
  (loop [tries 30]
    (let [m (chain-offset-s)]
      (if (or (zero? tries) (and (number? m) (<= (abs (- m offset-s)) clock/tolerance-s)))
        m
        (do (Thread/sleep 1000)
            (recur (dec tries)))))))

(defn- on-node!
  "Calls (f db test node) on `node` over SSH."
  [test node f]
  (c/on-nodes test [node] (fn [t nd] (f (:db t) t nd))))

(defn jump!
  "Moves every clock to `offset-s`: the chain's, then each node's, the down
  target's included, so that it starts with the others' time. Waits for
  the clocks to reread the files, and returns the offsets measured on the
  nodes that are up and on the chain."
  [test target offset-s]
  (info "outage: every clock jumps to" (clock/spec offset-s) "s")
  (write-chain-offset! offset-s)
  (c/on-nodes test (fn [_ _] (clock/set-offset! offset-s)))
  (Thread/sleep clock/settle-ms)
  {:fnn-offset-s   (into (sorted-map) (for [node (:nodes test) :when (not= node target)]
                                        [node (clock/fnn-offset-s node)]))
   :chain-offset-s (await-chain-offset offset-s)})

(defn outage!
  "One outage of `node` for `duration-s` seconds; see the namespace doc. `offset`
  is the nemesis's atom of the clocks' offset."
  [test offset {:keys [node duration-s]}]
  (let [jump      (:outage-jump test 0)
        waited-ms (await-in-flight test node (:outage-await test 30))
        _         (on-node! test node db/kill!)
        down-at   (System/currentTimeMillis)
        caught    (in-flight test node)
        jumped    (when (and (pos? jump) (seq caught))
                    (let [to (swap! offset + jump)]
                      (assoc (jump! test node to) :offset-s to, :jump-s jump)))
        _         (when (and (pos? jump) (empty? caught))
                    (info "outage: no TLC in flight to" node "- the clocks stay"))
        left-ms   (- (* 1000 duration-s) (- (System/currentTimeMillis) down-at))]
    (when (pos? left-ms)
      (Thread/sleep left-ms))
    (on-node! test node db/start!)
    (let [restart (try
                    (fdb/await-rpc! node)
                    (when jumped
                      {:restart-offset-s (clock/fnn-offset-s node)})
                    (catch Exception e
                      (warn e "outage:" node "didn't come back")
                      {:restart-error (or (.getMessage e) (.getSimpleName (class e)))}))]
      (merge {:node       node
              :duration-s duration-s
              :down-ms    (- (System/currentTimeMillis) down-at)
              :waited-ms  waited-ms
              :caught     caught}
             jumped
             restart))))

(defrecord Outage [offset]
  n/Reflection
  (fs [_] #{:outage :outage-heal})

  n/Nemesis
  (setup! [this test]
    (when (pos? (:outage-jump test 0))
      (let [base (read-chain-offset)]
        (when-not base
          (throw (ex-info (str "--outage-jump: no offset file for the CKB node at " chain-offset-file
                               " (the chain-clock volume of docker/docker-compose.yml)")
                          {:type :no-chain-clock})))
        ; A fresh chain is at +0; one that a run already moved forward stays there,
        ; and the nodes start from the same time.
        (reset! offset base)
        (c/on-nodes test (fn [_ _] (clock/set-offset! base)))))
    this)

  (invoke! [_ test op]
    (case (:f op)
      :outage      (assoc op :value (outage! test offset (:value op)))
      ; An outage restarts its node itself; this covers one that broke off.
      :outage-heal (do (c/on-nodes test (fn [t node] (db/start! (:db t) t node)))
                       (assoc op :value :started))))

  ; The clocks stay where they are: the chain's can't go back.
  (teardown! [_ _test]))

(defn package
  [opts]
  {:generator       nil
   :final-generator (when ((:faults opts) :outage) {:type :info, :f :outage-heal, :value nil})
   :nemesis         (Outage. (atom 0))
   ; An outage is one op: lines at its start and its end.
   :perf            #{{:name  "outage"
                       :start #{}
                       :stop  #{}
                       :fs    #{:outage}
                       :color "#E9A0A0"}}})

(defn ops
  "Endless :outage ops, each a target and a duration drawn from `r`. Options:
  :outage-targets (specs to choose from: one, middle) and :outage-duration
  ([min max] seconds, drawn uniformly)."
  [^java.util.SplittableRandom r opts]
  (let [specs   (vec (:outage-targets opts [:middle]))
        nodes   (vec (:nodes opts))
        middles (process/middle-nodes opts)
        [lo hi] (:outage-duration opts [120 180])]
    (when (and (some #{:middle} specs) (empty? middles))
      (throw (ex-info (str "--outage-targets middle: no node in topology "
                           (name (:topology opts :line)) " is on more than one channel")
                      {:type :no-middle-node, :nodes nodes})))
    (when (and (some #{:clock} (:faults opts)) (pos? (:outage-jump opts 0)))
      (throw (ex-info (str "--outage-jump can't run with --faults clock: the clock fault resets"
                           " fnn's clock, and the chain's can't go back")
                      {:type :conflicting-faults})))
    (repeatedly
      (fn []
        {:type  :info
         :f     :outage
         :value {:node       (case (schedule/pick r specs)
                               :one    (schedule/pick r nodes)
                               :middle (schedule/pick r middles))
                 :duration-s (+ lo (.nextInt r (int (inc (- hi lo)))))}}))))

(defn check
  "Every :outage completion in `history`: how many caught a TLC in flight, and,
  after each jump, every clock's offset as measured against the one set. `jump`
  is --outage-jump."
  [jump history]
  (let [outages  (filter #(and (= :outage (:f %)) (map? (:value %)) (contains? (:value %) :caught))
                         history)
        jumps    (filter (comp :offset-s :value) outages)
        readings (for [op    jumps
                       :let  [v (:value op)]
                       [clk m] (concat (:fnn-offset-s v)
                                       [["chain" (:chain-offset-s v)]]
                                       (when (contains? v :restart-offset-s)
                                         [[(:node v) (:restart-offset-s v)]]))]
                   {:time (:time op), :clock clk, :set (:offset-s v), :measured m})
        measured (filter (comp number? :measured) readings)
        off      (vec (remove #(<= (abs (- (:measured %) (:set %))) clock/tolerance-s) measured))
        chain?   (some #(and (= "chain" (:clock %)) (number? (:measured %))) readings)]
    {:valid?     (cond (seq off)                                    false
                       (and (pos? jump) (or (empty? jumps) (not chain?))) :unknown
                       :else                                        true)
     :outages    (count outages)
     :caught     (count (filter (comp seq :caught :value) outages))
     :tlcs       (reduce + 0 (map (comp count :caught :value) outages))
     :jumps      (count jumps)
     :offset-s   (:offset-s (:value (last jumps)) 0)
     :unmeasured (vec (remove (comp number? :measured) readings))
     :not-taken  off
     :restarts   (vec (for [op outages :when (:restart-error (:value op))]
                        (select-keys (:value op) [:node :restart-error])))
     :each       (vec (for [op outages :let [v (:value op)]]
                        (merge (select-keys v [:node :duration-s :waited-ms :offset-s])
                               {:time (:time op), :tlcs (count (:caught v))})))}))

(defn checker
  [jump]
  (reify checker/Checker
    (check [_ _test history _opts]
      (check jump history))))

(def fault
  {:faults  #{:outage}
   :stream  :outage
   :package package
   :ops     ops
   :env     (fn [opts] (when (pos? (:outage-jump opts 0)) (clock/env opts)))
   :checker (fn [opts] {:outage (checker (:outage-jump opts 0))})})
