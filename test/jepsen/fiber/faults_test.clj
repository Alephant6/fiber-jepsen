(ns jepsen.fiber.faults-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.nemesis :as n]
            [jepsen.fiber [db :as db]
                          [faults :as faults]
                          [schedule :as schedule]
                          [topology :as topology]]
            [jepsen.fiber.faults [chain-partition :as chain-partition]
                                 [packet :as packet]
                                 [partition :as partition]]))

(def nodes ["n1" "n2" "n3"])

(def opts
  {:nodes nodes, :nemesis-interval 2, :kill-targets [:one :all]
   :packet-targets [:one :all :chain], :packet-delay [500 3000], :packet-loss 10})

(defn fault-ops
  "The nemesis ops of the fault generator for `faults`, under Jepsen's simulator."
  [seed faults]
  (->> (gt/simulate (gen/context {:concurrency 1})
                    (gen/nemesis (gen/limit 200 (faults/generator (schedule/streams seed)
                                                                  (assoc opts :faults faults))))
                    (fn [_ctx op] (-> op (assoc :type :ok) (update :time + 1000000))))
       (filter #(= :info (:type %)))
       (map (juxt :f :value))))

(deftest registry
  (testing "every fault declares what the registry needs"
    (doseq [f faults/registry]
      (is (seq (:faults f)))
      (is (keyword? (:stream f)))
      (is (fn? (:package f)))
      (is (fn? (:ops f)))))
  (testing "no two faults share a --faults name or a stream"
    (is (apply distinct? (mapcat :faults faults/registry)))
    (is (apply distinct? (map :stream faults/registry))))
  (is (= #{:kill :pause :partition :disconnect :packet :chain-partition :clock :outage :cpu :memory
           :disk :power :slow-io :cch-kill :lnd-disconnect :old-state}
         faults/supported))
  (testing "only the faults asked for"
    (is (= [:partition :db] (map :stream (faults/in-use {:faults [:kill :partition]}))))
    (is (empty? (faults/in-use {:faults []})))))

(deftest streams
  (let [s (schedule/streams 7)]
    (testing "the first streams are split off the seeded root, as before"
      (is (identical? (:pay s) (schedule/stream s :pay))))
    (testing "a new stream depends on the seed and its name"
      (let [draw #(.nextLong (schedule/stream %1 %2))]
        (is (= (draw (schedule/streams 7) :packet) (draw (schedule/streams 7) :packet)))
        (is (not= (draw (schedule/streams 7) :packet) (draw (schedule/streams 8) :packet)))
        (is (not= (draw (schedule/streams 7) :packet) (draw (schedule/streams 7) :clock)))))))

(deftest adding-a-fault-changes-no-other
  (let [only (fn [fs ops] (filter (comp fs first) ops))
        kill #{:kill :start}]
    (doseq [seed [1 2 3]]
      (let [alone (only kill (fault-ops seed [:kill]))
            mixed (only kill (fault-ops seed [:kill :packet]))
            n     (min (count alone) (count mixed))]
        (is (< 10 n))
        (is (= (take n alone) (take n mixed)))))))

(deftest packet
  (let [ops    (take 400 (packet/ops (schedule/stream (schedule/streams 5) :packet) opts))
        starts (map :value (filter (comp #{:start-packet} :f) ops))]
    (testing "each fault is followed by its healing"
      (is (= (take 400 (cycle [:start-packet :stop-packet])) (map :f ops))))
    (testing "targets come from the specs asked for"
      (is (= #{[:one] [:all] [:chain]}
             (set (for [[targets _] starts]
                    (cond (= nodes targets)            [:all]
                          (= ["chain"] targets)        [:chain]
                          (= 1 (count targets))        [:one]))))))
    (testing "delays are drawn from the range, loss is as asked"
      (doseq [[_ b] starts]
        (is (<= 500 (parse-long (re-find #"\d+" (get-in b [:delay :time]))) 3000))
        (is (= "10%" (get-in b [:loss :percent])))))
    (testing "no loss unless asked"
      (is (nil? (:loss (packet/behaviors 800 0)))))
    (is (= [500 3000] (packet/parse-delay "500-3000")))
    (is (= [800 800] (packet/parse-delay "800")))))

(def six
  ["n1" "n2" "n3" "n4" "n5" "n6"])

(defn connected?
  "Whether `edges` connect all of `nodes`."
  [nodes edges]
  (let [nb (topology/neighbours edges)]
    (loop [seen #{(first nodes)}, todo [(first nodes)]]
      (if-let [[x & more] (seq todo)]
        (let [new (remove seen (nb x))]
          (recur (into seen new) (into (vec more) new)))
        (= (set nodes) seen)))))

(deftest topologies
  (is (= [["n1" "n2"] ["n2" "n3"]] (topology/edges {:nodes nodes})))
  (is (= [["n1" "n2"] ["n2" "n3"]] (topology/edges {:nodes nodes, :topology :line})))
  (is (= [["n1" "n2"] ["n2" "n3"] ["n3" "n1"]] (topology/edges {:nodes nodes, :topology :ring})))
  (testing "three nodes: the mesh is the ring"
    (is (= (topology/ring nodes) (topology/mesh nodes))))
  (testing "six nodes: the mesh is the ring and its three diameters, three channels a node"
    (is (= [["n1" "n2"] ["n2" "n3"] ["n3" "n4"] ["n4" "n5"] ["n5" "n6"] ["n6" "n1"]
            ["n1" "n4"] ["n2" "n5"] ["n3" "n6"]]
           (topology/edges {:nodes six, :topology :mesh})))
    (is (= {"n1" #{"n2" "n6" "n4"}} (select-keys (topology/neighbours (topology/mesh six)) ["n1"])))
    (is (every? #(= 3 (count %)) (vals (topology/neighbours (topology/mesh six))))))
  (testing "every graph spans its nodes, with one channel per pair and none to itself"
    (doseq [k (range 2 7), t (keys topology/topologies)]
      (let [ns    (subvec six 0 k)
            edges (topology/edges {:nodes ns, :topology t})
            pairs (map set edges)]
        (testing (str t " over " k)
          (is (connected? ns edges))
          (is (every? #(= 2 (count %)) pairs))
          (is (apply distinct? pairs))
          (when (and (< 2 k) (not= :line t))
            (is (every? #(<= 2 (count %)) (vals (topology/neighbours edges)))))))))
  (testing "node addresses"
    (is (= ["10.77.0.11" "10.77.0.13" "10.77.0.16" "10.77.0.22"]
           (map topology/node-ip ["n1" "n3" "n6" "n12"])))))

(defn partition-ops-before-target-specs
  "partition/ops as it was before target specs, at 84599bc."
  [r nodes]
  (mapcat (fn [node]
            [{:type  :info
              :f     :start-partition
              :value (n/complete-grudge [[node] (vec (remove #{node} nodes))])}
             {:type :info, :f :stop-partition, :value nil}])
          (repeatedly #(schedule/pick r nodes))))

(defn components
  "The sides of a complete grudge between two groups."
  [grudge]
  (set (for [[node enemies] grudge]
         (set (remove enemies (keys grudge))))))

(deftest partition-targets
  (let [r    #(schedule/stream (schedule/streams %) :partition)
        ops  (fn [seed opts]
               (take 200 ((:ops partition/fault) (r seed) (merge {:nodes six} opts))))
        cuts (fn [ops] (map (comp components :value) (filter (comp #{:start-partition} :f) ops)))]
    (testing "one, the default, gives the ops it gave before"
      (doseq [seed [1 2 3], ns [nodes six]]
        (let [before (take 200 (partition-ops-before-target-specs (r seed) ns))]
          (is (= before (take 200 (partition/ops (r seed) ns))))
          (is (= before (take 200 ((:ops partition/fault) (r seed) {:nodes ns}))))
          (is (= before (take 200 ((:ops partition/fault) (r seed)
                                                          {:nodes ns, :partition-targets [:one]})))))))
    (testing "half cuts a random half from the rest"
      (let [cs (cuts (ops 4 {:partition-targets [:half]}))]
        (is (every? #(= #{3} (set (map count %))) cs))
        (is (every? #(= (set six) (apply into %)) cs))
        (is (< 5 (count (set cs))))
        (is (= cs (cuts (ops 4 {:partition-targets [:half]}))))
        (is (not= cs (cuts (ops 5 {:partition-targets [:half]}))))))
    (testing "an odd count isolates the smaller half"
      (is (every? #(= #{2 3} (set (map count %)))
                  (cuts (take 40 ((:ops partition/fault) (r 4) {:nodes (subvec six 0 5)
                                                                :partition-targets [:half]}))))))
    (testing "one,half draws both"
      (is (= #{#{1 5} #{3}}
             (set (map #(set (map count %)) (cuts (ops 6 {:partition-targets [:one :half]})))))))))

(deftest partition-durations
  (let [usual #(take 20 (schedule/gaps (schedule/gaps-stream (schedule/streams 9) :partition) 15))]
    (testing "without --partition-duration, the usual gaps"
      (is (= (usual) (take 20 (partition/gaps (schedule/streams 9) {:nemesis-interval 15})))))
    (testing "with it, each partition lasts a drawn number of seconds; the healings keep their gaps"
      (let [g (take 20 (partition/gaps (schedule/streams 9) {:nemesis-interval 15
                                                            :partition-duration [120 300]}))]
        (is (every? #(and (<= 120e9 % 300e9) (zero? (mod % 1000000000))) (take-nth 2 g)))
        (is (< 3 (count (set (take-nth 2 g)))))
        (is (= (take 10 (usual)) (take-nth 2 (rest g))))))
    (testing "under the simulator, a partition lasts what was drawn"
      (let [hist  (->> (gt/simulate (gen/context {:concurrency 1})
                                    (gen/nemesis (gen/limit 20 (faults/generator
                                                                 (schedule/streams 9)
                                                                 {:nodes six, :faults [:partition]
                                                                  :nemesis-interval 15
                                                                  :partition-duration [120 300]})))
                                    (fn [_ctx op] (-> op (assoc :type :ok) (update :time + 1000000))))
                       (filter #(= :info (:type %))))
            spans (->> hist (partition 2) (map (fn [[a b]] [(:f a) (:f b) (- (:time b) (:time a))])))]
        (is (= 10 (count spans)))
        (is (every? (fn [[a b dt]] (and (= :start-partition a) (= :stop-partition b)
                                        (<= 120e9 dt 300e9)))
                    spans))))
    (is (= [120 300] (partition/parse-duration "120-300")))
    (is (= [180 180] (partition/parse-duration "180")))))

(deftest chain-partition
  (let [ops    (take 400 (chain-partition/ops (schedule/stream (schedule/streams 5) :chain-partition)
                                              (assoc opts :chain-partition-mode :reject)))
        starts (map :value (filter (comp #{:start-chain-partition} :f) ops))]
    (testing "each cut is followed by its healing"
      (is (= (take 400 (cycle [:start-chain-partition :stop-chain-partition])) (map :f ops))))
    (testing "one random node or all of them, in the mode asked for"
      (is (= #{1 3} (set (map (comp count :nodes) starts))))
      (is (= #{:reject} (set (map :mode starts))))))
  (testing "drop cuts both directions, reject resets connections to the CKB node"
    (is (= [["OUTPUT" "-d" "10.77.0.10" "-m" "comment" "--comment" "fiber-chain-partition" "-j" "DROP"]
            ["INPUT" "-s" "10.77.0.10" "-m" "comment" "--comment" "fiber-chain-partition" "-j" "DROP"]]
           (chain-partition/rules "10.77.0.10" :drop)))
    (is (= [["OUTPUT" "-d" "10.77.0.10" "-p" "tcp" "-m" "comment" "--comment" "fiber-chain-partition"
             "-j" "REJECT" "--reject-with" "tcp-reset"]]
           (chain-partition/rules "10.77.0.10" :reject))))
  (testing "adding it changes no other fault's operations"
    (let [kill  #{:kill :start}
          only  (fn [ops] (filter (comp kill first) ops))
          alone (only (fault-ops 4 [:kill]))
          mixed (only (fault-ops 4 [:kill :chain-partition]))
          n     (min (count alone) (count mixed))]
      (is (< 10 n))
      (is (= (take n alone) (take n mixed))))))

(defn nemesis-history
  "A history of these nemesis op completions, at times 0, 1, 2…: each after its
  invocation, which has no result yet, and a client op between them."
  [& completions]
  (vec (mapcat (fn [t op]
                 [{:type :invoke, :process 0, :f :pay, :time t}
                  {:type :info, :process :nemesis, :f (:f op), :value :invoked, :time t}
                  (merge {:type :info, :process :nemesis, :time t} op)])
               (range) completions)))

(deftest fired
  (let [check (fn [fs & ops] (faults/fired {:faults fs} (apply nemesis-history ops)))]
    (testing "each fault asked for took effect; recoveries and the health sample aren't counted"
      (let [r (check [:kill :pause]
                     {:f :kill, :value {"n1" nil}}
                     {:f :start, :value {"n1" "started"}}
                     {:f :pause, :value {"n2" "Tl"}}
                     {:f :health, :value {"n1" {}}}
                     {:f :resume, :value {"n2" "Sl"}})]
        (is (true? (:valid? r)))
        (is (= {:kill {:ops 1, :took-effect 1}, :pause {:ops 1, :took-effect 1}} (:faults r)))
        (is (zero? (:crashed-count r)))))
    (testing "a pause that stopped nothing breaks the run: it is a local signal"
      (let [r (check [:pause]
                     {:f :pause, :value {"n2" "T"}}
                     {:f :resume}
                     {:f :pause, :value {"n1" "Ssl"}})]
        (is (false? (:valid? r)))
        (is (= {:ops 2, :took-effect 1, :no-effect-count 1
                :no-effect [{:f :pause, :time 2, :why "fnn not stopped: n1 Ssl"}]}
               (get-in r [:faults :pause])))))
    (testing "a pause where fnn was down is skipped"
      (let [r (check [:pause]
                     {:f :pause, :value {"n2" "T"}}
                     {:f :resume}
                     {:f :pause, :value {"n1" :not-running}}
                     {:f :resume}
                     {:f :pause, :value {"n3" "Z"}})]
        (is (true? (:valid? r)))
        (is (= {:ops 3, :took-effect 1, :skipped 2} (get-in r [:faults :pause]))))
      (testing "and when every pause was, the run didn't test pauses"
        (is (= :unknown (:valid? (check [:pause] {:f :pause, :value {"n1" nil}}))))))
    (testing "a kill target typo: every kill threw, and Jepsen only logged it"
      (let [crash {:f :kill, :value :midle, :exception {:via []}
                   :error "indeterminate: Don't know how to create ISeq from: clojure.lang.Keyword"}
            r     (check [:kill] crash {:f :start} crash)]
        (is (false? (:valid? r)))
        (is (= {:ops 2, :took-effect 0, :no-effect-count 2
                :no-effect [{:f :kill, :time 0, :why "crashed"} {:f :kill, :time 2, :why "crashed"}]}
               (get-in r [:faults :kill])))
        (is (= 2 (:crashed-count r)))
        (is (= {:f :kill, :time 0, :error (:error crash)} (first (:crashed r))))))
    (testing "a crashed recovery fails the run too"
      (is (false? (:valid? (check [:kill] {:f :kill, :value {"n1" nil}}
                                  {:f :start, :exception {}, :error "indeterminate: x"})))))
    (testing "a fault that never took effect, though nothing threw"
      (let [r (check [:disconnect]
                     {:f :disconnect, :value {:from "n1", :to "n2", :result "failed: connection refused"}}
                     {:f :reconnect, :value {}})]
        (is (false? (:valid? r)))
        (is (= "disconnect_peer failed: connection refused"
               (get-in r [:faults :disconnect :no-effect 0 :why])))))
    (testing "one that failed now and then is fine, unless it is strict"
      (let [r (check [:disconnect]
                     {:f :disconnect, :value {:result "failed: peer down"}}
                     {:f :reconnect, :value {}}
                     {:f :flash-disconnect, :value {:disconnect {}, :reconnect {}}})]
        (is (true? (:valid? r)))
        (is (= {:ops 2, :took-effect 1, :no-effect-count 1}
               (dissoc (get-in r [:faults :disconnect]) :no-effect)))))
    (testing "a fault asked for and never tried: the run was shorter than its first gap"
      (let [r (check [:kill :partition] {:f :kill, :value {"n1" nil}})]
        (is (= :unknown (:valid? r)))
        (is (= {:ops 0, :took-effect 0} (get-in r [:faults :partition])))))
    (testing "cpu and memory share an op, told apart by the limit"
      (let [r (check [:cpu :memory]
                     {:f :limit-resources, :value {:limit {:nodes ["n1"], :cpus 0.25}, :nodes {"n1" {}}}}
                     {:f :lift-resources, :value {}}
                     {:f :limit-resources, :value {:limit {:nodes ["n2"], :memory-mib 192}, :nodes {"n2" {}}}}
                     {:f :lift-resources, :value {}}
                     {:f :limit-resources, :value {:nodes ["n3"], :cpus 0.5}, :exception {}, :error "indeterminate: x"})]
        (is (false? (:valid? r)))
        (is (= {:ops 2, :took-effect 1} (select-keys (get-in r [:faults :cpu]) [:ops :took-effect])))
        (is (= {:ops 1, :took-effect 1} (get-in r [:faults :memory])))))
    (testing "only the faults asked for are reported"
      (is (= [:kill] (keys (:faults (check [:kill] {:f :kill, :value {"n1" nil}}))))))))

(deftest every-fault-says-which-ops-inject-it
  (doseq [f faults/registry, nm (:faults f)]
    (testing nm
      (is (contains? (:fired f) nm)))))

(deftest final-order
  (let [opts {:nodes nodes, :faults [:kill :pause :partition :disconnect :packet :chain-partition
                                     :clock :cpu :memory], :kill-targets [:one]}
        final (:final-generator (faults/package (db/db) opts))]
    (testing "every other fault heals before fnn restarts, and peers reconnect last"
      (is (= [:stop-partition :stop-packet :stop-chain-partition :clock-reset :lift-resources
              :resume :start :reconnect]
             (map :f (flatten final)))))
    (testing "the faults keep their registry order otherwise"
      (is (= [:partition :clock :db :disconnect]
             (map :stream (faults/final-order (faults/in-use {:faults [:clock :disconnect :partition :kill]})))))))
  (testing "a single fault's package is untouched"
    (is (= [{:type :info, :f :stop-partition, :value nil}]
           [(:final-generator (faults/package nil {:nodes nodes, :faults [:partition]}))]))))

(deftest partition-heal-leaves-other-rules
  (let [s (str "-P INPUT ACCEPT\n"
               "-A INPUT -s 10.77.0.12/32 -j DROP\n"
               "-A INPUT -s 10.77.0.10/32 -m comment --comment fiber-chain-partition -j DROP\n"
               "-A INPUT -s 10.77.0.13/32 -j DROP\n"
               "-A INPUT -s 10.77.0.12/32 -j DROP\n")]
    (is (= ["10.77.0.12/32" "10.77.0.13/32" "10.77.0.12/32"] (partition/partition-sources s)))
    (is (empty? (partition/partition-sources nil)))))
