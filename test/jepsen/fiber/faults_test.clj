(ns jepsen.fiber.faults-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.nemesis :as n]
            [jepsen.fiber [faults :as faults]
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
  (is (= #{:kill :pause :partition :disconnect :packet :chain-partition :clock :cch-kill
           :lnd-disconnect}
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
