(ns jepsen.fiber.slow-io-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen.cli :as jc]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [db :as db]
                          [faults :as faults]
                          [schedule :as schedule]
                          [slowfs :as slowfs]]
            [jepsen.fiber.faults.slow-io :as slow-io]))

(def opts
  {:nodes ["n1" "n2" "n3"], :faults [:slow-io], :slow-io-targets [:one :middle :all]
   :slow-io-delay [20 200], :slow-io-ops [:write :fsync]})

(defn- ops
  [seed opts]
  (take 80 (slow-io/ops (schedule/stream (schedule/streams seed) :slow-io) opts)))

(deftest generated-ops
  (let [os     (ops 5 opts)
        starts (map :value (filter #(= :start-slow-io (:f %)) os))]
    (testing "a start, then a stop, in turn"
      (is (= (take 80 (cycle [:start-slow-io :stop-slow-io])) (map :f os)))
      (is (every? nil? (map :value (filter #(= :stop-slow-io (:f %)) os)))))
    (testing "one node or every node"
      (is (= #{1 3} (set (map (comp count :nodes) starts)))))
    (testing "a delay within the range, and the operations asked for"
      (is (every? #(<= 20 % 200) (map :delay-ms starts)))
      (is (< 1 (count (set (map :delay-ms starts)))))
      (is (= #{[:write :fsync]} (set (map :ops starts)))))
    (testing "the seed fixes them"
      (is (= os (ops 5 opts)))
      (is (not= os (ops 6 opts)))))
  (testing "middle: the node that forwards payments"
    (is (= #{["n2"]}
           (set (map (comp :nodes :value)
                     (filter #(= :start-slow-io (:f %)) (ops 1 (assoc opts :slow-io-targets [:middle])))))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (doall (ops 1 (assoc opts :nodes ["n1" "n2"] :slow-io-targets [:middle]))))))
  (testing "one fixed delay"
    (is (= #{75} (set (map (comp :delay-ms :value)
                           (filter #(= :start-slow-io (:f %)) (ops 1 (assoc opts :slow-io-delay [75 75])))))))))

(deftest config
  (testing "a slowdown delays the named operations on every file, always"
    (is (= (str "[errinj_slowdown]\n"
                "op_regexp = ^(fsync|write)$\n"
                "path_regexp = .*\n"
                "probability = 100\n"
                "duration = 150000000\n")
           (slowfs/config {:delay-ms 150, :ops [:write :fsync]})))
    (is (str/includes? (slowfs/config {:delay-ms 20, :ops [:fsync]}) "op_regexp = ^(fsync)$\n")))
  (testing "none: an empty file"
    (is (= "" (slowfs/config nil)))))

(deftest store
  (is (db/slow-store? {:faults #{:slow-io}}))
  (is (not (db/slow-store? {:faults #{:kill}})))
  (testing "the store is a directory in the mount, and the directory behind the mount isn't in it"
    (is (str/starts-with? slowfs/store (str slowfs/mount-dir "/")))
    (is (not (str/starts-with? slowfs/data-dir slowfs/mount-dir))))
  (testing "one filesystem of its own per test"
    (is (= ["slow-io"] (db/store-filesystems {:faults [:slow-io :kill]})))
    (is (= ["disk" "slow-io"] (db/store-filesystems {:faults [:slow-io :disk]})))
    (is (= ["power" "slow-io"] (db/store-filesystems {:faults [:power :slow-io]})))
    (is (= [] (db/store-filesystems {:faults [:kill]})))))

(defn- op
  [f value t]
  {:type :info, :process :nemesis, :f f, :value value, :time t})

(defn- pay
  [type from to t]
  {:type type, :process 0, :f :pay, :value {:from from, :to to}, :time t})

(def slow-ok
  {:status :ok, :rpc-ms 3, :store-write-ms 420})

(defn- stopped
  [delay-ms while-slow probe-after]
  {:delay-ms delay-ms, :while-slow while-slow, :probe-ms-after probe-after})

(deftest effect
  (testing "the probe took at least the delay"
    (is (nil? (slow-io/effect (op :start-slow-io {"n1" {:delay-ms 100, :probe-ms 204}} 1))))
    (is (nil? (slow-io/effect (op :start-slow-io {"n1" {:delay-ms 100, :probe-ms 100}
                                                  "n2" {:delay-ms 100, :probe-ms 230}} 1)))))
  (testing "a store that wasn't slowed"
    (is (= "a write and fsync on n2's store took 3 ms under a delay of 100 ms"
           (slow-io/effect (op :start-slow-io {"n1" {:delay-ms 100, :probe-ms 204}
                                               "n2" {:delay-ms 100, :probe-ms 3}} 1))))
    (is (string? (slow-io/effect (op :start-slow-io {} 1)))))
  (testing "a stop isn't judged"
    (is (nil? (slow-io/effect (op :stop-slow-io {"n1" (stopped 100 slow-ok 2)} 1)))))
  (testing "the :faults check counts the starts, and one without effect fails the run"
    (let [good [(op :start-slow-io {:nodes ["n1"], :delay-ms 100, :ops [:write :fsync]} 1)
                (op :start-slow-io {"n1" {:delay-ms 100, :probe-ms 204}} 2)
                (op :stop-slow-io nil 3)
                (op :stop-slow-io {"n1" (stopped 100 slow-ok 2)} 4)]
          bad  (assoc-in good [1 :value "n1" :probe-ms] 2)]
      (is (= {:valid? true, :faults {:slow-io {:ops 1, :took-effect 1}}}
             (select-keys (faults/fired opts good) [:valid? :faults])))
      (is (false? (:valid? (faults/fired opts bad)))))))

(deftest check
  (let [h [(op :start-slow-io {:nodes ["n2"], :delay-ms 100, :ops [:write :fsync]} 10)
           (op :start-slow-io {"n2" {:delay-ms 100, :probe-ms 204}} 11)
           (pay :ok "n1" "n2" 12)
           (pay :info "n2" "n3" 13)
           (pay :ok "n1" "n3" 14)
           (op :stop-slow-io nil 20)
           (op :stop-slow-io {"n2" (stopped 100 slow-ok 2)} 21)
           (pay :ok "n1" "n2" 22)
           (op :start-slow-io {:nodes ["n1" "n2" "n3"], :delay-ms 40, :ops [:write :fsync]} 30)
           (op :start-slow-io {"n1" {:delay-ms 40, :probe-ms 85}
                               "n2" {:delay-ms 40, :probe-ms 90}
                               "n3" {:delay-ms 40, :probe-ms 81}} 31)
           (pay :fail "n3" "n1" 32)
           (op :stop-slow-io nil 40)
           (op :stop-slow-io {"n1" (stopped 40 {:status :ok, :rpc-ms 2, :store-write-ms 90} 1)
                              "n2" (stopped 40 {:status :not-working, :rpc-ms 2, :store-write-ms 5001
                                                :store-write-error "SocketTimeoutException"} 3)
                              "n3" (stopped 40 {:status :exited} 2)} 41)
           ; The final stop, with nothing slowed.
           (op :stop-slow-io nil 50)
           (op :stop-slow-io {} 51)]]
    (testing "every start slowed its stores, and every stop made them fast again"
      (let [r (slow-io/check h)]
        (is (true? (:valid? r)))
        (is (= 4 (:windows-count r)))
        (is (= {"n1" 1, "n2" 2, "n3" 1} (:slowed r)))
        (is (= {:min 40, :median 40, :max 100} (:delay-ms r)))
        (is (= {:ok 2, :not-working 1, :exited 1} (:while-slow r)))
        (is (= {:min 90, :median 420, :max 5001} (:store-write-ms r)))
        (testing "a window: its delay, both probes, fnn while slow, and the node's payments in it"
          (is (= {:node "n2", :time 21, :delay-ms 100, :probe-ms 204, :probe-ms-after 2
                  :status :ok, :rpc-ms 3, :store-write-ms 420
                  :payments {:ok 1, :info 1}}
                 (first (:windows r))))
          (is (= {:fail 1} (:payments (first (filter #(= "n3" (:node %)) (:windows r)))))))))
    (testing "a start that didn't slow a store"
      (let [r (slow-io/check (assoc-in h [1 :value "n2" :probe-ms] 4))]
        (is (false? (:valid? r)))
        (is (= [{:node "n2", :time 11, :delay-ms 100, :probe-ms 4}] (:not-slowed r)))))
    (testing "a store still slow after its stop"
      (let [r (slow-io/check (assoc-in h [6 :value "n2"] (assoc (stopped 100 slow-ok 230) :still-slow true)))]
        (is (false? (:valid? r)))
        (is (= ["n2"] (map :node (:still-slow r))))))
    (testing "a mount that was gone"
      (let [r (slow-io/check (assoc-in h [6 :value "n2"] {:delay-ms 100, :mount-lost true}))]
        (is (false? (:valid? r)))
        (is (= ["n2"] (map :node (:mount-lost r))))))
    (testing "an op that threw"
      (let [r (slow-io/check (conj h (assoc (op :start-slow-io {:nodes ["n1"], :delay-ms 50} 60)
                                            :exception {:via []} :error "boom")))]
        (is (false? (:valid? r)))
        (is (= [{:f :start-slow-io, :time 60, :error "boom"}] (:nemesis-errors r)))))
    (testing "nothing was slowed"
      (is (= :unknown (:valid? (slow-io/check [(op :stop-slow-io nil 1) (op :stop-slow-io {} 2)])))))))

(defn- parse
  [args]
  (cli/parse-opts args (jc/merge-opt-specs jc/test-opt-spec fiber/cli-opts)))

(deftest options
  (testing "defaults: one node, 20 to 200 ms, writes and fsyncs"
    (let [{:keys [options errors]} (parse ["--faults" "slow-io"])]
      (is (empty? errors))
      (is (= [:slow-io] (:faults options)))
      (is (= [:one] (:slow-io-targets options)))
      (is (= [20 200] (:slow-io-delay options)))
      (is (= [:write :fsync] (:slow-io-ops options)))))
  (testing "a fixed delay, fsync alone, the middle node"
    (let [{:keys [options errors]} (parse ["--faults" "slow-io" "--slow-io-delay" "500"
                                           "--slow-io-ops" "fsync" "--slow-io-targets" "middle,all"])]
      (is (empty? errors))
      (is (= [500 500] (:slow-io-delay options)))
      (is (= [:fsync] (:slow-io-ops options)))
      (is (= [:middle :all] (:slow-io-targets options)))))
  (testing "what it refuses"
    (is (seq (:errors (parse ["--slow-io-delay" "0"]))))
    (is (seq (:errors (parse ["--slow-io-delay" "200-20"]))))
    (is (seq (:errors (parse ["--slow-io-delay" "20000"]))))
    (is (seq (:errors (parse ["--slow-io-ops" "read"]))))
    (is (seq (:errors (parse ["--slow-io-targets" "primaries"]))))))
