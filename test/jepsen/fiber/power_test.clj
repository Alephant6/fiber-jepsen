(ns jepsen.fiber.power-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen.cli :as jc]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [db :as db]
                          [faults :as faults]
                          [schedule :as schedule]]
            [jepsen.fiber.faults.power :as power]))

(def opts
  {:nodes ["n1" "n2" "n3"], :faults [:power], :power-targets [:one :all]})

(deftest ops
  (let [ops (take 60 (power/ops (schedule/stream (schedule/streams 5) :power) opts))]
    (testing "a power-off, then a power-on, in turn"
      (is (= (take 60 (cycle [:power-off :power-on])) (map :f ops))))
    (testing "one node or every node"
      (is (= #{1 3} (set (map #(count (get-in % [:value :nodes])) (filter #(= :power-off (:f %)) ops))))))
    (testing "the seed fixes them"
      (is (= ops (take 60 (power/ops (schedule/stream (schedule/streams 5) :power) opts)))))))

(deftest gaps
  (let [usual (take 40 (schedule/gaps (schedule/gaps-stream (schedule/streams 5) :power) 20))
        gaps  #(take 40 ((:gaps power/fault) (schedule/streams 5)
                                             (merge opts {:nemesis-interval 20} %)))]
    (testing "every fault's gaps"
      (is (= usual (gaps {})))
      (is (= usual (gaps {:nemesis-min-gap 0}))))
    (testing "with --nemesis-min-gap, a power loss lasts the gap drawn for it and the power is back at least that long"
      (is (some #(< % 30e9) (take-nth 2 (rest usual))))
      (is (= (take-nth 2 usual) (take-nth 2 (gaps {:nemesis-min-gap 30}))))
      (is (= (map #(max (long 30e9) %) (take-nth 2 (rest usual)))
             (take-nth 2 (rest (gaps {:nemesis-min-gap 30}))))))))

(deftest store
  (is (db/lazyfs-store? {:faults #{:power}}))
  (is (not (db/lazyfs-store? {:faults #{:kill}})))
  (let [lz (db/store-lazyfs "n2")]
    (is (= (db/store-dir "n2") (:dir lz)))
    (is (= "/fiber/lazyfs/fifo" (:fifo lz)))
    (is (= "/fiber/lazyfs/data" (:data-dir lz)))))

(def esc "\u001b")

(deftest sync-modes
  (is (= [false true]
         (power/sync-modes [(str "  2026-11-05T05:00:00Z " esc "[32m INFO" esc "[0m fnn: Store sync writes: false")
                            "  2026-11-05T05:00:01Z  INFO fnn: Starting node with git version 0.6.1"
                            "  2026-11-05T05:01:00Z  INFO fnn: Store sync writes: true"])))
  (is (= [] (power/sync-modes ["  INFO fnn: Starting node with git version 0.6.1 (ae6f7d3)"]))))

(defn- op
  [f value t]
  {:type :info, :process :nemesis, :f f, :value value, :time t})

(deftest check
  (let [h [(op :power-off {:nodes ["n1"]} 1)
           (op :power-off {"n1" {:store-bytes-before 5000, :store-bytes-after 3800}} 2)
           (op :power-on nil 3)
           (op :power-on {"n1" :started} 4)
           (op :power-off {"n1" {:store-bytes-before 6000, :store-bytes-after 6000}
                           "n2" {:store-bytes-before 6100, :store-bytes-after 6000}} 5)
           (op :power-on {"n1" :started, "n2" :started} 6)]
        modes {"n1" [false false false], "n2" [false false], "n3" [false]}]
    (testing "every power-on brought fnn back"
      (let [r (power/check h modes)]
        (is (true? (:valid? r)))
        (is (= {"n1" 2, "n2" 1} (:power-offs r)))
        (is (= {"n1" [1200 0], "n2" [100]} (:bytes-dropped r)))
        (is (= {"n1" false, "n2" false, "n3" false} (:store-sync-writes r)))))
    (testing "a node that didn't come back"
      (let [r (power/check (conj h (op :power-off {"n3" {}} 7) (op :power-on {"n3" "RPC didn't come back: x"} 8))
                           modes)]
        (is (false? (:valid? r)))
        (is (= ["n3"] (map :node (:not-back r))))))
    (testing "an op that threw"
      (is (false? (:valid? (power/check (conj h (assoc (op :power-off nil 7) :exception {}, :error "x")) modes)))))
    (testing "the sync mode each node logged, or why there is none"
      (is (= {"n1" true, "n2" :not-logged, "n3" :no-log, "n4" [false true]}
             (:store-sync-writes (power/check h {"n1" [true], "n2" [], "n3" nil, "n4" [false true]})))))
    (testing "no power loss at all"
      (is (= :unknown (:valid? (power/check [] modes)))))
    (testing "--store-sync-writes asked for a mode fnn didn't log: the run didn't test it"
      (is (true? (:valid? (power/check h modes false))))
      (let [r (power/check h modes true)]
        (is (= :unknown (:valid? r)))
        (is (= {"n1" false, "n2" false, "n3" false} (:other-mode r))))
      (is (= {"n2" :not-logged}
             (:other-mode (power/check h {"n1" [true], "n2" [], "n3" [true true]} true))))
      (is (false? (:valid? (power/check (conj h (assoc (op :power-off nil 7) :exception {}, :error "x"))
                                        modes true)))))))

(deftest in-the-test
  (is (contains? faults/supported :power))
  (is (= [:power] (keys (faults/checkers opts)))))

(deftest options
  (let [parse #(cli/parse-opts % (jc/merge-opt-specs jc/test-opt-spec fiber/cli-opts))]
    (is (true? (:store-sync-writes (:options (parse ["--store-sync-writes" "true"])))))
    (is (false? (:store-sync-writes (:options (parse ["--store-sync-writes" "false"])))))
    (testing "unset leaves fnn's default"
      (is (not (contains? (:options (parse [])) :store-sync-writes))))
    (is (seq (:errors (parse ["--store-sync-writes" "yes"]))))
    (is (= [:all] (:power-targets (:options (parse ["--power-targets" "all"])))))
    (is (seq (:errors (parse ["--power-targets" "some"]))))))
