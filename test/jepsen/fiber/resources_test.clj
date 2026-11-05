(ns jepsen.fiber.resources-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [faults :as faults]
                          [schedule :as schedule]]
            [jepsen.fiber.faults.resources :as resources])
  (:import (java.time Instant)))

(def nodes ["n1" "n2" "n3"])

(def opts
  {:nodes nodes, :faults [:cpu :memory], :resource-targets [:one :all]
   :cpu-limit [0.1 0.5], :memory-limit [128 256]})

(defn draw
  [seed opts n]
  (take n (resources/ops (schedule/stream (schedule/streams seed) :resources) opts)))

(deftest ops
  (let [ops    (draw 3 opts 400)
        limits (map :value (filter (comp #{:limit-resources} :f) ops))]
    (testing "each limit is followed by its lifting"
      (is (= (take 400 (cycle [:limit-resources :lift-resources])) (map :f ops))))
    (testing "the same seed draws the same limits"
      (is (= ops (draw 3 opts 400)))
      (is (not= ops (draw 4 opts 400))))
    (testing "both kinds, both target specs, amounts within their ranges"
      (is (= #{:cpu :memory} (set (map #(if (:cpus %) :cpu :memory) limits))))
      (is (= #{1 3} (set (map (comp count :nodes) limits))))
      (doseq [l limits]
        (if-let [c (:cpus l)]
          (is (and (<= 0.1 c 0.5) (= c (/ (Math/round (* 100 c)) 100.0))))
          (is (and (integer? (:memory-mib l)) (<= 128 (:memory-mib l) 256))))))
    (testing "only the kinds asked for"
      (is (every? :cpus (map :value (filter (comp #{:limit-resources} :f)
                                            (draw 3 (assoc opts :faults [:cpu]) 100)))))
      (is (every? :memory-mib (map :value (filter (comp #{:limit-resources} :f)
                                                  (draw 3 (assoc opts :faults [:memory :kill]) 100))))))
    (testing "a fixed amount"
      (is (every? #(= 0.25 (:cpus %))
                  (map :value (filter (comp #{:limit-resources} :f)
                                      (draw 3 (assoc opts :faults [:cpu] :cpu-limit [0.25 0.25]) 50))))))))

(deftest registered
  (is (= :resources (:stream (first (faults/in-use {:faults [:memory]})))))
  (is (contains? (faults/checkers {:faults [:cpu]}) :disconnect-loops))
  (is (empty? (faults/checkers {:faults [:kill]}))))

(deftest cli
  (let [parse (fn [& args] (cli/parse-opts args fiber/cli-opts))]
    (is (= [0.1 0.5] (:cpu-limit (:options (parse)))))
    (is (= [64.0 128.0] (:memory-limit (:options (parse "--memory-limit" "64-128")))))
    (is (= [0.25 0.25] (:cpu-limit (:options (parse "--cpu-limit" "0.25")))))
    (is (seq (:errors (parse "--cpu-limit" "0.5-0.1"))))
    (is (seq (:errors (parse "--memory-limit" "8"))))
    (is (seq (:errors (parse "--resource-targets" "one,most"))))
    (is (= [:cpu :memory] (:faults (:options (parse "--faults" "cpu,memory")))))))

(deftest limit-body
  (let [host {:cpus 4, :memory 16000000000}]
    (is (= {:NanoCpus 250000000} (resources/limit-body :cpu 0.25 host)))
    (is (= {:NanoCpus 4000000000} (resources/limit-body :cpu :lift host)))
    (is (= {:Memory 134217728, :MemorySwap 134217728} (resources/limit-body :memory 128 host)))
    (testing (str "lifting can't unset the limit, so it gives the host's memory and as much swap:"
                  " a MemorySwap of -1 doesn't reach runc, which then refuses memory above the old swap")
      (is (= {:Memory 16000000000, :MemorySwap 32000000000} (resources/limit-body :memory :lift host))))))

(deftest window-report
  (is (= {:throttled-ms 1500, :throttled-slices 40, :memory-max-hits 0, :ooms 0, :oom-kills 0
          :memory-mib 100, :anon-mib 60, :file-mib 38}
         (resources/window-report
           {:throttled-usec 500000, :nr-throttled 10, :memory-max-hits 3, :oom 1, :oom-kills 1}
           {:throttled-usec 2000000, :nr-throttled 50, :memory-max-hits 3, :oom 1, :oom-kills 1
            :memory.current (* 100 1024 1024), :anon (* 60 1024 1024), :file (* 38 1024 1024)})))
  (testing "counters a cgroup doesn't have are left out"
    (is (= {} (resources/window-report {} {})))))

(def log
  ["  2026-11-05T04:00:00.000001Z DEBUG fnn::fiber::network: Peer Pubkey(02aa) connected"
   "  2026-11-05T04:00:10Z DEBUG fnn::fiber::network: Peer Pubkey(02aa) disconnected on session SessionId(1)"
   "  2026-11-05T04:00:20Z DEBUG fnn::fiber::network: Peer Pubkey(02aa) disconnected on session SessionId(2)"
   "  2026-11-05T04:00:30Z  INFO fnn::fiber::channel: something else"
   "  2026-11-05T04:00:40Z DEBUG fnn::fiber::network: Peer Pubkey(02aa) disconnected on session SessionId(3)"
   "  2026-11-05T04:05:00Z DEBUG fnn::fiber::network: Peer Pubkey(03bb) disconnected on session SessionId(4)"])

(deftest disconnects
  (is (= [{:at (Instant/parse "2026-11-05T04:00:10Z"), :peer "Pubkey(02aa)"}
          {:at (Instant/parse "2026-11-05T04:00:20Z"), :peer "Pubkey(02aa)"}
          {:at (Instant/parse "2026-11-05T04:00:40Z"), :peer "Pubkey(02aa)"}
          {:at (Instant/parse "2026-11-05T04:05:00Z"), :peer "Pubkey(03bb)"}]
         (resources/disconnects log)))
  (testing "without network debug lines it can't tell"
    (is (nil? (resources/disconnects ["  2026-11-05T04:00:30Z  INFO fnn::fiber::channel: x"])))))

(deftest loop-check
  (let [history [{:type :info, :f :lift-resources, :value nil, :time 1}
                 {:type :info, :f :lift-resources, :time 2
                  :value {"n2" {:lifted :cpu, :since "2026-11-05T04:00:05Z", :wall "2026-11-05T04:00:35Z"}}}]
        ws      (resources/windows history)]
    (is (= [{:node "n2", :from (Instant/parse "2026-11-05T04:00:05Z")
             :to (Instant/parse "2026-11-05T04:01:35Z")}]
           ws))
    (testing "three disconnects of one peer within the window and the minute after"
      (let [r (resources/loop-check ws {"n1" (resources/disconnects log), "n2" []})]
        (is (false? (:valid? r)))
        (is (= [{:window-node "n2", :from "2026-11-05T04:00:05Z", :node "n1", :peer "Pubkey(02aa)"
                 :disconnects 3}]
               (:loops r)))))
    (testing "two is not a loop, and a disconnect outside every window doesn't count"
      (is (true? (:valid? (resources/loop-check ws {"n1" (drop-last 2 (resources/disconnects log))})))))
    (testing "a node without debug lines makes it unknown"
      (is (= :unknown (:valid? (resources/loop-check ws {"n1" nil})))))))

(deftest limits-check
  (let [limit (fn [t nodes] {:type :info, :process :nemesis, :f :limit-resources, :time t
                             :value {:limit {:nodes nodes, :cpus 0.2}
                                     :nodes (into {} (for [n nodes] [n {:cpu.max "20000 100000"}]))}})
        lift  (fn [t nodes] {:type :info, :process :nemesis, :f :lift-resources, :time t
                             :value (into {} (for [n nodes]
                                               [n {:lifted :cpu, :since "2026-11-05T04:00:00Z"
                                                   :wall "2026-11-05T04:00:30Z"}]))})]
    (testing "every limit lifted"
      (is (= {:valid? true, :limits 3, :lifted 3, :nemesis-errors []}
             (resources/limits-check [(limit 1 ["n1" "n2"]) (lift 2 ["n1" "n2"]) (limit 3 ["n3"]) (lift 4 ["n3"])]))))
    (testing "a lift that threw, so the limit stayed"
      (let [r (resources/limits-check [(limit 1 ["n1"])
                                       {:type :info, :process :nemesis, :f :lift-resources, :time 2
                                        :exception {:via []}, :error "indeterminate: Docker API 500"}])]
        (is (false? (:valid? r)))
        (is (= 0 (:lifted r)))
        (is (= [{:f :lift-resources, :time 2, :error "indeterminate: Docker API 500"}] (:nemesis-errors r)))))))
