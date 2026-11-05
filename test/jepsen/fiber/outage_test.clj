(ns jepsen.fiber.outage-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen [control :as c]
                    [db :as db]]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [chain :as chain]
                          [db :as fdb]
                          [faults :as faults]
                          [rpc :as rpc]
                          [schedule :as schedule]]
            [jepsen.fiber.faults [clock :as clock]
                                 [outage :as outage]]))

(def nodes ["n1" "n2" "n3"])

(def opts
  {:nodes nodes, :faults [:outage], :outage-targets [:middle], :outage-duration [120 180]
   :outage-after 0})

(defn draw
  [seed opts n]
  (take n (outage/ops (schedule/stream (schedule/streams seed) :outage) opts)))

(deftest ops
  (let [ops (draw 7 opts 200)]
    (testing "each op is one outage of the middle node, for a drawn time"
      (is (every? #{:outage} (map :f ops)))
      (is (every? #{"n2"} (map (comp :node :value) ops)))
      (is (every? #(<= 120 % 180) (map (comp :duration-s :value) ops)))
      (is (< 1 (count (distinct (map (comp :duration-s :value) ops))))))
    (testing "the seed fixes them"
      (is (= ops (draw 7 opts 200)))
      (is (not= ops (draw 8 opts 200)))))
  (testing "with :outage-after, a sleep comes first"
    (let [ops (draw 7 (assoc opts :outage-after 30) 3)]
      (is (= {:type :sleep, :value 30} (first ops)))
      (is (= (take 2 (draw 7 opts 2)) (rest ops)))))
  (testing "one: any node"
    (is (= (set nodes) (set (map (comp :node :value) (draw 7 (assoc opts :outage-targets [:one]) 200))))))
  (testing "middle needs a node on two channels"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"no node in topology"
                          (doall (draw 7 (assoc opts :nodes ["n1" "n2"]) 1)))))
  (testing "a jump can't run with the clock fault, which resets fnn's clock"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"can't run with --faults clock"
                          (draw 7 (assoc opts :faults [:outage :clock], :outage-jump 600) 1)))
    (is (= 1 (count (draw 7 (assoc opts :faults [:outage :clock]) 1))))))

(deftest tlc-states
  (is (= "Committed" (outage/outbound-state {:status {:Outbound "Committed"}})))
  (is (nil? (outage/outbound-state {:status {:Inbound "Committed"}})))
  (is (= "RemoteRemoved" (outage/outbound-state {:status "Outbound(RemoteRemoved)"})))
  (is (outage/unresolved-offer? {:status {:Outbound "Committed"}}))
  (is (outage/unresolved-offer? {:status {:Outbound "RemoveWaitAck"}}))
  (testing "not before the peer has it, nor once its removal is confirmed"
    (is (not (outage/unresolved-offer? {:status {:Outbound "LocalAnnounced"}})))
    (is (not (outage/unresolved-offer? {:status {:Outbound "RemoveAckConfirmed"}}))))
  (is (not (outage/unresolved-offer? {:status {:Inbound "Committed"}}))))

(defn tlc
  [status amount]
  {:status status, :amount (rpc/int->hex amount), :expiry (rpc/int->hex 5000), :payment_hash "0xh"})

(def test-map
  {:nodes nodes, :topology :line, :pubkeys (atom {"n1" "pk1", "n2" "pk2", "n3" "pk3"})})

(deftest in-flight
  (let [calls (atom [])]
    (with-redefs [rpc/call (fn [node method params & _]
                             (swap! calls conj [node method params])
                             (case node
                               "n1" {:channels [{:channel_id    "0xc12"
                                                 :pending_tlcs [(tlc {:Outbound "Committed"} 100)
                                                                (tlc {:Inbound "Committed"} 7)]}]}
                               "n3" (throw (java.net.ConnectException. "down"))))]
      (is (= [{:from "n1", :channel "0xc12", :hash "0xh", :amount 100, :expiry 5000, :state "Committed"}]
             (outage/in-flight test-map "n2"))))
    (testing "it asks each neighbour for its channels with the target"
      (is (= [["n1" "list_channels" {:pubkey "pk2"}] ["n3" "list_channels" {:pubkey "pk2"}]] @calls)))))

(defn fake-on-nodes
  ([test f] (fake-on-nodes test (:nodes test) f))
  ([test ns f] (into {} (for [n ns] [n (f test n)]))))

(defn run-outage
  "Runs one outage of n2 with every side effect recorded in the returned atom.
  `tlcs` is what the neighbours report before and after the kill."
  [jump tlcs & {:keys [chain-offset] :or {chain-offset (fn [] 3599.4)}}]
  (let [events (atom [])
        fake-db (reify db/Kill
                  (kill! [_ _ n] (swap! events conj [:kill n]))
                  (start! [_ _ n] (swap! events conj [:start n])))
        nemesis (outage/->Outage (atom 0))
        test    (assoc test-map :db fake-db, :outage-jump jump, :outage-await 0)
        file    (java.io.File/createTempFile "chain-clock" ".txt")]
    (spit file "+0\n")
    (with-redefs [c/on-nodes                 fake-on-nodes
                  outage/chain-offset-file   (.getPath file)
                  outage/in-flight           (fn [_ _] tlcs)
                  outage/chain-offset-s      chain-offset
                  clock/set-offset!          (fn [s] (swap! events conj [:offset s]))
                  clock/settle-ms            0
                  clock/fnn-offset-s         (fn [n] (if (= "n2" n) 3600.2 3599.9))
                  fdb/await-rpc!             (fn [n] (swap! events conj [:rpc n]))]
      (.setup! nemesis test)
      (let [op (.invoke! nemesis test {:type :info, :f :outage, :value {:node "n2", :duration-s 0}})]
        {:events @events, :value (:value op), :chain-file (slurp file)}))))

(deftest outage
  (testing "with a jump, a TLC caught moves every clock, the down node's too"
    (let [{:keys [events value chain-file]} (run-outage 3600 [{:from "n1", :amount 100}])]
      (is (= "+3600\n" chain-file))
      (is (= [[:offset 0] [:offset 0] [:offset 0]                 ; setup: the chain's offset
              [:kill "n2"]
              [:offset 3600] [:offset 3600] [:offset 3600]
              [:start "n2"] [:rpc "n2"]]
             events))
      (is (= {"n1" 3599.9, "n3" 3599.9} (:fnn-offset-s value)))
      (is (= 3599.4 (:chain-offset-s value)))
      (is (= 3600.2 (:restart-offset-s value)))
      (is (= [3600 3600] [(:offset-s value) (:jump-s value)]))
      (is (= 1 (count (:caught value))))))
  (testing "nothing in flight: no jump"
    (let [{:keys [events value chain-file]} (run-outage 3600 [])]
      (is (= "+0\n" chain-file))
      (is (= [[:offset 0] [:offset 0] [:offset 0] [:kill "n2"] [:start "n2"] [:rpc "n2"]] events))
      (is (nil? (:offset-s value)))))
  (testing "without --outage-jump, an outage is a kill and a restart"
    (let [{:keys [events value]} (run-outage 0 [{:from "n1", :amount 100}])]
      (is (= [[:kill "n2"] [:start "n2"] [:rpc "n2"]] events))
      (is (nil? (:restart-offset-s value))))))

(deftest chain-offset-file
  (let [file (java.io.File/createTempFile "chain-clock" ".txt")]
    (with-redefs [outage/chain-offset-file (.getPath file)]
      (outage/write-chain-offset! 34200)
      (is (= "+34200\n" (slurp file)))
      (is (= 34200 (outage/read-chain-offset)))
      (io/delete-file file)
      (is (nil? (outage/read-chain-offset))))))

(deftest chain-offset
  (with-redefs [chain/tip-time-ms (fn [] (+ (System/currentTimeMillis) 3600000))]
    (is (<= 3599 (outage/chain-offset-s) 3601)))
  (with-redefs [chain/call (fn [& _] (throw (java.net.ConnectException. "refused")))]
    (is (= "failed: refused" (outage/chain-offset-s)))))

(defn completion
  [value]
  {:type :info, :f :outage, :time 1, :value value})

(deftest check
  (let [jumped {:node "n2", :duration-s 150, :waited-ms 40, :caught [{:from "n1"}]
                :offset-s 34200, :jump-s 34200
                :fnn-offset-s {"n1" 34199.8, "n3" 34200.1}, :chain-offset-s 34199.3
                :restart-offset-s 34200.0}]
    (testing "every clock moved"
      (let [r (outage/check 34200 [{:type :info, :f :outage, :value {:node "n2", :duration-s 150}}
                                   (completion jumped)])]
        (is (true? (:valid? r)))
        (is (= [1 1 1 1 34200] ((juxt :outages :caught :tlcs :jumps :offset-s) r)))))
    (testing "the chain's clock didn't move"
      (let [r (outage/check 34200 [(completion (assoc jumped :chain-offset-s -0.6))])]
        (is (false? (:valid? r)))
        (is (= [{:time 1, :clock "chain", :set 34200, :measured -0.6}] (:not-taken r)))))
    (testing "the chain didn't answer"
      (is (= :unknown (:valid? (outage/check 34200 [(completion (assoc jumped :chain-offset-s "failed: x"))]))))))
  (testing "no outage caught a TLC, so nothing jumped"
    (let [r (outage/check 34200 [(completion {:node "n2", :caught []})])]
      (is (= :unknown (:valid? r)))
      (is (= [1 0 0] ((juxt :outages :caught :jumps) r)))))
  (testing "without a jump there is nothing to measure"
    (is (true? (:valid? (outage/check 0 [(completion {:node "n2", :caught []})]))))))

(deftest environment
  (testing "fnn runs under libfaketime only when outages jump"
    (is (= clock/library (:LD_PRELOAD (faults/fnn-env (assoc opts :outage-jump 600)))))
    (is (= {} (faults/fnn-env opts)))))

(deftest options
  (let [parse (fn [& args] (cli/parse-opts args fiber/cli-opts))]
    (is (= {:outage-targets [:middle], :outage-duration [120 180], :outage-await 30, :outage-jump 0
            :outage-after 30}
           (select-keys (:options (parse)) [:outage-targets :outage-duration :outage-await :outage-jump
                                            :outage-after])))
    (is (= [:outage] (:faults (:options (parse "--faults" "outage")))))
    (is (= 33660000 (:tlc-expiry-limit (:options (parse "--tlc-expiry-limit" "33660000")))))
    (is (seq (:errors (parse "--outage-targets" "all"))))
    (is (seq (:errors (parse "--outage-jump" "-5"))))
    (is (seq (:errors (parse "--outage-duration" "0-10"))))))
