(ns jepsen.fiber.health-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.nemesis :as n]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [health :as health]
                          [db :as db]]))

(def esc "\u001b")

(def log
  "An excerpt in fnn's format: tracing's pretty output, with colours, and Rust's
  panic message on stderr."
  [(str "  " esc "[2m2026-11-05T03:39:31.123456Z" esc "[0m " esc "[32m INFO" esc "[0m "
        esc "[1;32mfnn" esc "[0m" esc "[32m: Starting node with git version 0.6.1 (ae6f7d3)" esc "[0m")
   "    at crates/fiber-bin/src/main.rs:107"
   (str "  2026-11-05T03:40:02.5Z " esc "[31mERROR" esc "[0m fnn::fiber::network: "
        "Failed to send message to peer 0x02aa11bb: timeout after 3000 ms")
   "  2026-11-05T03:40:03.5Z ERROR fnn::fiber::network: Failed to send message to peer 0x03cc: timeout after 5000 ms"
   "  2026-11-05T03:40:04.0Z ERROR fnn::store: write failed"
   (str "  2026-11-05T03:40:04.1Z ERROR ractor::actor: Actor unexpectedly panicked (id: Actor { name: "
        "Some(\"Channel-Hash256(0x1b)\"), id: Local(7) }): \"commit should be OK\"")
   "  2026-11-05T03:40:04.2Z ERROR ractor::actor: Actor unexpectedly panicked (id: Actor { name: Some(\"Payment-1\"), id: Local(8) }): \"x\""
   "  2026-11-05T03:40:04.3Z ERROR ractor::actor: Actor unexpectedly panicked (id: Actor { name: Some(\"Payment-2\"), id: Local(9) }): \"x\""
   ""
   "thread 'tokio-runtime-worker' panicked at crates/fiber-store/src/native.rs:52:30:"
   "put should be ok: Error { message: \"IO error: No space left on device\" }"
   "note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"
   "thread 'main' panicked at 'old style', src/main.rs:2:5"
   "  2026-11-05T03:41:00Z  INFO fnn: Starting node with git version 0.6.1 (ae6f7d3)"
   "  2026-11-05T03:42:00Z  INFO fnn: SIGTERM received, shutting down"
   "thread 'tokio-rt-worker' (64) panicked at crates/fiber-lib/src/fiber/channel.rs:5833:14:"
   "network actor must be alive: SendErr"
   "  2026-11-05T03:43:00Z  INFO fnn: Starting node with git version 0.6.1 (ae6f7d3)"
   "thread 'x' panicked at a.rs:1:1:"
   "after the restart"])

(deftest parse-log
  (let [r (health/parse-log log)]
    (testing "panics in both message styles, with the time before each"
      (is (= [{:thread  "tokio-runtime-worker"
               :at      "crates/fiber-store/src/native.rs:52:30"
               :message "put should be ok: Error { message: \"IO error: No space left on device\" }"
               :after   "2026-11-05T03:40:04.3Z"
               :shutdown? false}
              {:thread "main", :at "'old style', src/main.rs:2:5", :after "2026-11-05T03:40:04.3Z"
               :shutdown? false}]
             (take 2 (:panics r)))))
    (testing "a panic with the thread's id after its name (newer Rust)"
      (is (= {:thread "tokio-rt-worker", :at "crates/fiber-lib/src/fiber/channel.rs:5833:14"
              :message "network actor must be alive: SendErr"}
             (select-keys (nth (:panics r) 2) [:thread :at :message])))
      (is (= 4 (count (:panics r)))))
    (testing "a panic after SIGTERM is in the shutdown, until fnn starts again"
      (is (= [true false] (map :shutdown? (drop 2 (:panics r))))))
    (testing "ERROR lines group by their message without numbers or hashes"
      (is (= 6 (:errors r)))
      (is (= ["Failed to send message to peer 0x..: timeout after N ms" 2]
             (first (:top-errors r)))))
    (testing "actors that ractor reported as panicked, by kind"
      (is (= {"Channel" 1, "Payment" 2} (:actor-panics r))))
    (testing "each start of fnn"
      (is (= 3 (:starts r))))
    (testing "an empty log"
      (is (= {:panics [], :actor-panics {}, :errors 0, :top-errors [], :starts 0}
             (health/parse-log []))))))

(def healthy
  {:process     {:pid "42", :state "Ssl"}
   :memory      {:oom-kills 0, :current-mib 80, :peak-mib 120}
   :rpc         {:ms 3}
   :store-write {:ms 5}})

(deftest status
  (is (= :ok (health/status healthy)))
  (is (= :unknown (health/status nil)))
  (is (= :exited (health/status (assoc healthy :process {:pid nil, :state "no-pidfile"}))))
  (is (= :exited (health/status (assoc healthy :process {:pid "42", :state "gone"}))))
  (is (= :exited (health/status (assoc-in healthy [:process :state] "Z"))))
  (is (= :stopped (health/status (assoc-in healthy [:process :state] "Tl"))))
  (is (= :unresponsive (health/status (assoc healthy :rpc {:ms 5000, :error "SocketTimeoutException"}))))
  (testing "node_info answers, but fnn can't write to its store"
    (is (= :not-working (health/status (assoc healthy :store-write {:ms 2, :error "new_invoice: IO error"}))))))

(deftest memory-events
  (is (= {:low 0, :high 0, :max 12, :oom 2, :oom_kill 1, :oom_group_kill 0}
         (health/parse-memory-events "low 0\nhigh 0\nmax 12\noom 2\noom_kill 1\noom_group_kill 0\n")))
  (is (= {} (health/parse-memory-events nil))))

(defn op
  [f value t]
  {:type :info, :process :nemesis, :f f, :value value, :time t})

(deftest unexpected-restarts
  (testing "a kill, then a start that starts it: expected"
    (is (= [] (health/unexpected-restarts
                [(op :kill [:one] 1) (op :kill {"n1" :killed} 2)
                 (op :start :all 3) (op :start {"n1" :started, "n2" :already-running} 4)]))))
  (testing "a start that finds fnn gone on a node nothing killed"
    (is (= [{:node "n2", :time 4}]
           (health/unexpected-restarts
             [(op :kill {"n1" :killed} 2)
              (op :start {"n1" :started, "n2" :started, "n3" :already-running} 4)]))))
  (testing "a kill covers only the next start"
    (is (= [{:node "n1", :time 6}]
           (health/unexpected-restarts
             [(op :kill {"n1" :killed} 2) (op :start {"n1" :started} 4)
              (op :start {"n1" :started} 6)]))))
  (testing "a SIGTERM kill's value (--kill-signal term) counts as a kill too"
    (is (= [] (health/unexpected-restarts
                [(op :kill {"n2" {:signal "TERM", :exit-ms 800}} 2)
                 (op :start {"n2" :started} 3)])))))

(def logs
  {"n1" {:panics [], :errors 0, :top-errors [], :starts 1}
   "n2" {:panics [], :errors 2, :top-errors [["x" 2]], :starts 3}})

(deftest check
  (let [final {"n1" healthy, "n2" healthy}]
    (testing "healthy"
      (let [r (health/check logs final [])]
        (is (true? (:valid? r)))
        (is (= {"n1" :ok, "n2" :ok} (:final-status r)))
        (is (= {"n1" 1, "n2" 3} (:fnn-starts r)))
        (is (= {"n1" 120, "n2" 120} (:peak-memory-mib r)))))
    (testing "a panic fails the run"
      (let [r (health/check (assoc-in logs ["n2" :panics] [{:thread "t", :at "a.rs:1:2", :message "m"}
                                                           {:thread "t", :at "a.rs:1:2", :message "m"}])
                            final [])]
        (is (false? (:valid? r)))
        (is (= 2 (:panics r)))
        (is (= [{:node "n2", :at "a.rs:1:2", :count 2, :message "m", :first-after nil}]
               (:panic-sites r)))
        (is (= 0 (:shutdown-panics r)))))
    (testing "panicked actors, per node and kind"
      (let [r (health/check (assoc-in logs ["n2" :actor-panics] {"Payment" 3, "Channel" 1})
                            final [])]
        (is (= {"n2" {"Channel" 1, "Payment" 3}} (:panicked-actors r)))
        (is (= {} (:panicked-actors (health/check logs final []))))))
    (testing "a panic in the graceful shutdown fails it too, and is marked"
      (let [r (health/check (assoc-in logs ["n1" :panics] [{:thread "t", :at "c.rs:5:6", :message "m"
                                                           :shutdown? true}])
                            final [])]
        (is (false? (:valid? r)))
        (is (= 1 (:shutdown-panics r)))
        (is (:during-shutdown (first (:panic-sites r))))))
    (testing "a node that looks alive but can't write"
      (let [r (health/check logs (assoc-in final ["n2" :store-write :error] "boom") [])]
        (is (false? (:valid? r)))
        (is (= :not-working (get-in r [:unhealthy "n2" :status])))))
    (testing "an OOM kill"
      (is (= {"n1" 1} (:oom-kills (health/check logs (assoc-in final ["n1" :memory :oom-kills] 1) [])))))
    (testing "fnn exited on its own and a harness start hid it"
      (let [r (health/check logs final [(op :start {"n1" :started} 9)])]
        (is (false? (:valid? r)))
        (is (= [{:node "n1", :time 9}] (:unexpected-restarts r)))))
    (testing "without a final sample, or a log, it can't tell"
      (is (= :unknown (:valid? (health/check logs nil []))))
      (is (= :no-sample (:final-status (health/check logs nil []))))
      (is (= :unknown (:valid? (health/check (assoc logs "n1" nil) final [])))))))

(deftest compose
  (let [faults (reify n/Nemesis
                 (setup! [this _] this)
                 (invoke! [_ _ op] (assoc op :value :faulted))
                 (teardown! [_ _]))
        nem    (health/compose faults)]
    (testing "fault ops still reach the faults' nemesis, which needn't support Reflection"
      (is (= :faulted (:value (n/invoke! nem {} {:f :kill}))))
      (is (= :faulted (:value (n/invoke! (n/setup! nem {}) {} {:f :start-partition})))))
    (testing "every completion carries the control node's wall clock, before and after setup"
      (let [before (System/currentTimeMillis)]
        (doseq [nem [nem (n/setup! nem {})]]
          (is (<= before (:wall-ms (n/invoke! nem {} {:f :kill})) (System/currentTimeMillis))))))))

(deftest pidfile
  (testing "the health sample reads the pidfile jepsen.fiber.db writes"
    (is (str/starts-with? db/pidfile "/"))))

(deftest in-the-generator
  (let [opts {:faults [:kill], :nodes ["n1" "n2" "n3"], :rate 5, :time-limit 10, :quiesce 5
              :nemesis-interval 3, :kill-targets [:one]}
        fs   (->> (gt/simulate (gen/context {:concurrency 3})
                               (:generator (fiber/fiber-test opts))
                               (fn [_ctx op] (-> op (assoc :type :ok) (update :time + 50000000))))
                  (filter #(#{:invoke :info} (:type %)))
                  (map :f))]
    (testing "one sample, by the nemesis, after the probe and before the final read"
      (is (= [:probe :health :read] (filter #{:probe :health :read} fs))))))
