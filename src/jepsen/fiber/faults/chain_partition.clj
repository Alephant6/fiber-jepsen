(ns jepsen.fiber.faults.chain-partition
  "The chain-partition fault: cuts fnn nodes off the CKB node with iptables on
  the fnn hosts, while the nodes keep talking to each other. fnn reads the
  chain over HTTP (ckb.rpc_url), so during a cut it can't submit funding,
  shutdown, commitment or settlement transactions, or see them confirm.

  --chain-partition-targets picks whom to cut: :one (a random fnn node) or
  :all; --chain-partition-mode how: :drop discards the packets both ways, so
  requests time out (toxiproxy's timeout); :reject answers every new packet to
  the CKB node with a TCP reset, so requests fail at once (reset_peer).

  The rules carry a comment, and healing deletes only them. The partition
  fault's healing flushes every rule on every node, so it ends a chain
  partition too when both run."
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen [control :as c]
                    [nemesis :as n]]
            [jepsen.control.net :as cn]
            [jepsen.fiber.schedule :as schedule]
            [slingshot.slingshot :refer [try+]]))

(def chain-host
  "The CKB node's host name in docker/docker-compose.yml."
  "chain")

(def target-specs #{:one :all})

(def modes #{:drop :reject})

(def tag "fiber-chain-partition")

(defn rules
  "iptables rule specs (after -A or -D) that cut a host off `ip` in `mode`."
  [ip mode]
  (let [mark ["-m" "comment" "--comment" tag]]
    (case mode
      :drop   [(into ["OUTPUT" "-d" ip] (concat mark ["-j" "DROP"]))
               (into ["INPUT" "-s" ip] (concat mark ["-j" "DROP"]))]
      :reject [(into ["OUTPUT" "-d" ip "-p" "tcp"] (concat mark ["-j" "REJECT" "--reject-with" "tcp-reset"]))])))

(defn- iptables!
  [& args]
  (c/su (apply c/exec :iptables "-w" args)))

(defn cut!
  [ip mode]
  (doseq [r (rules ip mode)]
    (apply iptables! "-A" r))
  :cut)

(defn heal!
  "Deletes every rule this fault may have added, as often as each is there."
  [ip]
  (doseq [mode modes
          r    (rules ip mode)]
    (loop [n 0]
      (when (< n 16)
        (when (try+ (apply iptables! "-D" r) true
                    (catch [:type :jepsen.control/nonzero-exit] _ false))
          (recur (inc n))))))
  :healed)

(defn nemesis
  []
  (reify
    n/Reflection
    (fs [_] #{:start-chain-partition :stop-chain-partition})

    n/Nemesis
    (setup! [this _test] this)

    (invoke! [_ test op]
      (let [ip (cn/ip chain-host)]
        (case (:f op)
          :start-chain-partition
          (let [{:keys [nodes mode]} (:value op)]
            (assoc op :value {:nodes nodes, :mode mode
                              :result (c/on-nodes test nodes (fn [_ _] (cut! ip mode)))}))

          :stop-chain-partition
          (assoc op :value (c/on-nodes test (fn [_ _] (heal! ip)))))))

    (teardown! [_ test]
      (try (c/on-nodes test (fn [_ _] (heal! (cn/ip chain-host))))
           (catch Exception e (warn e "Couldn't heal the chain partition"))))))

(defn package
  [opts]
  (let [needed? ((:faults opts) :chain-partition)]
    {:generator       nil
     :final-generator (when needed? {:type :info, :f :stop-chain-partition})
     :nemesis         (nemesis)
     :perf            #{{:name  "chain"
                         :start #{:start-chain-partition}
                         :stop  #{:stop-chain-partition}
                         :color "#E9D3A0"}}}))

(defn ops
  "Endless pairs of a cut, of the nodes drawn from `r`, and its healing."
  [r opts]
  (let [nodes (vec (:nodes opts))
        specs (vec (:chain-partition-targets opts [:one :all]))
        mode  (:chain-partition-mode opts :drop)]
    (mapcat (fn [targets]
              [{:type :info, :f :start-chain-partition, :value {:nodes targets, :mode mode}}
               {:type :info, :f :stop-chain-partition, :value nil}])
            (repeatedly #(case (schedule/pick r specs)
                           :one [(schedule/pick r nodes)]
                           :all nodes)))))

(def fault
  {:faults  #{:chain-partition}
   :stream  :chain-partition
   :package package
   :ops     ops})
