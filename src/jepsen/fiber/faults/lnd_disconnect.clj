(ns jepsen.fiber.faults.lnd-disconnect
  "The lnd-disconnect fault: cuts one link of the swap environment's Lightning
  side (jepsen.fiber.cch) with iptables, then restores it. Needs --cch.

  --lnd-disconnect-targets picks the links to choose from:
    :cch   CCH to the hub's LND (gRPC): CCH can't create hold invoices, send
           Lightning payments or follow them. The rules go on the cch host.
    :peer  the hub's LND to the BTC user's LND (Lightning p2p): payments between
           them can't move. The rules go on lnd-ingrid.
  --lnd-disconnect-mode how: :drop discards the packets both ways, so calls hang
  until they time out; :reject answers every TCP packet to the other end with a
  reset, so connections fail at once.

  These hosts aren't Jepsen nodes, so the partition fault's healing (which
  flushes the nodes' rules) doesn't reach them, and no other fault adds rules
  there. Healing deletes this fault's rules from both hosts and asks lnd-ingrid
  to reconnect to lnd-bob, rather than wait out LND's reconnection backoff."
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen [control :as c]
                    [nemesis :as n]]
            [jepsen.fiber [cch :as cch]
                          [schedule :as schedule]]
            [slingshot.slingshot :refer [try+]]))

(def target-specs #{:cch :peer})

(def modes #{:drop :reject})

(def links
  "Link -> the host whose rules cut it, and the host at its other end."
  {:cch  {:host cch/cch-host, :peer cch/hub-lnd}
   :peer {:host cch/hub-lnd, :peer cch/user-lnd}})

(defn rules
  "iptables rule specs (after -A or -D) that cut a host off `ip` in `mode`."
  [ip mode]
  (case mode
    :drop   [["OUTPUT" "-d" ip "-j" "DROP"]
             ["INPUT" "-s" ip "-j" "DROP"]]
    :reject [["OUTPUT" "-d" ip "-p" "tcp" "-j" "REJECT" "--reject-with" "tcp-reset"]
             ["INPUT" "-s" ip "-p" "tcp" "-j" "REJECT" "--reject-with" "tcp-reset"]]))

(defn- iptables!
  [& args]
  (c/su (apply c/exec :iptables "-w" args)))

(defn cut!
  [link mode]
  (let [{:keys [peer]} (links link)]
    (doseq [r (rules (cch/ips peer) mode)]
      (apply iptables! "-A" r))
    :cut))

(defn- heal-host!
  "Deletes every rule this fault may have added on the bound host, as often as
  each is there."
  [peer-ip]
  (doseq [mode modes
          r    (rules peer-ip mode)]
    (loop [n 0]
      (when (and (< n 16)
                 (try+ (apply iptables! "-D" r) true
                       (catch [:type :jepsen.control/nonzero-exit] _ false)))
        (recur (inc n)))))
  :healed)

(defn- try-op
  [what f]
  (try (f)
       (catch Exception e
         (warn e "lnd-disconnect nemesis:" what)
         (str "failed: " (.getMessage e)))))

(defn reconnect!
  "Asks lnd-ingrid to connect to lnd-bob, as a channel peer."
  []
  (let [bob (:identity_pubkey (cch/lnd-get cch/user-lnd "/v1/getinfo"))]
    (try (cch/lnd-post cch/hub-lnd "/v1/peers" {:addr {:pubkey bob
                                                       :host   (str (cch/ips cch/user-lnd) ":9735")}
                                                :perm true})
         :connected
         (catch clojure.lang.ExceptionInfo e
           (if (re-find #"already connected" (str (ex-message e)))
             :already-connected
             (throw e))))))

(defn heal!
  [test]
  (into (sorted-map)
        (concat (for [[link {:keys [host peer]}] links]
                  [link (try-op (str "heal " host)
                                #(cch/on-host test host (heal-host! (cch/ips peer))))])
                [[:reconnect (try-op "reconnect" reconnect!)]])))

(defn nemesis
  []
  (reify
    n/Reflection
    (fs [_] #{:start-lnd-disconnect :stop-lnd-disconnect})

    n/Nemesis
    (setup! [this _test] this)

    (invoke! [_ test op]
      (case (:f op)
        :start-lnd-disconnect
        (let [{:keys [link mode]} (:value op)
              host                (:host (links link))]
          (assoc op :value {:link   link
                            :mode   mode
                            :result (try-op (str "cut " (name link))
                                            #(cch/on-host test host (cut! link mode)))}))

        :stop-lnd-disconnect
        (assoc op :value (heal! test))))

    (teardown! [_ test]
      (heal! test))))

(defn package
  [opts]
  (let [needed? ((:faults opts) :lnd-disconnect)]
    {:generator       nil
     :final-generator (when needed? {:type :info, :f :stop-lnd-disconnect})
     :nemesis         (nemesis)
     :perf            #{{:name  "lnd"
                         :start #{:start-lnd-disconnect}
                         :stop  #{:stop-lnd-disconnect}
                         :color "#C8A0E9"}}}))

(defn ops
  "Endless pairs of a cut of a link drawn from `r` and its healing."
  [r opts]
  (let [specs (vec (:lnd-disconnect-targets opts [:cch :peer]))
        mode  (:lnd-disconnect-mode opts :drop)]
    (mapcat (fn [link]
              [{:type :info, :f :start-lnd-disconnect, :value {:link link, :mode mode}}
               {:type :info, :f :stop-lnd-disconnect, :value nil}])
            (repeatedly #(schedule/pick r specs)))))

(def fault
  {:faults  #{:lnd-disconnect}
   :stream  :lnd-disconnect
   :package package
   :ops     ops})
