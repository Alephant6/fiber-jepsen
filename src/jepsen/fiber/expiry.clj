(ns jepsen.fiber.expiry
  "C-03 (--expiry-check, with --onchain): when the next hop stays down past the
  expiry of a TLC, the node that offered the TLC for its own payment gets it
  back, on chain once it has force-closed the channel, and the payment ends
  Failed.

  A release build of fnn makes a payment's TLC last at least 4 hours, so the
  outage fault (jepsen.fiber.faults.outage, --faults outage --outage-jump)
  moves every clock, the CKB node's included, past the expiry while the node is
  down. A node force-closes a channel once an offered TLC is within 2/3 of the
  commitment delay of its expiry (160 minutes in a release build), checking
  every 60 s; after the commitment delay its watchtower takes the TLC back.

  After --onchain's settle, :final-payments reads every payment's status again.
  The check looks at every TLC a node's record of a closed channel still lists
  that the node offered for its own payment (it has no forwarding channel), and
  wants:

    resolved  every payment Success or Failed once the channels have settled;
    refunded  the node's receipt from the channel to hold every such TLC whose
              payment Failed. A TLC still in the balance goes to the peer only
              when the peer claims it with the preimage, which settles the
              payment; so the receipt must be at least balance + reserve - the
              channel's fees - the TLCs it offered that aren't a Failed payment
              of its own.

  Each TLC is a whole multiple of 0.01 CKB and fees are a few thousand
  shannons, so a TLC that went the wrong way shows. The verdict is :unknown
  when no such TLC was left on a closed channel: the outages then caught no
  payment in flight, and the run tested nothing of C-03."
  (:require [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.fiber [checker :as fc]
                          [client :as client]
                          [onchain :as onchain]]))

(def terminal
  #{"Success" "Failed"})

(defn offered?
  "Whether a pending_tlcs entry is outbound: its status is {:Outbound s}."
  [tlc]
  (let [s (:status tlc)]
    (if (map? s)
      (contains? s :Outbound)
      (str/includes? (str s) "Outbound"))))

(defn- tlc-status
  [tlc]
  (let [s (:status tlc)]
    (if (map? s) (str (name (key (first s))) "/" (val (first s))) (str s))))

(defn payer-tlcs
  "Every TLC that a node's record of a closed channel lists, offered by that
  node for a payment of its own, with the payment's final status from
  `statuses` {[node hash] status}."
  [channels statuses]
  (vec (for [[id ends]  (sort-by key (fc/by-channel channels))
             [node rec] (sort-by key ends)
             :when      (= "closed" (fc/state-name rec))
             tlc        (:pending_tlcs rec)
             :when      (and (offered? tlc) (nil? (:forwarding_channel_id tlc)))]
         {:channel    id
          :node       node
          :hash       (:payment_hash tlc)
          :amount     (fc/amount tlc :amount)
          :expiry     (fc/amount tlc :expiry)
          :tlc-status (tlc-status tlc)
          :payment    (get statuses [node (:payment_hash tlc)])})))

(defn refund-check
  "For one node's record `rec` of a settled channel: its receipt `received`
  against the least it may get when every TLC in `failed` (amounts of its own
  payments that Failed) comes back. `capacity` and `fee` are the channel's."
  [capacity fee received rec failed]
  (let [local    (fc/amount rec :local_balance)
        remote   (fc/amount rec :remote_balance)
        reserve  (quot (- capacity local remote) 2)
        offered  (reduce + 0 (map #(fc/amount % :amount) (filter offered? (:pending_tlcs rec))))
        refunds  (reduce + 0 failed)
        least    (- (+ local reserve) (- offered refunds) fee)]
    {:received received
     :least    least
     :refunds  refunds
     :short    (max 0 (- least received))
     :ok?      (<= least received)}))

(defn analyze
  "The verdict from --onchain's settle value and the :final-payments value."
  [settle final]
  (cond
    (nil? settle) {:valid? :unknown, :error "no settle read (--expiry-check needs --onchain)"}
    (nil? final)  {:valid? :unknown, :error "no final payment read"}
    (:error settle) {:valid? :unknown, :error (:error settle)}
    :else
    (let [statuses   (into {} (for [p final] [[(:node p) (:hash p)] (:status p)]))
          unresolved (vec (for [p final :when (not (terminal (:status p)))]
                            (select-keys p [:node :hash :status])))
          tlcs       (payer-tlcs (:channels settle) statuses)
          ends       (fc/by-channel (:channels settle))
          checks     (vec (for [[[id node] ts] (group-by (juxt :channel :node) tlcs)
                                :let [trace (get-in settle [:traces id])
                                      lock  (get-in settle [:locks node])]]
                            (if (or (nil? trace) (:error trace) (:funding-status trace)
                                    (seq (:live trace)) (nil? lock))
                              {:channel id, :node node, :status :unsettled}
                              (let [{:keys [fee net]} (onchain/accounting trace)
                                    failed (for [t ts :when (= "Failed" (:payment t))] (:amount t))]
                                (merge {:channel id, :node node
                                        :status  :checked
                                        :tlcs    (count ts)
                                        :failed  (count failed)}
                                       (refund-check (:capacity trace) fee (get net lock 0)
                                                     (get-in ends [id node]) failed))))))
          lost       (filterv #(and (= :checked (:status %)) (not (:ok? %))) checks)
          unsettled  (filterv #(= :unsettled (:status %)) checks)]
      {:valid?      (cond (or (seq unresolved) (seq lost))   false
                          (or (empty? tlcs) (seq unsettled)) :unknown
                          :else                             true)
       :payer-tlcs  (count tlcs)
       :by-payment  (into (sorted-map) (frequencies (map #(or (:payment %) "missing") tlcs)))
       :refunded    (reduce + 0 (map :refunds (filter :ok? checks)))
       :unresolved  unresolved
       :lost        lost
       :unsettled   unsettled
       :checks      checks
       :tlcs        tlcs})))

(defmethod client/invoke-op :final-payments
  [test _node op]
  (assoc op :type :ok, :value (client/payment-statuses test)))

(defn checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [value (fn [f] (->> history
                               (filter #(and (= :ok (:type %)) (= f (:f %))))
                               last
                               :value))]
        (analyze (value :settle) (value :final-payments))))))
