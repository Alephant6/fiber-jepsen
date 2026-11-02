(ns jepsen.fiber.ledger
  "With --ledger: checks that money adds up at every node, not just on every
  channel. The per-channel checks (jepsen.fiber.checker) pass even when a payment
  is only half done: if n2 settles a forwarded TLC with n3 but the upstream TLC
  from n1 fails, each channel still conserves its total and both ends still agree,
  yet n2 has paid n3 for nothing.

  For each node the checker compares its balance change over the channels opened
  during setup (the sum of its local balances) with the change the payments imply:

    sender      - (amount + fee)
    receiver    + amount
    forwarder   + fee        (the node between them on the line)

  It counts the payments the nodes themselves report: the final read lists every
  payment session on every node (list_payments), and each payment carries its
  attempt id in a custom record, so payments whose send_payment reply the client
  never saw (a timeout during a pause, say) are counted too. Amounts and
  destinations come from the history; statuses and fees from the senders.

  Invalid when a node's change differs from the expected one, when the status the
  client saw when a payment finished differs from the sender's final status (or
  the client was told no payment was created, and one succeeded), or when one
  attempt produced more than one payment. A difference is :unknown instead when
  the final records can't show every settlement: a channel that is closed,
  missing or still holds TLCs, a payment that hasn't finished, a payment the
  sender no longer lists, or a payment the checker can't attribute."
  (:require [jepsen.checker :as checker]
            [jepsen.fiber.checker :as fc]))

(defn intermediates
  "The nodes strictly between `from` and `to` on the line of channels through
  `nodes`, or nil if either is not on it."
  [nodes from to]
  (let [nodes (vec nodes)
        i     (.indexOf ^java.util.List nodes from)
        j     (.indexOf ^java.util.List nodes to)]
    (when-not (or (neg? i) (neg? j))
      (subvec nodes (inc (min i j)) (max i j)))))

(defn attempts
  "{attempt-id attempt} for every payment the test asked for: :pay completions and
  the probe's payments. :client is what the client learned when the attempt ended:
  \"Success\" or \"Failed\" (reported by the node), :none (the node certainly
  created no payment), or nil (unknown, e.g. the call timed out)."
  [history]
  (let [pays   (for [op    history
                     :when (and (= :pay (:f op))
                                (#{:ok :fail :info} (:type op))
                                (get-in op [:value :id]))]
                 (let [v (:value op)]
                   (assoc (select-keys v [:id :from :to :amount :hash])
                          :source :pay
                          :client (case (:type op)
                                    :ok   "Success"
                                    :fail (if (:hash v) "Failed" :none)
                                    nil))))
        probes (for [op    history
                     :when (and (= :probe (:f op)) (= :ok (:type op)))
                     p     (:value op)
                     :when (:id p)]
                 (assoc (select-keys p [:id :from :to :amount :hash])
                        :source :probe
                        :client (case (:status p)
                                  ("Success" "Failed") (:status p)
                                  "Rejected"           :none
                                  nil)))]
    (into {} (map (juxt :id identity)) (concat pays probes))))

(defn local-total
  "The sum of local balances over `channels` whose id is in `ids`."
  [channels ids]
  (reduce + 0 (for [ch channels :when (ids (:channel_id ch))]
                (fc/amount ch :local_balance))))

(defn channel-notes
  "Setup channels whose final record may not show every settlement: missing, no
  longer ready (settlement may have moved on chain, which this check doesn't
  read), or still holding TLCs."
  [initial final]
  (let [final (fc/by-channel final)]
    (vec (for [[node chs] initial
               ch         chs
               :let       [f (get-in final [(:channel_id ch) node])]
               :when      (or (nil? f)
                              (not= "channelready" (fc/state-name f))
                              (seq (:pending_tlcs f)))]
           {:node    node
            :channel (:channel_id ch)
            :state   (if f (get-in f [:state :state_name]) :missing)
            :tlcs    (count (:pending_tlcs f))}))))

(defn expected-changes
  "{node change} implied by `payments` (successful, each joined to its :attempt)."
  [nodes payments]
  (reduce (fn [m {:keys [fee attempt]}]
            (let [{:keys [from to amount]} attempt
                  mids                     (intermediates nodes from to)]
              (cond-> (-> m
                          (update from - (+ amount fee))
                          (update to + amount))
                (= 1 (count mids)) (update (first mids) + fee))))
          (zipmap nodes (repeat 0))
          payments))

(defn analyze
  "The ledger verdict for `nodes` (in line order), the channels at the end of setup
  {node [channel ...]}, the final read and the history."
  [nodes initial read history]
  (let [listed (:listed read)
        broken (into {} (filter (comp map? val)) listed)]
    (cond
      (nil? read)
      {:valid? :unknown, :outcome :incomplete, :error "no final read"}

      (empty? initial)
      {:valid? :unknown, :outcome :incomplete, :error "no channels recorded at setup"}

      (or (nil? listed) (seq broken))
      {:valid?  :unknown
       :outcome :incomplete
       :error   (if listed
                  {:list-payments-failed broken}
                  "the final read has no payment listing (run with --ledger)")}

      :else
      (let [attempts   (attempts history)
            payments   (for [[node ps] listed, p ps]
                         (assoc p :node node, :attempt (get attempts (:id p))))
            by-id      (group-by :id (filter :id payments))
            success    (filter #(= "Success" (:payment-status %)) payments)
            ; A successful payment can be counted only if the history says where it
            ; went and how much it carried, and its sender is the node listing it.
            countable? (fn [{:keys [node attempt]}]
                         (and attempt (= node (:from attempt)) (:to attempt) (:amount attempt)
                              (intermediates nodes (:from attempt) (:to attempt))))
            counted    (filter countable? success)
            ; The fee is attributed only when one node forwarded the payment.
            fee-split  (filter #(< 1 (count (intermediates nodes (get-in % [:attempt :from])
                                                           (get-in % [:attempt :to]))))
                               counted)
            totals     (into {} (for [[node chs] initial]
                                  (let [ids (set (map :channel_id chs))]
                                    [node [(local-total chs ids)
                                           (local-total (get-in read [:channels node]) ids)]])))
            expected   (expected-changes nodes counted)
            node-rows  (into (sorted-map)
                             (for [n nodes]
                               (let [[before after] (get totals n [0 0])
                                     change         (- after before)
                                     exp            (get expected n 0)]
                                 [n {:initial  before
                                     :final    after
                                     :change   change
                                     :expected exp
                                     :diff     (- change exp)}])))
            unbalanced (vec (for [[n row] node-rows :when (not (zero? (:diff row)))] n))
            ; What the client saw versus what the sender reports at the end.
            mismatches (vec (for [[id a] attempts
                                  :let   [ps       (get by-id id)
                                          statuses (set (map :payment-status ps))]
                                  :when  (and (seq ps)
                                              (case (:client a)
                                                ("Success" "Failed") (not= #{(:client a)} statuses)
                                                :none                (contains? statuses "Success")
                                                false))]
                              (assoc (select-keys a [:id :from :to :amount :source :client])
                                     :final-statuses (vec (sort statuses)))))
            duplicates (vec (for [[id ps] by-id :when (< 1 (count ps))]
                              {:id       id
                               :payments (mapv #(select-keys % [:node :hash :payment-status]) ps)}))
            listed-by  (into {} (for [[node ps] listed] [node (set (map :hash ps))]))
            missing    (vec (for [[_ a] attempts
                                  :when (and (:hash a)
                                             (not (contains? (get listed-by (:from a)) (:hash a))))]
                              (select-keys a [:id :from :to :hash :client])))
            unfinished (vec (for [p payments
                                  :when (not (#{"Success" "Failed"} (:payment-status p)))]
                              (select-keys p [:node :hash :id :payment-status])))
            notes      (channel-notes initial (:channels read))
            incomplete (cond-> {}
                         (seq notes)      (assoc :channels notes)
                         (seq unfinished) (assoc :unfinished-payments unfinished)
                         (seq missing)    (assoc :not-listed missing)
                         (not= (count counted) (count success))
                         (assoc :unattributed
                                (mapv #(select-keys % [:node :hash :id])
                                      (remove countable? success)))
                         (seq fee-split)  (assoc :fee-split-unknown
                                                 (mapv #(select-keys % [:node :hash :id]) fee-split)))
            found      (count (for [p payments
                                    :let  [a (:attempt p)]
                                    :when (and a (nil? (:hash a)) (nil? (:client a)))]
                                p))
            [valid? outcome] (cond
                               (or (seq mismatches) (seq duplicates)) [false :client-mismatch]
                               (and (seq unbalanced) (seq incomplete)) [:unknown :incomplete]
                               (seq unbalanced)                        [false :unbalanced]
                               :else                                   [true :balanced])]
        {:valid?             valid?
         :outcome            outcome
         :nodes              node-rows
         :unbalanced-nodes   unbalanced
         :payments           {:listed           (count payments)
                              :success          (count success)
                              :failed           (count (filter #(= "Failed" (:payment-status %)) payments))
                              :unfinished       (count unfinished)
                              ; Payments the client had no hash for, found on their sender.
                              :found-by-listing found}
         :status-mismatches  mismatches
         :duplicate-attempts duplicates
         :incomplete         incomplete}))))

(defn checker
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [read (->> history
                      (filter #(and (= :ok (:type %)) (= :read (:f %))))
                      last
                      :value)]
        (analyze (:nodes test) @(:initial-channels test) read history)))))
