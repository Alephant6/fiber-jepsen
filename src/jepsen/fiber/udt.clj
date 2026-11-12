(ns jepsen.fiber.udt
  "UDT channels and payments (--asset udt). Every channel the setup opens is
  funded with a user-defined token instead of CKB (open_channel's
  funding_udt_type_script), and every payment, invoice and probe names the token
  (udt_type_script), so the whole run moves UDT: balances, TLCs and fees are
  amounts of the token, the commitment transactions carry UDT cells, and a
  forced close settles them on chain. The CKB in such a channel is only what its
  cells need.

  The token is the devnet's simple UDT, which Fiber's udt-init mints to nodes
  1-3 and lists in every node's UDT whitelist as SIMPLE_UDT; n4 and up hold
  none, so a UDT run takes its nodes from n1-n3. The opener puts in as many
  units as a CKB channel has shannons (`funding`), so payment amounts and every
  check keep their scale. The acceptor adds the whitelist's auto-accept amount,
  1000 units, less than any payment: like a CKB acceptor, whose share is all
  reserve, it can send once it has been paid.

  The token's type script is read once at setup into `script`, an atom of this
  namespace rather than of the test map: the client and every payment kind's
  invoice read it from here, so a kind needs no change to pay in UDT. A process
  runs one test at a time, and setup sets it for each."
  (:require [clojure.string :as str]
            [jepsen.fiber.rpc :as rpc]))

(def simple-udt-args
  "The devnet simple UDT's type script args: the hash of the deployer's lock,
  the token's owner (Fiber's tests/nodes/deployer/config.yml)."
  "0x32e555f3ff8e135cece1351a6a2971518392c1e30375c1e006ad0ce8eac07947")

(def holders
  "The nodes the devnet mints the token to."
  #{"n1" "n2" "n3"})

(def funding
  "Units of the token the opener puts into each channel: the number of shannons
  in a CKB channel's 1000 CKB."
  100000000000)

(defonce ^{:doc "The token's type script while a UDT test runs, else nil."} script
  (atom nil))

(defn enabled?
  "Whether the test runs on UDT channels."
  [opts]
  (= :udt (:asset opts)))

(defn read-script
  "The simple UDT's type script, from `node`'s UDT whitelist (node_info)."
  [node]
  (let [udts (:udt_cfg_infos (rpc/call node "node_info"))
        udt  (first (filter #(= "SIMPLE_UDT" (:name %)) udts))]
    (when-not udt
      (throw (ex-info (str node " has no SIMPLE_UDT in its UDT whitelist")
                      {:type :no-udt, :node node, :udts (mapv :name udts)})))
    {:code_hash (get-in udt [:script :code_hash])
     :hash_type (str/lower-case (name (get-in udt [:script :hash_type])))
     :args      simple-udt-args}))

(defn setup!
  "Sets `script` for `test`: the token's type script with --asset udt, nil
  without. Returns it."
  [test]
  (reset! script (when (enabled? test) (read-script (first (:nodes test))))))

(defn payment-params
  "What a send_payment or new_invoice call adds to pay in the token, or nil."
  []
  (when-let [s @script]
    {:udt_type_script s}))

(defn channel-params
  "open_channel's funding parameters for the token, or nil for a CKB channel."
  []
  (when-let [s @script]
    {:funding_amount          (rpc/int->hex funding)
     :funding_udt_type_script s}))

(defn check-opts!
  "Throws when --asset udt comes with something it can't run with: a node that
  holds no token, or an option whose accounting is in CKB."
  [opts]
  (when (enabled? opts)
    (let [bad (cond
                (not-every? holders (map name (:nodes opts)))
                (str "nodes from " (str/join ", " (sort holders))
                     ": the devnet mints the token to them only")

                (:onchain opts)          "no --onchain: the on-chain check counts CKB"
                (:cch opts)              "no --cch: the swap setup opens UDT channels of its own"
                (:acceptor-funding opts) "no --acceptor-funding: it funds acceptors in CKB"
                (some #{:mpp} (:pay-kinds opts))
                "no --pay-kinds mpp: an acceptor holds too little of the token to split over")]
      (when bad
        (throw (IllegalArgumentException. (str "--asset udt needs " bad)))))))
