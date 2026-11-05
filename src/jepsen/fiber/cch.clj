(ns jepsen.fiber.cch
  "The cross-chain hub (CCH) and the Lightning side of a swap test (--cch, C-15),
  on the hosts of docker/docker-compose.cch.yml. None of them is a Jepsen node:

    bitcoind    Bitcoin Core, regtest
    lnd-ingrid  the hub operator's LND, which CCH drives
    lnd-bob     the BTC user's LND
    cch         fnn running only the CCH and RPC services, against the hub's
                Fiber node over RPC: Fiber's separate-service mode, as in its
                e2e test tests/bruno/e2e/cross-chain-hub-separate

  The CKB user is the first of :nodes (n1) and the hub's Fiber node the last
  (n3). Setup follows Fiber's tests/deploy/lnd-init/setup-lnd.sh and that e2e
  test: a funded bitcoind wallet; a channel from lnd-ingrid to lnd-bob, here with
  half of it pushed to bob so that swaps can go both ways from the start; CCH
  with the wrapped-BTC UDT; and two wrapped-BTC channels between n1 and n3, one
  opened by each, so that n1 can pay the hub and the hub can pay n1.

  The control node reaches the daemons' RPCs by host name (bitcoind's JSON-RPC,
  LND's REST API without TLS, CCH's JSON-RPC) and the hosts themselves over SSH,
  with the test's SSH options, to write configs and start and stop daemons.
  Jepsen downloads logs only from its nodes, so `db` fetches these hosts' logs
  into the store too, under the host's name."
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [store :as store]
                    [util :refer [await-fn]]]
            [jepsen.control.util :as cu]
            [jepsen.fiber [rpc :as rpc]
                          [topology :as topology]]
            [slingshot.slingshot :refer [try+]])
  (:import (java.util Base64)))

;; Hosts and roles

(def bitcoind-host "bitcoind")
(def hub-lnd "lnd-ingrid")
(def user-lnd "lnd-bob")
(def cch-host "cch")

(def hosts
  [bitcoind-host hub-lnd user-lnd cch-host])

(def ips
  "Each host's address in docker/docker-compose.cch.yml."
  {bitcoind-host "10.77.0.20"
   hub-lnd       "10.77.0.21"
   user-lnd      "10.77.0.22"
   cch-host      "10.77.0.23"})

(defn ckb-user
  "The Fiber node of the CKB user: the first of the test's nodes."
  [test]
  (first (:nodes test)))

(defn hub-node
  "The hub's Fiber node, which CCH drives over RPC: the last of the test's nodes."
  [test]
  (last (:nodes test)))

(def wrapped-btc-args
  "The wrapped-BTC UDT's type script args: Fiber's devnet mints this simple UDT
  (owned by the deployer) to nodes 1-3, and its e2e tests use it as wrapped BTC
  (tests/nodes/deployer/config.yml, cch.wrapped_btc_type_script_args)."
  "0x32e555f3ff8e135cece1351a6a2971518392c1e30375c1e006ad0ce8eac07947")

(def currency
  "The devnet's invoice currency."
  "Fibd")

(def ln-channel-sats
  "The capacity of the channel lnd-ingrid opens to lnd-bob."
  10000000)

(def ln-push-sats
  "The part of it pushed to lnd-bob at opening."
  5000000)

(def udt-channel-amount
  "Wrapped BTC (in sats: the UDT has 8 decimals) that n1 and n3 each put into the
  channel they open to the other. The acceptor adds its auto-accept amount."
  10000000)

;; Encodings

(defn ->long
  "LND's REST API writes 64-bit integers as strings."
  [x]
  (cond (nil? x)    nil
        (number? x) (long x)
        :else       (parse-long (str x))))

(defn b64->hex
  "Base64 bytes, as LND's REST API writes them, to 0x-prefixed hex as Fiber does."
  [^String s]
  (when s
    (str "0x" (apply str (map #(format "%02x" (bit-and % 0xff))
                              (.decode (Base64/getDecoder) s))))))

(defn hex->b64
  [^String hex]
  (let [hex (cond-> hex (str/starts-with? hex "0x") (subs 2))]
    (.encodeToString (Base64/getEncoder)
                     (byte-array (map #(unchecked-byte (Integer/parseInt (apply str %) 16))
                                      (partition 2 hex))))))

(defn strip-0x
  [^String hex]
  (cond-> hex (str/starts-with? hex "0x") (subs 2)))

(defn yaml
  "A YAML document for nested maps, with sequences of scalars and string or
  number scalars, in block style. Strings are double-quoted with JSON escapes,
  which YAML reads the same way."
  [m]
  (letfn [(scalar [x] (if (string? x) (json/generate-string x) (str x)))
          (lines [m indent]
            (let [pad (apply str (repeat indent " "))]
              (mapcat (fn [[k v]]
                        (cond (map? v)        (cons (str pad (name k) ":") (lines v (+ indent 2)))
                              (sequential? v) (cons (str pad (name k) ":")
                                                    (map #(str pad "  - " (scalar %)) v))
                              :else           [(str pad (name k) ": " (scalar v))]))
                      m)))]
    (str (str/join "\n" (lines m 0)) "\n")))

;; HTTP

(defn- http-json
  "Sends a request with an optional JSON body and returns the parsed JSON
  response. Throws ex-info with :type ::http-error for a status other than 2xx;
  connection errors propagate."
  [method url {:keys [body query timeout auth raw?] :or {timeout 10000}}]
  (let [resp (http/request (cond-> {:method             method
                                    :url                url
                                    :accept             :json
                                    :socket-timeout     timeout
                                    :connection-timeout 2000
                                    :throw-exceptions   false}
                             body  (assoc :body (json/generate-string body) :content-type :json)
                             query (assoc :query-params query)
                             auth  (assoc :basic-auth auth)))
        text (:body resp)]
    (if (<= 200 (:status resp) 299)
      (if raw? text (some-> text not-empty (json/parse-string true)))
      (let [parsed (try (json/parse-string text true) (catch Exception _ text))]
        (throw (ex-info (str (name method) " " url ": " (:status resp) " "
                             (or (when (map? parsed)
                                   (or (get-in parsed [:error :message]) (:message parsed)
                                       (:error parsed)))
                                 text))
                        {:type ::http-error, :status (:status resp), :body parsed}))))))

(defn btc-rpc
  "Calls bitcoind's JSON-RPC `method` with positional `params`."
  [method & params]
  (:result (http-json :post (str "http://" bitcoind-host ":18443/")
                      {:body    {:jsonrpc "1.0", :id "jepsen", :method method, :params (vec params)}
                       :auth    ["btc" "btc"]
                       :timeout 30000})))

(defn lnd-get
  [host path & [query opts]]
  (http-json :get (str "http://" host ":8080" path) (assoc opts :query query)))

(defn lnd-post
  [host path body & [opts]]
  (http-json :post (str "http://" host ":8080" path) (assoc opts :body body)))

(defn lnd-stream-last
  "The last message of one of LND's server-streaming REST responses, which are
  newline-separated JSON objects, each {:result ...} or {:error ...}."
  [text]
  (last (with-open [r (java.io.PushbackReader. (java.io.StringReader. (str text)))]
          (doall (json/parsed-seq r true)))))

;; SSH to the hosts

(defn on-host*
  "Calls (f) with an SSH session to `host` bound, using the test's SSH options."
  [test host f]
  (c/with-remote (or (:remote test) c/ssh)
    (c/with-ssh (:ssh test)
      (c/on host (f)))))

(defmacro on-host
  [test host & body]
  `(on-host* ~test ~host (fn [] ~@body)))

;; Daemons

(def bitcoind-dir "/btc/bitcoind")
(def bitcoind-pidfile "/btc/bitcoind.pid")
(def bitcoind-log "/btc/bitcoind.log")

(def lnd-dir "/btc/lnd")
(def lnd-pidfile "/btc/lnd.pid")
(def lnd-log "/btc/lnd.log")

(def fnn-binary "/usr/local/bin/fnn")
(def cch-dir "/fiber/cch")
(def cch-pidfile "/fiber/cch.pid")
(def cch-log "/fiber/cch.log")

(defn bitcoin-conf
  []
  (str/join "\n" ["server=1"
                  "regtest=1"
                  ""
                  "[regtest]"
                  "rpcbind=0.0.0.0"
                  "rpcport=18443"
                  "rpcallowip=127.0.0.1"
                  "rpcallowip=10.77.0.0/24"
                  "rpcuser=btc"
                  "rpcpassword=btc"
                  "zmqpubrawblock=tcp://0.0.0.0:28332"
                  "zmqpubrawtx=tcp://0.0.0.0:28333"
                  ; regtest has no fee estimates
                  "fallbackfee=0.0002"
                  ""]))

(defn lnd-conf
  "LND's config on `host`, as Fiber's tests/deploy/lnd-init has it, but listening
  on the container's network. REST is plain HTTP and needs no macaroon; gRPC,
  which CCH uses, is TLS with a certificate that names the host's address."
  [host]
  (let [btc (ips bitcoind-host)]
    (str/join "\n" ["[Application Options]"
                    "listen=0.0.0.0:9735"
                    "rpclisten=0.0.0.0:10009"
                    "restlisten=0.0.0.0:8080"
                    "no-macaroons=true"
                    "no-rest-tls=true"
                    "noseedbackup=true"
                    (str "alias=" host)
                    (str "tlsextraip=" (ips host))
                    (str "tlsextradomain=" host)
                    ""
                    "[Bitcoin]"
                    "bitcoin.active=1"
                    "bitcoin.regtest=1"
                    "bitcoin.node=bitcoind"
                    ""
                    "[Bitcoind]"
                    (str "bitcoind.rpchost=" btc ":18443")
                    "bitcoind.rpcuser=btc"
                    "bitcoind.rpcpass=btc"
                    (str "bitcoind.zmqpubrawblock=tcp://" btc ":28332")
                    (str "bitcoind.zmqpubrawtx=tcp://" btc ":28333")
                    ""])))

(defn cch-config
  "The standalone CCH's config: what Fiber's udt-init writes for its cch node
  (tests/deploy/udt-init, node index 4), with this cluster's addresses. `script`
  is the wrapped-BTC type script: in standalone mode CCH has no contracts
  context to build it from."
  [test script]
  {:rpc      {:listening_addr  (str (ips cch-host) ":" rpc/port)
              :enabled_modules ["cch"]}
   :cch      {:lnd_rpc_url                  (str "https://" (ips hub-lnd) ":10009")
              :lnd_cert_path                (str cch-dir "/lnd-tls.cert")
              :wrapped_btc_type_script_args wrapped-btc-args
              :wrapped_btc_type_script      (json/generate-string script)
              :fiber_rpc_url                (str "http://" (topology/node-ip (hub-node test))
                                                 ":" rpc/port)}
   :services ["rpc" "cch"]})

(defn- start-bitcoind!
  []
  (c/su (cu/start-daemon! {:logfile bitcoind-log, :pidfile bitcoind-pidfile, :chdir "/btc"}
                          "/usr/local/bin/bitcoind"
                          (str "-conf=" bitcoind-dir "/bitcoin.conf")
                          (str "-datadir=" bitcoind-dir)
                          "-printtoconsole")))

(defn- start-lnd!
  []
  (c/su (cu/start-daemon! {:logfile lnd-log, :pidfile lnd-pidfile, :chdir "/btc"}
                          "/usr/local/bin/lnd"
                          (str "--lnddir=" lnd-dir))))

(defn start-cch!
  "Starts CCH on the cch host (bound session). :already-running if it runs."
  [test]
  (c/su (cu/start-daemon! {:logfile cch-log
                           :pidfile cch-pidfile
                           :chdir   cch-dir
                           :env     {:RUST_LOG                  (str (:log-level test "info")
                                                                     ",fnn::cch=debug")
                                     :RUST_BACKTRACE            "1"
                                     :FIBER_SECRET_KEY_PASSWORD "password-cch"}}
                          fnn-binary
                          :-d cch-dir)))

(defn- pid
  [pidfile]
  (try+ (not-empty (str/trim (c/exec :cat pidfile)))
        (catch [:type :jepsen.control/nonzero-exit] _ nil)))

(defn kill-cch!
  "SIGKILLs CCH on the cch host (bound session) and waits for it to exit.
  Returns {:signal \"KILL\" :pid p :gone? b}, where :gone? says the pid no
  longer runs afterwards, or :not-running."
  []
  (c/su (let [p (pid cch-pidfile)]
          (cu/stop-daemon! fnn-binary cch-pidfile)
          (if p
            {:signal "KILL"
             :pid    p
             :gone?  (try+ (c/exec :ps :-o "pid=" :-p p) false
                           (catch [:type :jepsen.control/nonzero-exit] _ true))}
            :not-running))))

(defn cch-up?
  "Whether CCH answers RPC calls. An unknown order is an RPC error, which only
  a running CCH returns."
  []
  (try (rpc/call cch-host "get_cch_order" {:payment_hash (str "0x" (apply str (repeat 64 "0")))})
       true
       (catch clojure.lang.ExceptionInfo e (= :rpc-error (:type (ex-data e))))
       (catch Exception _ false)))

(defn await-cch!
  [timeout-ms]
  (await-fn #(or (cch-up?) (throw (ex-info "CCH not up" {})))
            {:log-message "Waiting for CCH RPC", :timeout timeout-ms, :retry-interval 500}))

(defn restart-cch!
  "Starts CCH and waits up to 60 s for its RPC, for the cch-kill fault."
  [test]
  (let [t0      (System/nanoTime)
        started (on-host test cch-host (start-cch! test))
        up?     (try (await-cch! 60000) true (catch Exception _ false))]
    {:started started
     :rpc-up? up?
     :ms      (long (/ (- (System/nanoTime) t0) 1e6))}))

(defn- await-lnd!
  "Waits for `host`'s LND to answer and be synced to the chain; its getinfo."
  [host]
  (await-fn #(let [i (lnd-get host "/v1/getinfo")]
               (if (:synced_to_chain i) i (throw (ex-info (str host " not synced") {}))))
            {:log-message (str "Waiting for " host " to sync"), :timeout 120000,
             :retry-interval 1000}))

(defn- await-ln-channel!
  "Waits until `host` lists an active channel to `peer-pubkey`."
  [host peer-pubkey]
  (await-fn #(or (some (fn [ch] (and (:active ch) (= peer-pubkey (:remote_pubkey ch))))
                       (:channels (lnd-get host "/v1/channels")))
                 (throw (ex-info "no active channel" {})))
            {:log-message (str "Waiting for an active channel on " host), :timeout 120000,
             :retry-interval 1000}))

(defn ln-channels
  "The LND channels of `host`, with the fields the swap checker uses."
  [host]
  (vec (for [ch (:channels (lnd-get host "/v1/channels"))]
         {:chan-id        (:chan_id ch)
          :channel-point  (:channel_point ch)
          :remote-pubkey  (:remote_pubkey ch)
          :active         (:active ch)
          :capacity       (->long (:capacity ch))
          :local-balance  (->long (:local_balance ch))
          :remote-balance (->long (:remote_balance ch))
          :commit-fee     (->long (:commit_fee ch))
          :pending-htlcs  (count (:pending_htlcs ch))})))

(defn wrapped-btc-script
  "The wrapped-BTC type script, from the hub node's UDT whitelist: Fiber's
  devnet names the simple UDT SIMPLE_UDT."
  [test]
  (let [udts (:udt_cfg_infos (rpc/call (hub-node test) "node_info"))
        udt  (first (filter #(= "SIMPLE_UDT" (:name %)) udts))]
    (when-not udt
      (throw (ex-info "The hub node has no SIMPLE_UDT in its UDT whitelist" {:udts udts})))
    {:code_hash (get-in udt [:script :code_hash])
     :hash_type (str/lower-case (name (get-in udt [:script :hash_type])))
     :args      wrapped-btc-args}))

(defn udt-channel?
  [ch]
  (= wrapped-btc-args (get-in ch [:funding_udt_type_script :args])))

(defn udt-channels
  "{node [channel ...]}: the wrapped-BTC channels of the CKB user and the hub,
  closed ones too."
  [test]
  (into {} (for [n [(ckb-user test) (hub-node test)]]
             [n (vec (filter udt-channel?
                             (:channels (rpc/call n "list_channels" {:include_closed true}))))])))

(defn- ready-udt-channels
  [node]
  (->> (:channels (rpc/call node "list_channels" {}))
       (filter udt-channel?)
       (filter #(= "channelready" (topology/state-name %)))
       count))

;; Setup

(defn- setup-bitcoind!
  "Starts bitcoind with an empty chain and a wallet holding 101 blocks' coinbase.
  Returns the wallet's mining address."
  [test]
  (on-host test bitcoind-host
    (c/su (cu/stop-daemon! bitcoind-pidfile)
          (c/exec :rm :-rf bitcoind-dir)
          (c/exec :mkdir :-p bitcoind-dir)
          (cu/write-file! (bitcoin-conf) (str bitcoind-dir "/bitcoin.conf")))
    (start-bitcoind!))
  (await-fn #(btc-rpc "getblockchaininfo")
            {:log-message "Waiting for bitcoind RPC", :timeout 60000, :retry-interval 500})
  (btc-rpc "createwallet" "jepsen")
  (let [addr (btc-rpc "getnewaddress")]
    (btc-rpc "generatetoaddress" 101 addr)
    addr))

(defn- setup-lnd!
  [test host]
  (on-host test host
    (c/su (cu/stop-daemon! lnd-pidfile)
          (c/exec :rm :-rf lnd-dir)
          (c/exec :mkdir :-p lnd-dir)
          (cu/write-file! (lnd-conf host) (str lnd-dir "/lnd.conf")))
    (start-lnd!)))

(defn- open-ln-channel!
  "Funds lnd-ingrid and opens its channel to lnd-bob, pushing ln-push-sats."
  [miner]
  (let [bob    (await-lnd! user-lnd)
        _      (await-lnd! hub-lnd)
        addr   (:address (lnd-get hub-lnd "/v1/newaddress" {:type "WITNESS_PUBKEY_HASH"}))]
    (info "Funding" hub-lnd "at" addr)
    (btc-rpc "sendtoaddress" addr 1)
    (btc-rpc "generatetoaddress" 1 miner)
    (await-fn #(or (pos? (->long (:confirmed_balance (lnd-get hub-lnd "/v1/balance/blockchain"))))
                   (throw (ex-info "no confirmed balance" {})))
              {:log-message (str "Waiting for " hub-lnd "'s funds"), :timeout 60000,
               :retry-interval 1000})
    (await-lnd! hub-lnd)
    (lnd-post hub-lnd "/v1/peers" {:addr {:pubkey (:identity_pubkey bob)
                                          :host   (str (ips user-lnd) ":9735")}
                                   :perm true})
    (await-fn #(or (some (fn [p] (= (:identity_pubkey bob) (:pub_key p)))
                         (:peers (lnd-get hub-lnd "/v1/peers")))
                   (throw (ex-info "not connected" {})))
              {:log-message (str "Waiting for " hub-lnd " to connect to " user-lnd),
               :timeout 60000, :retry-interval 1000})
    (info "Opening a" ln-channel-sats "sat channel" hub-lnd "->" user-lnd)
    (lnd-post hub-lnd "/v1/channels" {:node_pubkey          (hex->b64 (:identity_pubkey bob))
                                      :local_funding_amount (str ln-channel-sats)
                                      :push_sat             (str ln-push-sats)
                                      :sat_per_vbyte        "1"}
              {:timeout 60000})
    (btc-rpc "generatetoaddress" 6 miner)
    (let [ingrid (lnd-get hub-lnd "/v1/getinfo")]
      (await-ln-channel! hub-lnd (:identity_pubkey bob))
      (await-ln-channel! user-lnd (:identity_pubkey ingrid)))))

(defn- setup-cch!
  [test script]
  (let [cert   (on-host test hub-lnd (c/exec :cat (str lnd-dir "/tls.cert")))
        config (yaml (cch-config test script))]
    (on-host test cch-host
      (c/su (kill-cch!)
            (c/exec :rm :-rf cch-dir)
            (c/exec :mkdir :-p cch-dir)
            (cu/write-file! (str (str/trim cert) "\n") (str cch-dir "/lnd-tls.cert"))
            (cu/write-file! config (str cch-dir "/config.yml")))
      (start-cch! test))
    (await-cch! 120000)))

(defn- open-udt-channels!
  "Opens a wrapped-BTC channel from the CKB user to the hub node and one back,
  one after the other, and waits until both ends know them."
  [test script]
  (let [user  (ckb-user test)
        hub   (hub-node test)
        infos {user (rpc/call user "node_info"), hub (rpc/call hub "node_info")}]
    (rpc/call user "connect_peer" {:address (topology/p2p-address hub (infos hub))})
    (topology/await-condition! (str "Waiting for " user " to connect to " hub) 30000
                               #(some (fn [p] (= (:pubkey (infos hub)) (:pubkey p)))
                                      (:peers (rpc/call user "list_peers"))))
    (doseq [[i [a b]] (map-indexed vector [[user hub] [hub user]])]
      (info "Opening a" udt-channel-amount "wrapped-BTC channel" a "->" b)
      (rpc/call a "open_channel" {:pubkey                  (:pubkey (infos b))
                                  :funding_amount          (rpc/int->hex udt-channel-amount)
                                  :public                  true
                                  :funding_udt_type_script script})
      (doseq [n [a b]]
        (topology/await-condition! (str "Waiting for " (inc i) " ready wrapped-BTC channels on " n)
                                   180000 #(<= (inc i) (ready-udt-channels n)))))
    (let [expected (+ 2 (count (topology/edges test)))]
      (doseq [n [user hub]]
        (topology/await-condition! (str "Waiting for " n " to learn the wrapped-BTC channels") 180000
                                   #(<= expected (count (:channels (rpc/call n "graph_channels" {})))))))))

(defn- check-hosts!
  []
  (doseq [h hosts]
    (try (java.net.InetAddress/getByName h)
         (catch java.net.UnknownHostException _
           (throw (ex-info (str "--cch needs the hosts of docker/docker-compose.cch.yml, and " h
                                " is unknown: run the test with scripts/run.sh --compose cch")
                           {:type ::no-hosts, :host h}))))))

(defn setup-env!
  "Sets up the Lightning side, CCH and the wrapped-BTC channels, and records the
  starting balances in the test's :swap-env."
  [test]
  (check-hosts!)
  (let [miner  (setup-bitcoind! test)
        _      (doseq [h [hub-lnd user-lnd]] (setup-lnd! test h))
        _      (open-ln-channel! miner)
        script (wrapped-btc-script test)]
    (setup-cch! test script)
    (open-udt-channels! test script)
    (reset! (:swap-env test)
            {:script       script
             :user         (ckb-user test)
             :hub          (hub-node test)
             :udt-channels (udt-channels test)
             :ln-channels  {user-lnd (ln-channels user-lnd)
                            hub-lnd  (ln-channels hub-lnd)}})
    (info "Swap environment ready:" (pr-str (dissoc @(:swap-env test) :udt-channels)))))

(defn teardown-env!
  "Stops the daemons on every host. The hosts may be missing (a run without
  --compose cch fails in setup instead)."
  [test]
  (doseq [[host stop!] {bitcoind-host #(cu/stop-daemon! bitcoind-pidfile)
                        hub-lnd       #(cu/stop-daemon! lnd-pidfile)
                        user-lnd      #(cu/stop-daemon! lnd-pidfile)
                        cch-host      #(cu/stop-daemon! fnn-binary cch-pidfile)}]
    (try (on-host test host (c/su (stop!)))
         (catch Exception e (warn "Couldn't stop the daemon on" host (.getMessage e))))))

(def log-files
  {bitcoind-host [bitcoind-log]
   hub-lnd       [lnd-log]
   user-lnd      [lnd-log]
   cch-host      [cch-log (str cch-dir "/config.yml")]})

(defn download-logs!
  "Fetches the hosts' logs into the store, under each host's name."
  [test]
  (doseq [[host files] log-files]
    (try (on-host test host
           (doseq [f files :when (cu/exists? f)]
             (info "downloading" f "from" host)
             (c/download f (.getCanonicalPath
                             (store/path! test host (last (str/split f #"/")))))))
         (catch Exception e (warn "Couldn't download logs from" host (.getMessage e))))))

(defn- primary?
  [test node]
  (= node (first (:nodes test))))

(defrecord SwapDB [inner]
  db/DB
  (setup! [_ test node]
    (db/setup! inner test node))

  (teardown! [_ test node]
    (when (primary? test node) (teardown-env! test))
    (db/teardown! inner test node))

  db/Primary
  (primaries [_ test]
    (db/primaries inner test))

  (setup-primary! [_ test node]
    (db/setup-primary! inner test node)
    (setup-env! test))

  db/Kill
  (kill! [_ test node]
    (db/kill! inner test node))

  (start! [_ test node]
    (db/start! inner test node))

  db/Pause
  (pause! [_ test node]
    (db/pause! inner test node))

  (resume! [_ test node]
    (db/resume! inner test node))

  db/LogFiles
  (log-files [_ test node]
    ; Jepsen asks every node for its log files; on the first, this fetches the
    ; swap hosts' logs as well, which Jepsen can't reach.
    (when (primary? test node) (download-logs! test))
    (db/log-files inner test node)))

(defn db
  "Wraps the fnn DB `inner`: after its setup (the channel graph), sets up the
  swap environment, and tears it down and fetches its logs with it."
  [inner]
  (SwapDB. inner))
