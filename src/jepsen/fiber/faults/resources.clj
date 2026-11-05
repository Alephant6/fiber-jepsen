(ns jepsen.fiber.faults.resources
  "The cpu and memory faults (C-12 of the chaos-test plan): limit a node
  container's CPU or memory, as `docker update` does, then lift the limit.

  Docker sets the limits on the container's cgroup, so they cover fnn and sshd
  together. The control container reaches Docker's Engine API through the
  socket docker/docker-compose.yml mounts at /var/run/docker.sock, with curl.
  That socket gives the control container control of the host's Docker daemon,
  which is root on the host: fine on a throwaway CI runner, a trade-off on a
  workstation.

  A :limit-resources op names its nodes and one limit, {:nodes [n] :cpus 0.25}
  (a CFS quota of a quarter of a CPU) or {:nodes [n] :memory-mib 192} (memory
  and memory+swap both, so no swap); a :lift-resources op lifts every limit.
  Their completions record, per node, the limits the kernel then has and, on
  lifting, the window's report.
  Docker's update API can't remove a limit (zero means unchanged), so lifting
  sets it to the host's size: all its CPUs, all its memory and unlimited swap.

  Each op records what the node's cgroup says (cpu.max, memory.max), so a run
  shows the limit took effect. Before lifting, it samples the node's health
  (jepsen.fiber.health/sample!), so that a node that stopped working under the
  limit shows, and reports the CPU throttling and memory events of the window."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [control :as c]
                    [nemesis :as n]
                    [store :as store]]
            [jepsen.fiber [health :as health]
                          [schedule :as schedule]
                          [topology :as topology]]
            [slingshot.slingshot :refer [try+]])
  (:import (java.net URLEncoder)
           (java.time Duration Instant)
           (java.util SplittableRandom)))

(def socket
  "Docker's socket, as docker/docker-compose.yml mounts it into the control
  container."
  "/var/run/docker.sock")

(def target-specs
  #{:one :all})

(defn parse-range
  "\"0.1-0.5\" -> [0.1 0.5]; \"0.25\" -> [0.25 0.25]."
  [s]
  (let [[lo hi] (map parse-double (str/split s #"-" 2))]
    [lo (or hi lo)]))

;; Docker's Engine API

(defn docker-api
  "Calls Docker's Engine API over its socket with curl. Returns the parsed JSON
  body, keywordized, or nil when it is empty; throws ex-info when curl fails or
  the status isn't 2xx."
  ([method path]
   (docker-api method path nil))
  ([method path body]
   (let [args (cond-> ["curl" "-sS" "--max-time" "30" "--unix-socket" socket
                       "-X" method "-w" "\n%{http_code}"]
                body (into ["-H" "Content-Type: application/json"
                            "-d" (json/generate-string body)])
                true (conj (str "http://docker" path)))
         {:keys [exit out err]} (apply sh/sh args)]
     (when-not (zero? exit)
       (throw (ex-info (str "Docker API " method " " path ": curl exited " exit ": " (str/trim err))
                       {:type :docker-api, :exit exit})))
     (let [i      (str/last-index-of out "\n")
           status (parse-long (str/trim (subs out (inc i))))
           text   (subs out 0 i)]
       (if (and status (<= 200 status 299))
         (when-not (str/blank? text) (json/parse-string text true))
         (throw (ex-info (str "Docker API " method " " path ": " status " " (str/trim text))
                         {:type :docker-api, :status status, :body text})))))))

(defn host-size
  "The Docker host's CPUs and memory in bytes, which a lifted limit is set to."
  []
  (let [i (docker-api "GET" "/info")]
    {:cpus (:NCPU i), :memory (:MemTotal i)}))

(defn container-id
  "The id of node `node`'s container: the compose service of that name whose
  address on the test network is the node's (several compose projects may run
  on one host, but not on one subnet)."
  [node]
  (let [filters (json/generate-string {:label [(str "com.docker.compose.service=" node)]})
        cs      (docker-api "GET" (str "/containers/json?filters=" (URLEncoder/encode filters "UTF-8")))
        ip      (topology/node-ip node)
        mine    (filter (fn [c] (some #(= ip (:IPAddress %))
                                      (vals (get-in c [:NetworkSettings :Networks]))))
                        cs)]
    (or (:Id (first mine))
        (throw (ex-info (str "No container for " node " at " ip " among " (count cs)
                             " containers of service " node)
                        {:type :no-container, :node node})))))

(defn limit-body
  "The body of Docker's container update call for one limit, or for lifting it
  to the host's size (memory: the host's, plus as much swap)."
  [kind amount host]
  (case kind
    :cpu    {:NanoCpus (long (* 1e9 (if (= :lift amount) (:cpus host) amount)))}
    :memory (if (= :lift amount)
              ; Docker passes MemorySwap on only when positive, so -1 would keep the
              ; limit's swap (equal to its memory) and runc refuses memory above it.
              {:Memory (:memory host), :MemorySwap (* 2 (:memory host))}
              (let [bytes (* amount 1024 1024)]
                {:Memory bytes, :MemorySwap bytes}))))

;; The node's cgroup, read over SSH

(defn- cat-or-nil
  [path]
  (try+ (str/trim (c/exec :cat path))
        (catch [:type :jepsen.control/nonzero-exit] _ nil)))

(defn parse-flat-keyed
  "A cgroup v2 flat-keyed file (\"usage_usec 12\\nnr_throttled 3\") -> {:usage_usec 12, ...}."
  [s]
  (health/parse-memory-events s))

(defn cgroup
  "What the current node's cgroup (v2) says: its limits as the kernel has them,
  and the counters a window's report is computed from."
  []
  (let [cpu (parse-flat-keyed (cat-or-nil "/sys/fs/cgroup/cpu.stat"))
        mem (parse-flat-keyed (cat-or-nil "/sys/fs/cgroup/memory.events"))
        st  (parse-flat-keyed (cat-or-nil "/sys/fs/cgroup/memory.stat"))]
    {:cpu.max         (cat-or-nil "/sys/fs/cgroup/cpu.max")
     :memory.max      (cat-or-nil "/sys/fs/cgroup/memory.max")
     :memory.current  (some-> (cat-or-nil "/sys/fs/cgroup/memory.current") parse-long)
     ; Of memory.current: anonymous memory (the processes' heaps) and page cache,
     ; which the kernel reclaims under a limit.
     :anon            (:anon st)
     :file            (:file st)
     :throttled-usec  (:throttled_usec cpu)
     :nr-throttled    (:nr_throttled cpu)
     :memory-max-hits (:max mem)
     :oom             (:oom mem)
     :oom-kills       (:oom_kill mem)}))

(defn- delta
  [after before k]
  (when (and (get after k) (get before k))
    (- (get after k) (get before k))))

(defn window-report
  "What happened in a node's cgroup between `before` (when its limit was set) and
  `after` (just before it was lifted)."
  [before after]
  (let [mib #(some-> % (quot (* 1024 1024)))]
    (into {}
          (remove (comp nil? val))
          {:throttled-ms     (some-> (delta after before :throttled-usec) (quot 1000))
           :throttled-slices (delta after before :nr-throttled)
           :memory-max-hits  (delta after before :memory-max-hits)
           :ooms             (delta after before :oom)
           :oom-kills        (delta after before :oom-kills)
           :memory-mib       (mib (:memory.current after))
           :anon-mib         (mib (:anon after))
           :file-mib         (mib (:file after))})))

;; The nemesis

(defn- kind-and-amount
  [limit]
  (cond (:cpus limit)       [:cpu (:cpus limit)]
        (:memory-mib limit) [:memory (:memory-mib limit)]))

(defn resources-nemesis
  "Sets and lifts limits. Remembers which nodes it limited, with what, and their
  cgroup counters at the time."
  []
  (let [limited (atom {})   ; node -> {:kind k, :before cgroup, :wall instant}
        ids     (atom {})
        host    (delay (host-size))
        id      (fn [node] (or (@ids node) ((swap! ids assoc node (container-id node)) node)))
        update! (fn [node kind amount]
                  (docker-api "POST" (str "/containers/" (id node) "/update")
                              (limit-body kind amount @host)))]
    (reify
      n/Reflection
      (fs [_] #{:limit-resources :lift-resources})

      n/Nemesis
      (setup! [this _test]
        (when-not (.exists (io/file socket))
          (throw (ex-info (str "--faults cpu and memory need Docker's socket at " socket
                               " in the control container (docker/docker-compose.yml mounts it)")
                          {:type :no-docker-socket})))
        (info "Docker host for resource limits:" @host)
        this)

      (invoke! [_ test op]
        (case (:f op)
          :limit-resources
          (let [[kind amount] (kind-and-amount (:value op))
                nodes         (:nodes (:value op))]
            (assoc op :value
                   {:limit (:value op)
                    :nodes (into (sorted-map)
                                 (c/on-nodes test nodes
                                             (fn [_test node]
                                               (let [before (cgroup)
                                                     wall   (str (Instant/now))]
                                                 (update! node kind amount)
                                                 (swap! limited assoc node {:kind kind, :before before, :wall wall})
                                                 ; What the kernel now has, to show the limit took.
                                                 (select-keys (cgroup) [:cpu.max :memory.max])))))}))

          :lift-resources
          (let [nodes (vec (keys @limited))]
            (assoc op :value
                   (into (sorted-map)
                         (when (seq nodes)
                           (c/on-nodes test nodes
                                       (fn [test node]
                                         (let [{:keys [kind before wall]} (@limited node)
                                               sample (health/sample! test node)
                                               after  (cgroup)]
                                           (update! node kind :lift)
                                           (swap! limited dissoc node)
                                           (let [now (cgroup)]
                                             {:lifted          kind
                                              :since           wall
                                              :wall            (str (Instant/now))
                                              :under-limit     (assoc (window-report before after)
                                                                      :status (health/status sample)
                                                                      :rpc-ms (get-in sample [:rpc :ms])
                                                                      :store-write-ms (get-in sample [:store-write :ms]))
                                              :now             (select-keys now [:cpu.max :memory.max])}))))))))))

      (teardown! [_ _test]))))

(defn package
  [opts]
  (let [needed? (some (:faults opts) [:cpu :memory])
        stop    {:type :info, :f :lift-resources, :value nil}]
    {:generator       nil
     :final-generator (when needed? stop)
     :nemesis         (resources-nemesis)
     :perf            #{{:name  "resources"
                         :start #{:limit-resources}
                         :stop  #{:lift-resources}
                         :color "#E9C4A0"}}}))

(defn ops
  "Endless pairs of a :limit-resources, its kind (among the --faults cpu and
  memory in use), targets and amount drawn from `r`, and a :lift-resources.
  Options: :resource-targets (specs to choose from), :cpu-limit and
  :memory-limit ([min max], drawn uniformly; CPUs to a hundredth, MiB)."
  [^SplittableRandom r opts]
  (let [kinds     (vec (filter (set (:faults opts)) [:cpu :memory]))
        nodes     (vec (:nodes opts))
        specs     (vec (:resource-targets opts [:one]))
        [clo chi] (:cpu-limit opts [0.1 0.5])
        [mlo mhi] (:memory-limit opts [128 512])]
    (mapcat (fn [limit]
              [{:type :info, :f :limit-resources, :value limit}
               {:type :info, :f :lift-resources, :value nil}])
            (repeatedly
              (fn []
                (let [kind    (schedule/pick r kinds)
                      targets (case (schedule/pick r specs)
                                :one [(schedule/pick r nodes)]
                                :all nodes)]
                  (case kind
                    :cpu    {:nodes targets
                             :cpus  (/ (Math/round (* 100 (+ clo (* (.nextDouble r) (- chi clo))))) 100.0)}
                    :memory {:nodes      targets
                             :memory-mib (+ (long mlo) (.nextInt r (int (inc (- (long mhi) (long mlo))))))})))))))

;; Disconnect loops, from fnn's debug log

(def ^:private ansi
  #"\u001b\[[0-9;]*m")

(defn disconnects
  "Every `Peer <pubkey> disconnected` line in a log (a seq of lines) as
  {:at instant, :peer pubkey}. fnn logs these at debug level, for target
  fnn::fiber::network. Nil when the log has no debug lines from that target at
  all, so that a run without them can't pass silently."
  [lines]
  (let [seen-debug? (volatile! false)
        out         (vec (for [line  lines
                               :let  [line (str/replace line ansi "")]
                               :when (and (str/includes? line "fnn::fiber::network")
                                          (str/includes? line "DEBUG")
                                          (vreset! seen-debug? true))
                               :let  [[_ ts]   (re-find #"^\s*(\S+Z)\s" line)
                                      [_ peer] (re-find #"Peer (\S+) disconnected" line)]
                               :when (and ts peer)]
                           {:at (Instant/parse ts), :peer peer}))]
    (when @seen-debug? out)))

(def loop-threshold
  "A pair of nodes that disconnects this many times within one window is in a
  disconnect loop."
  3)

(def recovery-s
  "How long after a limit is lifted its window still counts."
  60)

(defn windows
  "Each limit as {:node n, :from instant, :to instant}: from when it was set to
  `recovery-s` after it was lifted, from the :lift-resources completions."
  [history]
  (for [op            history
        :when         (and (= :lift-resources (:f op)) (map? (:value op)))
        [node lifted] (:value op)
        :when         (and (map? lifted) (:since lifted) (:wall lifted))]
    {:node node
     :from (Instant/parse (:since lifted))
     :to   (.plus (Instant/parse (:wall lifted)) (Duration/ofSeconds recovery-s))}))

(defn loop-check
  "Disconnects per window, on the limited node and on every other node, by
  peer: {:valid? .. :windows [...] :loops [...]}. Invalid when a node saw one
  peer disconnect loop-threshold times or more within a window. :unknown
  without debug logs for fnn::fiber::network."
  [windows logs]
  (if (some nil? (vals logs))
    {:valid? :unknown
     :error  (str "No fnn::fiber::network debug lines in some node's log; run with"
                  " --log-level 'info,fnn::fiber::network=debug' to check for disconnect loops")}
    (let [counted (vec (for [{:keys [node from to]} windows]
                         {:node        node
                          :from        (str from)
                          :disconnects (into (sorted-map)
                                             (for [[n ds] logs
                                                   :let  [by-peer (frequencies
                                                                    (for [d ds
                                                                          :when (and (not (.isBefore ^Instant (:at d) from))
                                                                                     (.isBefore ^Instant (:at d) to))]
                                                                      (:peer d)))]
                                                   :when (seq by-peer)]
                                               [n by-peer]))}))
          loops   (vec (for [w       counted
                             [n bp]  (:disconnects w)
                             [p cnt] bp
                             :when   (<= loop-threshold cnt)]
                         {:window-node (:node w), :from (:from w), :node n, :peer p, :disconnects cnt}))]
      {:valid?   (empty? loops)
       :windows  counted
       :loops    loops})))

(defn checker
  "With --faults cpu or memory: no disconnect loop while a limit is on or in the
  minute after it is lifted. OOM kills and nodes that end unhealthy fail the
  health checker (jepsen.fiber.health), and recovery is the probe's."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [logs (into {} (for [node (:nodes test)]
                            [node (let [f (store/path test (name node) health/log-name)]
                                    (try (when (.exists (io/file f))
                                           (with-open [r (io/reader f)]
                                             (disconnects (line-seq r))))
                                         (catch Exception e
                                           (warn e "Couldn't read" f)
                                           nil)))]))]
        (loop-check (windows history) logs)))))

(defn limits-check
  "Every limit took and was lifted. Invalid when a :limit-resources or
  :lift-resources op threw (a node may have stayed limited for the rest of the
  run), or when fewer limits were lifted than set."
  [history]
  (let [crashed (vec (for [op    history
                           :when (and (#{:limit-resources :lift-resources} (:f op)) (:exception op))]
                       (select-keys op [:f :time :error])))
        set     (reduce + (for [op    history
                                :when (and (= :limit-resources (:f op)) (map? (get-in op [:value :nodes])))]
                            (count (get-in op [:value :nodes]))))
        lifted  (count (windows history))]
    {:valid?         (and (empty? crashed) (= set lifted))
     :limits         set
     :lifted         lifted
     :nemesis-errors crashed}))

(defn limits-checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (limits-check history))))

(def fault
  {:faults  #{:cpu :memory}
   :stream  :resources
   :package package
   :ops     ops
   :checker (fn [_opts] {:disconnect-loops (checker)
                         :resource-limits  (limits-checker)})
   ; A completed limit's value holds the limit under :limit; a crashed one's is the limit.
   :fired   {:cpu    (fn [op] (and (= :limit-resources (:f op))
                                   (some? (or (get-in op [:value :limit :cpus]) (get-in op [:value :cpus])))))
             :memory (fn [op] (and (= :limit-resources (:f op))
                                   (some? (or (get-in op [:value :limit :memory-mib])
                                              (get-in op [:value :memory-mib])))))}})
