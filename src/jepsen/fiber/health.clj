(ns jepsen.fiber.health
  "Process health, assertion 7 of the chaos-test plan: no panic, and no node that
  is alive but does no work (the plan's 假活: the process is there but doesn't work).

  Two sources:
  - Each node's fnn.log, which Jepsen downloads into the store before the
    checkers run: every `panicked at` line (Rust prints one per panic, whether
    or not the process survives it), ERROR lines, and how many times fnn started.
  - A :health op, run by the nemesis after healing, the quiet period and the
    probe: per node, whether fnn's process is there and in what state, whether
    node_info answers (fnn's network actor handles it), whether new_invoice
    answers (it writes to fnn's store, so it fails when the store can't), and
    the container's out-of-memory kills (cgroup v2 memory.events).

  The checker is invalid when a node panicked, when a node isn't healthy in the
  final sample (exited, stopped, unresponsive, or not working: node_info answers
  but new_invoice doesn't), when the kernel killed a process for memory, or when
  a harness restart found fnn already gone although nothing had killed it (it
  exited on its own and a later :start brought it back).

  `sample!` is public so that a fault can record the same facts while it is in
  effect (a full disk, say), to tell a node that stopped from one that looks
  alive."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [warn]]
            [jepsen [checker :as checker]
                    [control :as c]
                    [nemesis :as n]
                    [store :as store]]
            [jepsen.fiber [db :as db]
                          [rpc :as rpc]]
            [slingshot.slingshot :refer [try+]]))

(def log-name
  "fnn's log, as Jepsen downloads it into each node's store directory."
  "fnn.log")

(def rpc-timeout-ms
  "How long a health sample waits for each RPC."
  5000)

;; Samples

(defn- exec-or-nil
  "Runs a command on the current node; its trimmed output, or nil when it exits
  non-zero."
  [& args]
  (try+ (str/trim (apply c/exec args))
        (catch [:type :jepsen.control/nonzero-exit] _ nil)))

(defn process
  "fnn's process on the current node: {:pid p, :state s} with s its ps state
  (\"Ssl\"; T = stopped, Z = exited but not reaped), or :state \"gone\" when the
  pidfile names no process, \"no-pidfile\" when there is none (fnn was stopped
  by the harness, or never started). :others counts live fnn processes the
  pidfile doesn't name."
  []
  (c/su
    (let [pid    (exec-or-nil :cat db/pidfile)
          state  (when pid (exec-or-nil :ps :-o "stat=" :-p pid))
          ; Other live fnn processes; zombies of killed ones don't count.
          others (->> (some-> (exec-or-nil :ps :-C "fnn" :-o "pid=,stat=") str/split-lines)
                      (map #(str/split (str/trim %) #"\s+"))
                      (remove (fn [[p st]] (or (= pid p) (nil? st) (str/starts-with? st "Z"))))
                      count)]
      (cond-> {:pid   pid
               :state (cond (nil? pid)          "no-pidfile"
                            (str/blank? state)  "gone"
                            :else               state)}
        (pos? others) (assoc :others others)))))

(defn parse-memory-events
  "cgroup v2 memory.events (\"oom 2\\noom_kill 1\\n...\") -> {:oom 2, :oom_kill 1, ...}."
  [s]
  (into {}
        (for [line  (str/split-lines (or s ""))
              :let  [[k v] (str/split (str/trim line) #"\s+")]
              :when (and k v (parse-long v))]
          [(keyword k) (parse-long v)])))

(defn- mib
  [s]
  (some-> s parse-long (quot (* 1024 1024))))

(defn memory
  "The node container's memory, from its cgroup (v2): :oom-kills (processes the
  kernel killed for memory since the container started), :current-mib and
  :peak-mib. Empty when the cgroup files can't be read (cgroup v1, say)."
  []
  (let [events (some-> (exec-or-nil :cat "/sys/fs/cgroup/memory.events") parse-memory-events)]
    (into {}
          (remove (comp nil? val))
          {:oom-kills   (when events (:oom_kill events 0))
           :current-mib (mib (exec-or-nil :cat "/sys/fs/cgroup/memory.current"))
           :peak-mib    (mib (exec-or-nil :cat "/sys/fs/cgroup/memory.peak"))})))

(defn- timed-rpc
  "{:ms m} when `method` answers within rpc-timeout-ms, {:ms m, :error e} when it
  doesn't."
  [node method params]
  (let [t0 (System/nanoTime)
        ms #(long (/ (- (System/nanoTime) t0) 1e6))]
    (try (rpc/call node method params {:timeout rpc-timeout-ms})
         {:ms (ms)}
         (catch clojure.lang.ExceptionInfo e
           {:ms (ms), :error (ex-message e)})
         (catch Exception e
           {:ms (ms), :error (.getSimpleName (class e))}))))

(defn sample!
  "The health of `node`, the current node of a jepsen.control session (call it
  from c/on-nodes): its process, its container's memory, whether node_info
  answers (:rpc) and whether new_invoice answers (:store-write). The invoice is
  never paid; it only shows that fnn can still write to its store."
  [_test node]
  {:process     (process)
   :memory      (memory)
   :rpc         (timed-rpc node "node_info" nil)
   :store-write (timed-rpc node "new_invoice"
                           {:amount      (rpc/int->hex 1000000)
                            :currency    "Fibd"
                            :description "fiber-jepsen health check"
                            :expiry      (rpc/int->hex 3600)})})

(defn status
  "One word for a node's health in a sample:
    :exited        no fnn process (or only a zombie);
    :stopped       fnn is stopped (SIGSTOP);
    :unresponsive  fnn is there, but node_info doesn't answer;
    :not-working   node_info answers, new_invoice doesn't;
    :ok            both answer."
  [sample]
  (let [state (get-in sample [:process :state] "")]
    (cond (nil? sample)                         :unknown
          (or (#{"gone" "no-pidfile"} state)
              (str/starts-with? state "Z"))     :exited
          (str/starts-with? state "T")          :stopped
          (get-in sample [:rpc :error])         :unresponsive
          (get-in sample [:store-write :error]) :not-working
          :else                                 :ok)))

(defn nemesis
  "Answers {:f :health} with a sample of every node."
  []
  (reify
    n/Reflection
    (fs [_] #{:health})

    n/Nemesis
    (setup! [this _test] this)

    (invoke! [_ test op]
      (assoc op :value (into (sorted-map) (c/on-nodes test sample!))))

    (teardown! [_ _test])))

(defn compose
  "The faults' nemesis and the health nemesis, as one. The faults' nemesis gets
  every op but :health, so it needn't support Reflection."
  [faults-nemesis]
  (n/compose {#{:health}                  (nemesis)
              #(when-not (= :health %) %) faults-nemesis}))

(def final-sample
  "The op that samples every node after healing."
  {:type :info, :f :health})

;; fnn's log

(def ^:private ansi
  #"\u001b\[[0-9;]*m")

(def ^:private timestamp
  #"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z")

(defn- subs*
  [s start end]
  (subs s start (min end (count s))))

(defn error-class
  "An error message with numbers, hashes and quoted values taken out, so that
  errors of one kind group together."
  [msg]
  (-> msg
      (str/replace #"0x[0-9a-fA-F]+" "0x..")
      (str/replace #"\b[0-9a-fA-F]{16,}\b" "..")
      (str/replace #"\b\d+\b" "N")
      (subs* 0 160)))

(defn parse-log
  "Facts from fnn's log, a seq of lines: every panic as {:thread, :at, :message,
  :after, :shutdown?} (:after is the timestamp of the last log line before it;
  :shutdown? says fnn had logged that it got SIGTERM or SIGINT and was shutting
  down), the number of ERROR lines and their most frequent classes, and how many
  times fnn started (its \"Starting node\" line)."
  [lines]
  (loop [[line & more] lines, last-ts nil, stopping? false, panics [], errors {}, starts 0]
    (if (nil? line)
      {:panics panics
       :errors (reduce + 0 (vals errors))
       :top-errors (->> errors (sort-by (comp - val)) (take 5) vec)
       :starts starts}
      (let [line   (str/replace line ansi "")
            ts     (or (re-find timestamp line) last-ts)
            start? (str/includes? line "Starting node with git version")
            stop?  (boolean (re-find #"SIG(TERM|INT) received, shutting down" line))]
        (if-let [[_ thread at] (re-find #"thread '([^']*)'(?: \(\d+\))? panicked at (.*)$" line)]
          ; Since Rust 1.73: "... panicked at src/x.rs:1:2:" and the message on the
          ; next line. Before: "... panicked at 'message', src/x.rs:1:2". Newer
          ; Rust puts the thread's id after its name: "thread 'w' (42) panicked".
          (let [new-style? (str/ends-with? at ":")
                panic      (if new-style?
                             {:thread thread, :at (subs at 0 (dec (count at)))
                              :message (some-> (first more) (str/replace ansi "") str/trim (subs* 0 300))}
                             {:thread thread, :at at})]
            (recur more ts stopping? (conj panics (assoc panic :after last-ts :shutdown? stopping?))
                   errors starts))
          (let [[_ msg] (re-find #"\sERROR\s+(?:\w+::)*\w+:\s*(.*)$" line)]
            (recur more ts (cond start? false stop? true :else stopping?) panics
                   (if msg (update errors (error-class msg) (fnil inc 0)) errors)
                   (cond-> starts start? inc))))))))

(defn read-log
  "parse-log of `node`'s fnn.log in the test's store, or nil without one."
  [test node]
  (let [f (store/path test (name node) log-name)]
    (when (.exists (io/file f))
      (with-open [r (io/reader f)]
        (parse-log (line-seq r))))))

;; The checker

(defn unexpected-restarts
  "Harness restarts that found fnn already gone although nothing had stopped it
  since it last started: each a :start op completion (its value maps node to
  :started or :already-running) naming the node :started, while no :kill op
  completion had named that node since. Means fnn exited on its own and the
  harness brought it back, which hides the exit from the final sample."
  [history]
  (loop [[op & more] (seq history), killed #{}, out []]
    (if (nil? op)
      out
      (let [v (:value op)]
        (if-not (map? v)
          (recur more killed out)
          (case (:f op)
            :kill  (recur more (into killed (keys v)) out)
            :start (let [started (for [[node r] v :when (= :started r)] node)
                         bad     (remove killed started)]
                     (recur more
                            (reduce disj killed started)
                            (into out (for [node bad] {:node node, :time (:time op)}))))
            (recur more killed out)))))))

(defn- final-health
  "The last :health op's value, {node sample}, or nil."
  [history]
  (->> history
       (filter #(and (= :health (:f %)) (map? (:value %))))
       last
       :value))

(defn check
  "The verdict from the logs ({node parse-log-result}, nil for a node without a
  log), the final sample ({node sample}, nil without one) and the history."
  [logs final history]
  (let [nodes    (sort (into (set (keys logs)) (keys final)))
        statuses (into (sorted-map) (for [n nodes] [n (status (get final n))]))
        panics   (for [[n l] logs, p (:panics l)] (assoc p :node n))
        ooms     (into (sorted-map)
                       (for [[n s] final
                             :let  [k (get-in s [:memory :oom-kills])]
                             :when (and k (pos? k))]
                         [n k]))
        restarts (unexpected-restarts history)
        unhealthy (into (sorted-map) (remove (comp #{:ok} val) statuses))]
    {:valid?              (cond (seq panics)    false
                                (seq ooms)      false
                                (seq restarts)  false
                                (nil? final)    :unknown
                                (seq unhealthy) false
                                (some nil? (vals logs)) :unknown
                                :else           true)
     :final-status        (if final statuses :no-sample)
     :unhealthy           (into (sorted-map)
                                (for [[n st] unhealthy]
                                  [n (assoc (select-keys (get final n) [:process :rpc :store-write])
                                            :status st)]))
     :panics              (count panics)
     ; Panics after fnn logged SIGTERM: its graceful shutdown (S1's --kill-signal
     ; term, or the TERM before Jepsen collects logs) has panicked before.
     :shutdown-panics     (count (filter :shutdown? panics))
     :panic-sites         (->> panics
                               (group-by (juxt :node :at :shutdown?))
                               (map (fn [[[n at down?] ps]]
                                      (cond-> {:node n, :at at, :count (count ps)
                                               :message (:message (first ps)), :first-after (:after (first ps))}
                                        down? (assoc :during-shutdown true))))
                               (sort-by (juxt :node :at))
                               vec)
     :oom-kills           ooms
     :unexpected-restarts (vec restarts)
     :fnn-starts          (into (sorted-map) (for [[n l] logs :when l] [n (:starts l)]))
     :error-lines         (into (sorted-map) (for [[n l] logs :when l] [n (:errors l)]))
     :top-errors          (into (sorted-map) (for [[n l] logs :when (seq (:top-errors l))]
                                               [n (:top-errors l)]))
     :peak-memory-mib     (into (sorted-map) (for [[n s] final
                                                   :let  [p (get-in s [:memory :peak-mib])]
                                                   :when p]
                                               [n p]))}))

(defn checker
  "Assertion 7: see the namespace doc."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [logs (into {} (for [node (:nodes test)]
                            [node (try (read-log test node)
                                       (catch Exception e
                                         (warn e "Couldn't read" log-name "of" node)
                                         nil))]))]
        (check logs (final-health history) history)))))
