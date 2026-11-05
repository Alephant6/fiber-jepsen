(ns jepsen.fiber.faults
  "Every fault this suite can inject, and how a test combines the ones it uses.

  Each fault lives in its own namespace under jepsen.fiber.faults and defines
  `fault`, a map of
    :faults   the --faults names it handles;
    :stream   the name of its random stream (jepsen.fiber.schedule/stream);
    :package  (fn [opts]) -> a nemesis package in jepsen.nemesis.combined's shape,
              of which the test uses :nemesis, :final-generator and :perf;
    :ops      (fn [r opts]) -> its endless fault operations, drawn from the
              SplittableRandom `r` alone, so that the seed fixes them;
    :gaps     optional, (fn [streams opts]) -> the gaps in ns between its ops,
              for a fault that sets how long each fault lasts. Without it, gaps
              average :nemesis-interval seconds, from the fault's gap stream.
    :checker  optional, (fn [opts]) -> nil or {name checker}: checks of its own
              that the test adds to its checker when the fault is in use;
    :env      optional, (fn [opts]) -> environment variables fnn starts with
              when the fault is in use (jepsen.fiber.db);
    :fired    {--faults name, the set of op :f's that inject it, or a
              predicate on a completed op}: the ops the :faults check
              (`fired`) counts for that name;
    :effect   optional, (fn [op]) -> nil when a completed op of this fault
              took effect, :skipped when it couldn't (its node was down, say),
              or a string saying why it didn't. Without it, an op that didn't
              crash took effect;
    :strict?  optional, true when a single op without effect means the fault
              is broken (a local signal, say), not just unlucky.
  Adding a fault is a new namespace and a line in `registry`."
  (:require [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.nemesis.combined :as nc]
            [jepsen.fiber.schedule :as schedule]
            [jepsen.fiber.faults [cch-kill :as cch-kill]
                                 [chain-partition :as chain-partition]
                                 [clock :as clock]
                                 [disconnect :as disconnect]
                                 [disk :as disk]
                                 [lnd-disconnect :as lnd-disconnect]
                                 [outage :as outage]
                                 [packet :as packet]
                                 [partition :as partition]
                                 [power :as power]
                                 [process :as process]
                                 [resources :as resources]]))

(def registry
  "Every fault, in the order their generators and final generators combine."
  [partition/fault
   process/fault
   disconnect/fault
   packet/fault
   chain-partition/fault
   clock/fault
   outage/fault
   resources/fault
   disk/fault
   power/fault
   cch-kill/fault
   lnd-disconnect/fault])

(def supported
  "Every name --faults accepts."
  (into #{} (mapcat :faults) registry))

(defn in-use
  "The faults in the registry that the test options ask for."
  [opts]
  (let [faults (set (:faults opts))]
    (filter #(some faults (:faults %)) registry)))

(defn package-opts
  "The options jepsen.nemesis.combined's packages take."
  [db opts]
  {:db        db
   :nodes     (:nodes opts)
   :faults    (set (:faults opts))
   :interval  (:nemesis-interval opts)
   :kill      {:targets (:kill-targets opts)}
   :pause     {:targets [:one]}
   :partition {:targets [:one]}})

(def ^:private heal-last
  "Faults whose final step brings nodes back (restarts and resumes fnn, then
  reconnects the peers), in that order. Their final steps run after every other
  fault has healed, so that fnn doesn't restart under a CPU or memory limit,
  cut off the CKB node, or with its clock off, when several faults run
  together (C-16)."
  [process/fault disconnect/fault])

(defn final-order
  "The faults in use, in the order their final generators run: as in the
  registry, but with heal-last's at the end."
  [faults]
  (let [last? (set heal-last)]
    (concat (remove last? faults) (filter (set faults) heal-last))))

(defn package
  "The nemesis package for the faults in use. Only those faults' packages are
  built: jepsen.nemesis.combined's full package also sets up file-corruption
  tooling (it downloads bitflip onto every node) even when that fault is off.
  The final generators run in final-order."
  [db opts]
  (let [popts  (package-opts db opts)
        faults (in-use opts)
        pkgs   (into {} (map (fn [f] [f ((:package f) popts)])) faults)
        pkg    (nc/compose-packages (mapv pkgs faults))]
    (if (< 1 (count faults))
      (assoc pkg :final-generator (vec (keep (comp :final-generator pkgs) (final-order faults))))
      pkg)))

(defn fault-completions
  "The completions of the faults' ops in `history`. A nemesis op is :info both
  when invoked and when it completes, and the one nemesis thread runs its ops
  one at a time, so its ops alternate: invocation, completion. The health
  sample (jepsen.fiber.health) shares the nemesis, but isn't a fault."
  [history]
  (->> history
       (filter #(= :nemesis (:process %)))
       rest
       (take-nth 2)
       (remove #(= :health (:f %)))))

(defn- why-not
  "Why the completed `op` of `fault` didn't take effect: \"crashed\" when it
  threw, else what the fault's :effect says (nil: it did)."
  [fault op]
  (if (:exception op)
    "crashed"
    (when-let [effect (:effect fault)]
      (effect op))))

(defn fired
  "The :faults check on `history`, for the faults in use: did each fault the
  test asked for take effect?

  Jepsen records a fault op that throws as :info with an :exception, and no
  stock checker fails a run for it, so a fault that never worked (a typo in a
  target, a signal that stopped nothing) would leave a valid run that tested
  nothing. Invalid when a fault op crashed, when an op of a :strict? fault
  didn't take effect, or when a fault was tried and never took effect;
  :unknown when a fault in use was never tried (a run shorter than its first
  gap) or every op of it was skipped."
  [opts history]
  (let [ops     (vec (fault-completions history))
        crashed (vec (for [op ops :when (:exception op)]
                       {:f (:f op), :time (:time op), :error (:error op)}))
        names   (set (:faults opts))
        faults  (for [fault (in-use opts)
                      [nm fs] (:fired fault)
                      :when (names nm)]
                  (let [mine    (filter (if (set? fs) (comp fs :f) fs) ops)
                        whys    (map #(why-not fault %) mine)
                        failed  (vec (for [[op why] (map vector mine whys)
                                           :when (string? why)]
                                       {:f (:f op), :time (:time op), :why why}))]
                    {:fault       nm
                     :strict?     (boolean (:strict? fault))
                     :ops         (count mine)
                     :took-effect (count (filter nil? whys))
                     :skipped     (count (filter #{:skipped} whys))
                     :no-effect   failed}))
        tried   #(- (:ops %) (:skipped %))
        broken  (filter #(or (and (:strict? %) (seq (:no-effect %)))
                             (and (pos? (tried %)) (zero? (:took-effect %))))
                        faults)
        untried (filter #(zero? (tried %)) faults)]
    {:valid?  (cond (seq crashed) false
                    (seq broken)  false
                    (seq untried) :unknown
                    :else         true)
     :faults  (into (sorted-map)
                    (for [f faults]
                      [(:fault f) (cond-> (select-keys f [:ops :took-effect])
                                    (pos? (:skipped f))   (assoc :skipped (:skipped f))
                                    (seq (:no-effect f))  (assoc :no-effect-count (count (:no-effect f))
                                                                 :no-effect (vec (take 5 (:no-effect f)))))]))
     :crashed (vec (take 5 crashed))
     :crashed-count (count crashed)}))

(defn fired-checker
  "See `fired`."
  [opts]
  (reify checker/Checker
    (check [_ _test history _opts]
      (fired opts history))))

(defn checkers
  "{name checker} of the checks the faults in use add (their :checker)."
  [opts]
  (into {} (for [{:keys [checker]} (in-use opts)
                 :when checker]
             (checker opts))))

(defn fnn-env
  "The environment variables the faults in use start fnn with (their :env)."
  [opts]
  (into {} (for [{:keys [env]} (in-use opts)
                 :when env]
             (env opts))))

(defn generator
  "The faults in use, each a stream of operations drawn from the seed and paced
  by gaps averaging :nemesis-interval seconds (or the fault's own :gaps),
  combined as nc/compose-packages combines generators. Nil without faults."
  [streams opts]
  (let [gens (for [{:keys [stream ops gaps]} (in-use opts)]
               (schedule/paced (if gaps
                                 (gaps streams opts)
                                 (schedule/gaps (schedule/gaps-stream streams stream)
                                                (:nemesis-interval opts)))
                               (ops (schedule/stream streams stream) opts)))]
    (when (seq gens)
      (apply gen/any gens))))
