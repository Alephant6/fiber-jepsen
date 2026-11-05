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
              when the fault is in use (jepsen.fiber.db).
  Adding a fault is a new namespace and a line in `registry`."
  (:require [jepsen.generator :as gen]
            [jepsen.nemesis.combined :as nc]
            [jepsen.fiber.schedule :as schedule]
            [jepsen.fiber.faults [chain-partition :as chain-partition]
                                 [clock :as clock]
                                 [disconnect :as disconnect]
                                 [packet :as packet]
                                 [partition :as partition]
                                 [process :as process]]))

(def registry
  "Every fault, in the order their generators and final generators combine."
  [partition/fault
   process/fault
   disconnect/fault
   packet/fault
   chain-partition/fault
   clock/fault])

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

(defn package
  "The nemesis package for the faults in use. Only those faults' packages are
  built: jepsen.nemesis.combined's full package also sets up file-corruption
  tooling (it downloads bitflip onto every node) even when that fault is off."
  [db opts]
  (let [popts (package-opts db opts)]
    (nc/compose-packages (mapv #((:package %) popts) (in-use opts)))))

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
