(ns jepsen.fiber.verdict
  "Which checks failed in stored runs, for CI summaries:

    lein run -m jepsen.fiber.verdict STORE-DIR...

  prints one line per run directory: the directory, a tab, and the checks in its
  results.edn whose :valid? isn't true, comma-separated (name for false,
  name=unknown and so on otherwise), or `none`. A directory whose results.edn is
  missing or can't be read gets `?`."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(defn failed
  "The top-level checks of results map `r` that didn't pass, as {name valid?},
  sorted by name."
  [r]
  (into (sorted-map)
        (for [[k v] r
              :when (and (map? v) (contains? v :valid?) (not (true? (:valid? v))))]
          [(name k) (:valid? v)])))

(defn read-results
  "The results map in run directory `dir`. Records and other tagged values read
  as tagged literals."
  [dir]
  (with-open [r (java.io.PushbackReader. (io/reader (io/file dir "results.edn")))]
    (edn/read {:default tagged-literal} r)))

(defn describe
  "`failed`'s map as one field: name for false, name=value otherwise; none when
  empty."
  [f]
  (if (empty? f)
    "none"
    (str/join "," (for [[k v] f]
                    (if (false? v) k (str k "=" (if (keyword? v) (name v) (pr-str v))))))))

(defn line
  [dir]
  (str dir "\t" (try (describe (failed (read-results dir)))
                     (catch Exception _ "?"))))

(defn -main
  [& dirs]
  (doseq [d dirs] (println (line d)))
  (flush)
  (shutdown-agents))
