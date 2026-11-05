(ns jepsen.fiber.verdict-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [jepsen.fiber.verdict :as verdict]))

(defn- run-dir
  "A temporary run directory with `results` as its results.edn."
  [results]
  (let [d (.toFile (java.nio.file.Files/createTempDirectory
                     "verdict" (make-array java.nio.file.attribute.FileAttribute 0)))]
    (when results (spit (io/file d "results.edn") results))
    (.getPath d)))

(deftest failed-checks
  (testing "only top-level checks whose :valid? isn't true, by name"
    (is (= {"fiber" false, "health" :unknown}
           (verdict/failed {:valid? false
                            :stats  {:valid? true}
                            :fiber  {:valid? false, :stuck [1 2]}
                            :health {:valid? :unknown}
                            :perf   {:valid? true}
                            :notes  "not a check"}))))
  (testing "one field"
    (is (= "none" (verdict/describe {})))
    (is (= "fiber,health=unknown" (verdict/describe (verdict/failed {:fiber  {:valid? false}
                                                                     :health {:valid? :unknown}}))))))

(deftest lines
  (testing "results.edn with records and other tagged values"
    (let [d (run-dir (str "{:valid? false,\n"
                          " :stats {:valid? true, :count 3/2},\n"
                          " :exceptions {:valid? true,\n"
                          "              :ops [#jepsen.history.Op{:index 1, :f :pay}]},\n"
                          " :timeline {:valid? true, :at #inst \"2026-11-05T00:00:00Z\"},\n"
                          " :fiber {:valid? false, :x #object[java.lang.Object 0x1 \"x\"]}}"))]
      (is (= (str d "\tfiber") (verdict/line d)))))
  (testing "a run without results.edn"
    (let [d (run-dir nil)]
      (is (= (str d "\t?") (verdict/line d))))))
