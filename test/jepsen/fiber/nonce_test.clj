(ns jepsen.fiber.nonce-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.fiber.nonce :as nonce]))

(defn line
  ([kind n ctx pid] (line kind n ctx pid "channel_v2.rs:407"))
  ([kind n ctx pid site]
   (str "NONCE-ORACLE " kind " nonce=" n " context=" ctx " site=" site
        " label=ch=00/n=1/commitment pid=" pid " boot=b" pid)))

(defn analyze
  [& lines]
  (nonce/analyze (keep nonce/parse-line lines)))

(deftest parses-records-and-skips-other-lines
  (is (nil? (nonce/parse-line "2026-10-09T19:00:00Z INFO fnn::fiber::network: hello")))
  (is (= {:kind "Reestablish", :channel "ab", :v "2", :site "s.rs:1", :pid "7", :boot "x"}
         (nonce/parse-line "NONCE-ORACLE Reestablish channel=ab v=2 site=s.rs:1 pid=7 boot=x"))))

(deftest a-clean-v2-history
  (let [a (analyze (line "Sent" "n1" "c1" 10)
                   (line "Sent" "n2" "c2" 10)
                   (line "Cached" "n1" "c1" 11)
                   "NONCE-ORACLE Reestablish channel=ab v=2 site=s.rs:1 pid=11 boot=b11"
                   "NONCE-ORACLE Replay channel=ab cs=true ack=false site=s.rs:2 pid=11 boot=b11")]
    (is (= 0 (:violation-count a)))
    (is (= 2 (:processes a)))
    (is (= 1 (:cached-after-restart a)))
    (is (= 0 (:cached-context-mismatch a)))
    (is (= {"v2" 1} (:reestablish a)))
    (is (= 1 (:replayed-cs a)))
    (is (= 0 (:replayed-ack a)))))

(deftest a-second-context-after-a-restart-is-a-violation
  (let [a (analyze (line "Sent" "n1" "c1" 10)
                   (line "Sent" "n1" "c2" 11 "planted:0"))
        v (first (:violations a))]
    (is (= 1 (:violation-count a)))
    (is (:across-restarts v))
    (is (= #{"c1" "c2"} (set (map :context (:sent v)))))))

(deftest the-same-context-signed-again-is-not-a-violation
  (let [a (analyze (line "Sent" "n1" "c1" 10)
                   (line "Sent" "n1" "c1" 11))]
    (is (= 0 (:violation-count a)))
    (is (= 1 (:resigned-after-restart a)))))

(deftest v1-aggregation-is-counted-but-not-a-violation
  (let [a (analyze (line "Sent" "n1" "c1" 10 "channel.rs:10142")
                   (line "Aggregated" "n1" "c2" 10 "channel.rs:10178")
                   (line "Aggregated" "n1" "c3" 10 "channel.rs:10178"))]
    (is (= 0 (:violation-count a)))
    (is (= 1 (:sent-once-aggregated-elsewhere a)))
    (is (= 1 (:aggregated-2plus a)))))

(deftest two-sent-and-an-aggregated-context-reveal-the-key
  (let [a (analyze (line "Sent" "n1" "c1" 10)
                   (line "Sent" "n1" "c2" 10)
                   (line "Aggregated" "n1" "c3" 10))]
    (is (= 1 (:violation-count a)))
    (is (= 1 (:revealable-count a)))))
