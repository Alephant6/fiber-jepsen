(ns jepsen.fiber.disk-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [db :as db]
                          [faults :as faults]
                          [schedule :as schedule]]
            [jepsen.fiber.faults.disk :as disk]))

(def opts
  {:nodes ["n1" "n2" "n3"], :faults [:disk], :rate 5, :time-limit 60, :quiesce 5
   :nemesis-interval 10, :seed 7})

(deftest ops
  (let [ops (take 40 (disk/ops (schedule/stream (schedule/streams 7) :disk) opts))]
    (testing "a fill of one node, then a free, in turn"
      (is (= (take 40 (cycle [:fill-disk :free-disk])) (map :f ops)))
      (is (= #{true false} (set (map #(get-in % [:value :restart]) (filter #(= :fill-disk (:f %)) ops))))
          "some fills restart fnn on the full disk")
      (is (every? #(= 1 (count (get-in % [:value :nodes]))) (filter #(= :fill-disk (:f %)) ops)))
      (is (= #{"n1" "n2" "n3"} (set (mapcat #(get-in % [:value :nodes]) ops)))))
    (testing "the seed fixes them"
      (is (= ops (take 40 (disk/ops (schedule/stream (schedule/streams 7) :disk) opts)))))))

(deftest small-store
  (is (db/small-store? {:faults #{:disk}}))
  (is (not (db/small-store? {:faults #{:kill}})))
  (is (= "/fiber/small/fill" disk/fill-file)))

(defn- rec
  [while-full after-free & [after-restart]]
  (cond-> {:while-full while-full, :after-free after-free}
    after-restart (assoc :restarted :ok, :after-restart after-restart)))

(def ok {:status :ok, :state "Ssl"})
(def exited {:status :exited, :state "gone"})

(deftest while-full
  (is (= :stopped (disk/while-full (rec exited ok))))
  (is (= :errored (disk/while-full (rec {:status :not-working, :store-write-error "new_invoice: IO error: No space left on device"} ok))))
  (is (= :hung (disk/while-full (rec {:status :not-working, :store-write-error "SocketTimeoutException"} ok))))
  (is (= :unresponsive (disk/while-full (rec {:status :unresponsive, :rpc-error "SocketTimeoutException"} ok))))
  (is (= :unaffected (disk/while-full (rec ok ok))))
  (is (= :unknown (disk/while-full {}))))

(deftest after-free
  (is (= :by-itself (disk/after-free (rec {:status :not-working} ok))))
  (testing "it had stopped, so the restart is what an operator would do"
    (is (= :after-restart (disk/after-free (rec exited exited ok))))
    (is (= :after-restart (disk/after-free (rec {:status :not-working} exited ok)))))
  (testing "running, but not working, after the space came back"
    (is (= :not-working (disk/after-free (rec {:status :not-working} {:status :not-working} ok)))))
  (is (= :not-recovered (disk/after-free (rec exited exited exited)))))

(defn- op
  [f value t]
  {:type :info, :process :nemesis, :f f, :value value, :time t})

(defn- pay
  [type from to t]
  {:type type, :process 0, :f :pay, :value {:from from, :to to}, :time t})

(deftest check
  (testing "a node that stopped and came back after a restart, and one that errored and came back by itself"
    (let [h [(op :fill-disk {"n2" {:avail-before 1, :avail-after 0, :restart :exited
                                   :store {:apparent 3000000, :allocated 70000000}}} 10)
             (pay :invoke "n1" "n2" 11) (pay :fail "n1" "n2" 12) (pay :ok "n1" "n3" 13)
             (pay :info "n2" "n3" 14) (pay :ok "n1" "n2" 30)
             (op :free-disk {"n2" (assoc (rec exited exited ok) :space-errors 3, :panics 1)} 20)
             (op :fill-disk {"n3" {:avail-before 1, :avail-after 0}} 40)
             (op :free-disk {"n3" (rec {:status :not-working, :store-write-error "new_invoice: IO error"} ok)} 50)]
          r (disk/check h)]
      (is (true? (:valid? r)))
      (is (= {:stopped 1, :errored 1} (:while-full r)))
      (is (= {:after-restart 1, :by-itself 1} (:after-free r)))
      (is (= {:fail 1, :info 1} (:payments (first (:windows r))))
          "the payments from or to n2 between its fill and its free")
      (is (= 3 (:space-errors (first (:windows r)))))
      (is (= 1 (:panics (first (:windows r)))))
      (testing "with what the fill recorded"
        (is (= :exited (:restarted-full (first (:windows r)))))
        (is (= 70000000 (:store-allocated-bytes (first (:windows r)))))
        (is (not (contains? (second (:windows r)) :restarted-full))))))
  (testing "alive but doing nothing while full"
    (let [r (disk/check [(op :fill-disk {"n1" {}} 1)
                         (op :free-disk {"n1" (rec {:status :not-working, :store-write-error "SocketTimeoutException"} ok)} 2)])]
      (is (false? (:valid? r)))
      (is (= [:hung] (map :while-full (:failures r))))))
  (testing "still not working once the space came back"
    (is (false? (:valid? (disk/check [(op :fill-disk {"n1" {}} 1)
                                      (op :free-disk {"n1" (rec {:status :not-working} {:status :not-working} ok)} 2)])))))
  (testing "invocations are no windows"
    (is (= [] (:windows (disk/check [(op :fill-disk {:nodes ["n1"]} 1) (op :free-disk nil 2)])))))
  (testing "a fill or free that threw fails the run: a disk may have stayed full"
    (let [r (disk/check [(op :fill-disk {"n1" {}} 1)
                         (op :free-disk {"n1" (rec {:status :not-working} ok)} 2)
                         (assoc (op :free-disk nil 3) :exception {:via []}, :error "indeterminate: boom")])]
      (is (false? (:valid? r)))
      (is (= [{:f :free-disk, :time 3, :error "indeterminate: boom"}] (:nemesis-errors r)))))
  (testing "a free with nothing filled, as the final generator's, is no window"
    (is (= [] (:windows (disk/check [(op :free-disk {} 5)])))))
  (testing "nothing ran, or the fault never bit"
    (is (= :unknown (:valid? (disk/check []))))
    (is (= :unknown (:valid? (disk/check [(op :fill-disk {"n1" {}} 1) (op :free-disk {"n1" (rec ok ok)} 2)]))))))

(deftest in-the-test
  (testing "registered, with a checker of its own"
    (is (contains? faults/supported :disk))
    (is (= [:disk] (keys (faults/checkers opts))))
    (is (empty? (faults/checkers (assoc opts :faults [:kill])))))
  (testing "the generator fills and frees, ending with a free after the fault window"
    (let [fs (->> (gt/simulate (gen/context {:concurrency 3})
                               (:generator (fiber/fiber-test opts))
                               (fn [_ctx op] (-> op (assoc :type :ok) (update :time + 50000000))))
                  (filter #(= :info (:type %)))
                  (map :f)
                  (filter #{:fill-disk :free-disk}))]
      (is (< 2 (count fs)))
      (is (= :free-disk (last fs)))
      (is (every? #{[:fill-disk :free-disk] [:free-disk]} (map vec (partition-all 2 fs)))))))
