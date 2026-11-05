(ns jepsen.fiber.faults.partition
  "The partition fault: iptables drops all traffic between one node and the
  others, through jepsen.nemesis.combined's partition package. TCP keeps the
  connection open through the drop and retransmits once it heals, so this rarely
  makes channels reestablish; the disconnect fault does."
  (:require [jepsen.nemesis :as n]
            [jepsen.nemesis.combined :as nc]
            [jepsen.fiber.schedule :as schedule]))

(defn ops
  "Endless pairs of a partition that isolates one node and its healing. The grudge
  is computed here, not from :one in the nemesis."
  [r nodes]
  (mapcat (fn [node]
            [{:type  :info
              :f     :start-partition
              :value (n/complete-grudge [[node] (vec (remove #{node} nodes))])}
             {:type :info, :f :stop-partition, :value nil}])
          (repeatedly #(schedule/pick r nodes))))

(def fault
  {:faults  #{:partition}
   :stream  :partition
   :package nc/partition-package
   :ops     (fn [r opts] (ops r (vec (:nodes opts))))})
