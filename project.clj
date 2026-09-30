(defproject jepsen.fiber "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Nervos Fiber, a payment channel network on CKB"
  :url "https://github.com/Alephant6/fiber-jepsen"
  :main jepsen.fiber
  :dependencies [[org.clojure/clojure "1.12.6"]
                 [jepsen "0.3.14"]
                 [clj-http "3.13.1"]
                 [cheshire "6.2.0"]]
  :jvm-opts ["-Xmx4g" "-Djava.awt.headless=true"]
  ; The project is bind-mounted from the host; keep build output inside the container.
  :target-path "/tmp/fiber-jepsen-target/%s")
