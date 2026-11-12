(ns jepsen.fiber.slowfs
  "A filesystem that can be made slow, for fnn's store (--faults slow-io):
  unreliablefs (https://github.com/ligurio/unreliablefs), a FUSE filesystem that
  passes every operation on to a directory behind it and can delay the
  operations its configuration file names.

  Each node has one mount: `mount-dir` shows `data-dir`. jepsen.fiber.db links
  fnn's store into the mount, so RocksDB's reads and writes all pass through
  it. unreliablefs reads `data-dir`/unreliablefs.conf again whenever that file
  is touched through the mount; `configure!` writes it and touches it. A
  slowdown delays every operation of the named kinds by a fixed time, before
  the operation runs. unreliablefs sleeps holding its one lock, so while an
  operation is delayed every other operation on the mount waits behind it: the
  filesystem serves one delayed operation at a time, as a busy disk with a
  queue of one would.

  unreliablefs is in the node image, at `bin`, only when the image was built
  for it, and the node containers need FUSE (docker/docker-compose.slowio.yml;
  scripts/run.sh adds it for --faults with slow-io in it, or --compose slowio).
  It is built against FUSE 2, whose library sits beside it.

  Every function here runs on the current node of a jepsen.control session."
  (:require [clojure.string :as str]
            [jepsen [control :as c]
                    [util :refer [await-fn]]]
            [jepsen.control.util :as cu]
            [slingshot.slingshot :refer [try+ throw+]]))

(def dir
  "Where the node image keeps unreliablefs and the FUSE 2 library it needs."
  "/opt/unreliablefs")

(def bin
  (str dir "/unreliablefs"))

(def root
  "/fiber/slowio")

(def data-dir
  "The directory behind the mount: what is written through `mount-dir` lands here."
  (str root "/data"))

(def mount-dir
  (str root "/mnt"))

(def store
  "Where jepsen.fiber.db links fnn's store: a directory in the mount."
  (str mount-dir "/store"))

(def conf-name
  "unreliablefs's configuration file, in the root of the mount."
  "unreliablefs.conf")

(def probe-file
  "A file in the mount, beside the store, that `probe-ms` writes."
  (str mount-dir "/probe"))

(def slow-ops
  "The operations a slowdown can delay, by unreliablefs's names: write is every
  write(2) of up to 128 KiB, fsync is fsync(2) and fdatasync(2). RocksDB appends
  to its write-ahead log with write, and reaches fsync when it syncs a log or
  finishes a table file."
  #{:write :fsync})

(defn installed?
  "Whether unreliablefs is in the node's image."
  []
  (c/su (cu/exists? bin)))

(defn mounted?
  "Whether `mount-dir` is a mount that answers. False too once the unreliablefs
  process behind it has died."
  []
  (try+ (c/su (c/exec :mountpoint :-q mount-dir))
        true
        (catch [:type :jepsen.control/nonzero-exit] _ false)))

(defn umount!
  "Unmounts `mount-dir`, if it is mounted, which also ends unreliablefs."
  []
  (c/su
    (doseq [cmd [[:fusermount :-uz mount-dir] [:umount :-l mount-dir]]]
      (try+ (apply c/exec cmd)
            (catch [:type :jepsen.control/nonzero-exit] _ nil)))))

(defn mount!
  "Starts unreliablefs with nothing slowed: an empty `data-dir` (and in it the
  directory for fnn's store), shown at `mount-dir`. unreliablefs leaves the
  foreground once the filesystem is mounted. big_writes lets a write carry up
  to 128 KiB, FUSE 2's limit; without it every 4 KiB would be a write of its
  own, each delayed."
  []
  (c/su
    (umount!)
    (c/exec :rm :-rf root)
    (c/exec :mkdir :-p (str data-dir "/store") mount-dir)
    (c/exec :env (str "LD_LIBRARY_PATH=" dir)
            bin mount-dir (str "-basedir=" data-dir) "-seed=1" :-o "big_writes")
    (await-fn (fn [] (or (mounted?) (throw+ {:type ::not-mounted})))
              {:retry-interval 200
               :log-interval   5000
               :log-message    "Waiting for unreliablefs to mount"
               :timeout        10000})))

(defn config
  "unreliablefs.conf for a slowdown {:delay-ms d, :ops #{:write :fsync}}: every
  operation of those kinds, on any file, sleeps d ms first. Empty for nil: no
  slowdown."
  [slowdown]
  (if-let [{:keys [delay-ms ops]} slowdown]
    (str "[errinj_slowdown]\n"
         "op_regexp = ^(" (str/join "|" (sort (map name ops))) ")$\n"
         "path_regexp = .*\n"
         "probability = 100\n"
         "duration = " (* 1000000 (long delay-ms)) "\n")
    ""))

(defn configure!
  "Sets the mount's slowdown (see `config`; nil for none). The file is replaced
  behind the mount in one step, then opened through the mount, which is what
  makes unreliablefs read it again."
  [slowdown]
  (c/su
    (let [tmp (str data-dir "/." conf-name ".new")]
      (cu/write-file! (config slowdown) tmp)
      (c/exec :mv tmp (str data-dir "/" conf-name))
      (c/exec :cat (str mount-dir "/" conf-name))
      nil)))

(defn probe-ms
  "How long it takes on this node, in ms, to write one 4 KiB block to
  `probe-file` and fsync it, through the mount: one write and one fsync, so at
  least the delay for each of the two that the slowdown names. Timed on the
  node, so the control node's SSH round trip isn't in it."
  []
  (parse-long
    (str/trim
      (c/su (c/exec :sh :-c (str "s=$(date +%s%N);"
                                 " dd if=/dev/zero of=" probe-file
                                 " bs=4k count=1 conv=fsync status=none;"
                                 " e=$(date +%s%N); echo $(( (e - s) / 1000000 ))"))))))
