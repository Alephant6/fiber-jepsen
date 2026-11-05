# fiber-jepsen

[Jepsen](https://github.com/jepsen-io/jepsen) tests for [Fiber](https://github.com/nervosnetwork/fiber), the payment channel network on Nervos CKB.

Fiber's unit tests drive nodes in-process, and its integration tests replay scripted scenarios. This suite does something different. It runs real `fnn` binaries on separate hosts and sends concurrent payments through them while injecting faults: crashes, pauses and network partitions. It then checks invariants over the final state of every node.

**Status: work in progress.** It runs end to end against a CKB devnet; what it has found so far is in [FINDINGS.md](FINDINGS.md).

## What it checks

After all faults are healed and the network has had time to converge:

- every channel opened during setup is still `ChannelReady`, with no pending TLCs;
- both ends of a channel agree on its balances (A's local balance equals B's remote balance);
- no channel's total (local + remote) changed: payments move money within a channel, they never create or destroy it;
- every payment reached `Success` or `Failed`;
- no payment the client saw succeed reports a different status later;
- a fresh payment each way over every channel (the probe) succeeds, unless the sender's balance on that channel is below the probe amount; with `--probe-routes`, so does one between every two nodes without a channel, which only multi-hop routing can deliver;
- every node is healthy (assertion 7 of the [chaos-test plan](https://gist.github.com/quake/c73bba93b5b1928391063038fc9fe34a)): no fnn panicked (`panicked at` in its log), and after the probe every fnn is running, answers `node_info` and can still write to its store (`new_invoice`, an invoice that is never paid). A node whose `node_info` answers but whose `new_invoice` doesn't is alive but not working. The run also fails if the kernel killed a process in a node's container for memory, or if a restart by the harness found fnn already gone although nothing had stopped it (it exited on its own, and the restart would otherwise hide that). The `:health` result lists each node's ERROR lines by kind, how many times fnn started, and its peak memory.
- every fault asked for with `--faults` took effect (the `:faults` check). Jepsen records a fault op that throws as `:info` and no stock checker fails a run for it, so without this a fault that never worked would leave a valid run that tested nothing. The check fails when a fault op threw, when a pause stopped no running fnn, or when every op of a fault failed (a `disconnect_peer` or CCH kill that returned an error, say), and is `:unknown` when a fault asked for never ran. It reports how many ops each fault had and how many took effect.

With `--nonce-check` and an instrumented fnn, it also checks that no MuSig2 nonce signed two different contexts, across restarts (see [below](#musig2-nonce-reuse---nonce-check)).

With `--close`, every channel is asked to close cooperatively halfway through the faults (payments carry on), and the test instead checks that every channel ends `Closed`. This exercises the closing signatures, which the other workloads never reach.

With `--ledger`, it also checks that money adds up at every node, not only on every channel (see [below](#per-node-ledger---ledger)).

With `--force-close-ok`, a channel that ended closed (every end `Closed`) doesn't fail the run, nor do its ends' balances, a payment whose TLC it still holds, a probe over it, or a route probe between nodes it cut apart; the result lists them under `:force-closed`. A node force-closes a channel when its peer hasn't answered an update for 30 s, so a pause or partition longer than that closes channels. Whether that is expected is open question 1 on the [chaos-test plan](https://gist.github.com/quake/c73bba93b5b1928391063038fc9fe34a); until it is answered, the default stays: a closed channel fails the run.

With `--onchain`, after the final read it closes every channel and checks on chain that each party got its last balance (see [below](#on-chain-settlement---onchain)).

With `--expiry-check` as well (C-03, with the `outage` fault's clock jump), it checks that a node whose next hop stayed down past a TLC's expiry got the TLC back on chain, and that every payment ended `Success` or `Failed` (see [below](#tlc-expiry-c-03---expiry-check)).

With `--channel-ops open,close,force-close`, the clients also open, cooperatively close and force-close channels on the topology's edges while payments run (`src/jepsen/fiber/channels.clj`), about every `--channel-interval` seconds, so that faults land in the middle of opening and closing. A close takes one of the edge's channels that both ends report ready, and leaves at least `--channel-keep` of them (default 1), so payments and the probe keep a route. The test then also checks that every end of every channel is `ChannelReady` or `Closed`, never in between, and that both ends agree on which. Fiber notices a counterparty's force close only in a check every 300 s (`CHECK_CHANNELS_SHUTDOWN_INTERVAL`), so after the quiet period the test waits up to `--channel-settle-timeout` seconds (default 360) for that, before the probe, and reports how long it waited. Setup channels the workload closed are left out of the checks above, and a payment whose TLC is still pending on such a channel isn't counted as unfinished, nor are its TLCs upstream on ready channels: it settles on chain, by preimage or once it expires (hours, in a release build). `--ledger` reports `incomplete` once a channel was opened during the run, since it sums only the setup channels.

With `--availability-window`, it also checks that the payment success rate after healing is back to what it was before the first fault (see [below](#availability-after-healing---availability-window)).

## Faults

| Fault | How |
|---|---|
| `kill` | `SIGKILL` on one node, all nodes, or the middle node (`--kill-targets one,all,middle`; middle is a node on more than one channel, n2 in `line`), then restart. A crash can land in the middle of a write. With `--kill-signal term`, `SIGTERM` instead: fnn stops its RPC server, cancels its tasks and waits for them, a path `SIGKILL` never reaches. The nemesis waits up to `--term-grace` seconds (default 30, the grace period fnn's signal handler assumes) for fnn to exit and records how long it took; past that it sends `SIGKILL`, and the `:term-exits` check fails. `SIGCONT` follows each `SIGTERM`, as systemd sends it, so a paused fnn handles it. |
| `pause` | `SIGSTOP` / `SIGCONT` on one node, by the pid in fnn's pidfile. The peer stays connected but gets no replies. The nemesis op records each node's process state afterwards (`T` = stopped). Before 2026-11-05 this fault stopped nothing on the node images, so earlier pause results are runs without a fault (see [FINDINGS.md](FINDINGS.md#reproducibility)). |
| `partition` | iptables drops traffic between one node and the others, or with `--partition-targets half` (or `one,half`) between a random half of the nodes and the other half. A partition lasts until the next fault op, or with `--partition-duration MIN-MAX` a number of seconds drawn per partition, minutes if need be. |
| `disconnect` | `disconnect_peer` over RPC closes the connection between two neighbours; a later `connect_peer` reconnects them. Channels reestablish while both processes keep running, which a partition rarely causes (TCP keeps the connection and retransmits). With `--disconnect-flash MIN-MAX`, each fault is a flash instead: one op disconnects a pair, waits a time drawn from MIN-MAX ms and reconnects it (C-05 uses 100-5000), and records whether the pair was already connected again before the reconnect. |
| `packet` | tc netem delays traffic, and with `--packet-loss` drops some of it: to and from one node, between all nodes, or from the nodes to the CKB node (`--packet-targets one,all,chain`). Each fault draws its delay from `--packet-delay MIN-MAX` (ms; default 500-3000), with a jitter of a tenth of it. One shaping is active at a time. |
| `chain-partition` | iptables on the fnn hosts cuts one node, or all of them, off the CKB node, while they keep talking to each other: `drop` (requests time out) or `reject` (TCP reset, they fail at once), with `--chain-partition-targets one,all` and `--chain-partition-mode drop\|reject`. fnn can't submit or follow funding, closing and settlement transactions until it heals. |
| `clock` | fnn's wall clock jumps forward or back by `--clock-jump MIN-MAX` seconds (default 60-1800), on one node or on every node by the same offset (`--clock-targets one,all`), then returns to the real time. fnn runs under libfaketime, which reads the offset from a file the fault rewrites; only `CLOCK_REALTIME` moves, as when NTP steps a clock. fnn measures its 30 s peer-response deadline with the wall clock, so a forward jump of more than 30 s while a channel waits for an ack can make fnn force-close it. The CKB devnet keeps real time. After each change the op compares the timestamp of a fresh invoice, from fnn's clock, with the control node's time, and the run fails if fnn's clock didn't move (`:clock`). |
| `outage` | One node goes down long enough for the TLCs its neighbours offered it to expire (C-03): the first outage comes `--outage-after` seconds into the fault window (default 30), so that payments flow first; an outage waits up to `--outage-await` seconds (default 30) for a neighbour to hold a TLC in flight to the target, kills the target's fnn, keeps it down for `--outage-duration MIN-MAX` seconds (default 120-180) and starts it again. The target is a node that forwards payments, or any node (`--outage-targets middle,one`; default middle). A release build makes a payment's TLC last at least 4 hours, so with `--outage-jump SECONDS` an outage that caught a TLC moves every clock forward by that much while the node is down: every fnn's, with libfaketime as the `clock` fault does, and the CKB node's, through a small wrapper preloaded into ckb (`docker/chain/clockshift.c`) that adds the offset in a file on a volume the chain container shares with the control container. (Not libfaketime there: ckb links jemalloc in place of `malloc`, and a jemalloc program under libfaketime 0.9.10 hangs at start.) The clocks never jump back, since block times can't. The op records the TLCs caught and every clock's offset as measured afterwards, and the run fails if one didn't move (`:outage`). Not with `clock` when it jumps. |
| `cpu`, `memory` | `docker update` limits one node's container, or all of them (`--resource-targets one,all`), to `--cpu-limit MIN-MAX` CPUs (default 0.1-0.5) or `--memory-limit MIN-MAX` MiB without swap (default 128-256, above the 92 MiB of anonymous memory fnn used at most under c-12's load), then lifts the limit. The control container calls Docker's API through the host's Docker socket, which `docker/docker-compose.yml` mounts into it: that gives it control of the host's Docker daemon, fine on a CI runner, a trade-off on a workstation. Docker can't remove a limit, so lifting sets it to the host's size. Each op records the limit the node's cgroup has; a lift records the window's CPU throttling, memory events, memory in use (anonymous and page cache) and a health sample taken under the limit. A limit or lift that throws, or a limit never lifted, fails the run (`:resource-limits`). With fnn's network debug log (`--log-level 'info,fnn::fiber::network=debug'`), a pair of nodes that disconnects three times or more within a limit and the minute after it fails the run (`:disconnect-loops`). |
| `disk` | Fills the filesystem that holds one node's store (RocksDB), then frees it. With this fault each node's store is on a 256 MiB tmpfs of its own (`docker/docker-compose.yml`), so a fill can't touch the host's disk; fnn's log is not on it. RocksDB preallocates its write-ahead log (about 70 MB here), so a running fnn can go on writing into that space on a full disk; half the fills, at random, also crash fnn (SIGKILL) and start it again on the full disk, so that RocksDB must open the store with no space. Before freeing, the nemesis samples the node's health and reads what fnn logged since the fill; 10 s after, it samples again, and restarts fnn if it isn't healthy. The `disk` check passes a node that stopped or answered with an error while full and is healthy again after the space came back (by itself, or after a restart if it had stopped); it fails a node that was alive but didn't answer (`hung`, `unresponsive`) or still didn't work once space was freed. Scenario `c-13`. |
| `power` | Power loss: SIGKILL fnn on one node or every node (`--power-targets one,all`) and drop every write to its store that wasn't fsynced, then start fnn again. The store is on [LazyFS](https://github.com/dsrhaslab/lazyfs) (`jepsen.lazyfs`), which keeps writes in a cache of its own until fsync; `scripts/run.sh` builds it into the node image and gives the node containers FUSE (`docker/docker-compose.lazyfs.yml`) when its arguments mention `power`. Run it with `--store-sync-writes true` and `false` (Fiber's `fiber.store_sync_writes`, #1703: in Fiber from `4777d5d6`, off by default). The `power` check reports the mode each node logged, the power losses per node and the bytes each dropped, and fails if fnn didn't come back; lost state shows in the channel, ledger and health checks. |
| `old-state` | One node comes back from an old copy of its store (C-11): a `save-state` op copies the store while fnn is stopped by `SIGSTOP`; `--old-state-hold` seconds later (default 40) a `restore-state` op crashes fnn, puts the copy back and starts it, so the node holds commitments its peers have revoked. `--old-state-restore raw` (default) copies it over the store directory, as a disk or snapshot rollback would; `fiber` runs `fnn --restore`, Fiber's own restore, which marks the channels `Stale`. With `--old-state-force-close` the restored node then force-closes its channels. Targets: `--old-state-targets one,middle`. See [Old state](#old-state-c-11---faults-old-state). Not with `disk` or `power`. |
| `cch-kill` | With `--cch` only: `SIGKILL` on the cross-chain hub (CCH), then a restart. The op records the pid and whether it is gone. |
| `lnd-disconnect` | With `--cch` only: iptables cuts CCH from the hub's LND, or the hub's LND from the user's (`--lnd-disconnect-targets cch,peer`), dropping packets or resetting connections (`--lnd-disconnect-mode drop` or `reject`). Healing removes the rules and reconnects the two LNDs. |

Each fault is a namespace under `src/jepsen/fiber/faults/` that defines `fault`: the `--faults` names it handles, its nemesis package, and its operations, drawn from a random stream of its own so that the seed fixes them. `faults.clj` lists them; adding a fault is a new namespace and a line there, and changes no other fault's operations for a given seed (`test/jepsen/fiber/faults_test.clj` checks this).

Why a clock fault: Fiber expires TLCs by wall-clock timestamp (off-chain checks use each node's local clock; on-chain claims use timestamp `since`), rather than by block height as in Lightning. Jepsen's built-in clock nemesis changes the host clock, which Docker containers share, so the skew is injected per process with libfaketime.

## Layout

- `docker/`: a CKB devnet (`chain`), six fnn nodes (`n1`–`n6`) and the Jepsen control node, on a private network `10.77.0.0/24`. A test uses `n1`–`n3` unless `--nodes` names others.
  `docker-compose.cch.yml` adds, for `--cch`, bitcoind, two LNDs and a standalone CCH (`docker/btc/` is their image); the default cluster doesn't start them.
- `scripts/make-snapshot.sh`: creates the devnet with Fiber's own `tests/deploy/init-dev-chain.sh` (funded wallets, contracts, UDT), then stages it with the binaries into `docker/build/`. Fiber's test nodes are 1–3; the script makes keys for nodes 4–6 from their numbers and funds each with 500000 CKB (no UDT). Every test starts from this snapshot. With `FNN_BIN` set empty it leaves out `bin/fnn`, which nothing else depends on; CI adds each commit's fnn afterwards.
- `src/jepsen/fiber/`:
  - `db.clj` (process lifecycle)
  - `topology.clj` (opens the channels of a named graph, `--topology`, over `--nodes`: `line` is n1 - n2 - n3; `ring` closes the line; `mesh` adds a channel from each node to the one opposite it on the ring, so six nodes get three channels each)
  - `client.clj` (keysend payments, final read; workloads add payment kinds with `send-payment!` and operations with `invoke-op`)
  - `channels.clj` (opening and closing channels during the run, with `--channel-ops`)
  - `invoice.clj` (invoice payments, with `--pay-kinds invoice`)
  - `checker.clj` (invariants)
  - `health.clj` (process health: panics, and a sample of every node after healing)
  - `ledger.clj` (per-node money check, with `--ledger`)
  - `closes.clj` (what `--force-close-ok` excuses)
  - `nonce.clj` (MuSig2 nonce reuse, with `--nonce-check`)
  - `onchain.clj` and `chain.clj` (on-chain settlement, with `--onchain`; reading the CKB node)
  - `expiry.clj` (C-03: TLCs taken back on chain after they expire, with `--expiry-check`)
  - `faults.clj` and `faults/` (one namespace per fault: `process` for kill and pause, `partition`, `disconnect`, `packet`, `chain-partition`, `clock`, `outage`, `resources` for cpu and memory, `disk`, `power`, `cch-kill`, `lnd-disconnect`)
  - `cch.clj` and `swap.clj` (cross-chain swaps, with `--cch`: the Lightning side, CCH, the swap workload and its checker)
  - `scenarios.clj` (named scenarios of the chaos-test plan, `--scenario`)
  - `verdict.clj` (which checks failed in a stored run, for CI summaries)
  - `schedule.clj` (payments, and the random streams faults draw from, fixed in advance by the run's seed)
- `instrument/`: the logging-only fnn instrumentation the nonce check needs.
- `.github/workflows/`: `ci.yml` (lint and unit tests), `smoke.yml` (a run without faults whenever the harness changes), `jepsen.yml` (any arguments against any Fiber commit; other repositories can call it, see [below](#in-another-repositorys-ci)), `matrix.yml` (every scenario against one Fiber commit, one job each, see [below](#the-fault-matrix-matrixyml)) and `failure-summary.yml` (a Copilot note on failed manual runs, see [below](#a-note-on-failed-runs-copilot)); `.github/failure-summary/` has its digest script, prompt and known issues.

## Running

On Linux or WSL2, with Docker, a Fiber checkout, ckb 0.202.0 and ckb-cli in `PATH`:

```bash
# once: build fnn and the devnet snapshot
cargo build --release -p fiber-bin          # in your Fiber checkout
FNN_BIN=/path/to/fiber/target/release/fnn FIBER_SNAP=/path/to/a/clean/fiber/checkout \
  scripts/make-snapshot.sh

# each run starts from a fresh devnet
scripts/run.sh --time-limit 60 --faults kill
scripts/run.sh --time-limit 300 --faults kill,pause,partition --rate 10

# several runs, one summary line each: verdict, post-heal probe, channel states, seed, store directory
scripts/repeat.sh 4 --time-limit 60 --faults kill --kill-targets all --rate 20 --nemesis-interval 8 --quiesce 60

# no faults: checks the harness itself (the cluster comes up, payments flow, every checker passes)
scripts/run.sh --time-limit 45 --faults none --ledger

# six nodes in a mesh; a random half cut off for 2-5 minutes at a time
scripts/run.sh --nodes n1,n2,n3,n4,n5,n6 --topology mesh --faults partition \
  --partition-targets half --partition-duration 120-300 --time-limit 600 --probe-routes
```

CI runs that last command on every pull request that touches the harness, and on `main` after a merge, against the Fiber commit pinned in `.github/workflows/smoke.yml`; fnn is built once per Fiber commit and cached, and its compiled dependencies are cached across commits, so a new commit compiles only Fiber's own crates. The devnet snapshot doesn't use fnn, so it is cached by the contents of Fiber's `tests/deploy` and `tests/nodes` (with the ckb version and the snapshot scripts): a commit that changes only Fiber's code reuses it. The smoke run is a check of this suite, not of Fiber, so it never injects faults. To run any arguments against another Fiber commit on a GitHub runner, start the Jepsen workflow (`jepsen.yml`) by hand from the Actions tab.

Results, including node logs and a timeline, are written to `store/`. To test another fnn build, copy it over `docker/build/bin/fnn`; `run.sh` rebuilds the node image.

The node image is Ubuntu 24.04 (glibc 2.39), the system of the GitHub `ubuntu-24.04` runners on which both Fiber's CI and this repository's CI build fnn. An fnn built on a system with glibc 2.39 or older runs in it: one from Fiber's Docker image (built on Debian 12) or a portable release package (built on Ubuntu 22.04) does too. One linked against a newer glibc doesn't start, and Jepsen then reports only that the RPC never came up. `NODE_BASE=debian:bookworm-slim scripts/run.sh ...` builds the nodes on another base.

`scripts/stall_report.py store/<test>/<run> ...` lists, for each channel with a problem, whether it is stalled on an unbalanced revocation nonce ledger ([#1561](https://github.com/nervosnetwork/fiber/issues/1561)), closed, or fine but still holding TLCs. It needs channel debug logs: add `--log-level 'info,fnn::fiber::channel=debug'` to the run.

### Scenarios of the chaos-test plan (`--scenario`)

The team's chaos-test plan ([C-01a to C-16](https://gist.github.com/quake/c73bba93b5b1928391063038fc9fe34a)) names each scenario by its topology, workload and fault. `src/jepsen/fiber/scenarios.clj` lists the ones this suite covers, each with the arguments that run it, its priority in the plan, and where it departs from the plan (three nodes in a line instead of A-B, for example; c-01a and c-05 run A-B with `--nodes n1,n2`, which overrides `run.sh`'s three nodes). `--scenario NAME` puts those arguments right after the option, so arguments after it override them:

```bash
scripts/run.sh --scenario c-01b                  # as listed: SIGKILL one node, 300 s
scripts/run.sh --scenario c-08 --time-limit 60   # a shorter run of the same faults
```

A scenario is added to the list once the suite covers it; `test/jepsen/fiber/scenarios_test.clj` checks that every listed scenario's arguments parse.

### Running a run again (`--seed`)

Every run has a seed, and the payments and faults are drawn from it in advance (`schedule.clj`): each payment's amount, destination and attempt id, each fault's kind and target (node or channel), and the gaps between operations. A run with the same seed, arguments and Fiber build issues the same payments, and the same faults, in the same order. Each fault namespace draws from a stream of its own (kill and pause share one), so adding a fault to `--faults` doesn't change the others' operations. Without `--seed` the harness picks one; it is in the test map at the top of `jepsen.log` (`:seed`), and `repeat.sh` prints it. In CI every run gets its own, listed in the job summary.

```bash
scripts/run.sh --time-limit 60 --faults kill --seed 123456789
```

A seed doesn't make a run deterministic. When each operation runs, how payments and faults interleave, which node sends each payment (the first client that is free), and everything inside fnn and ckb (keysend preimages, MuSig2 nonces, funding transactions, block times) still vary, so a failure that depends on timing may take several runs with its seed to come back.

Jepsen's own `--seed` isn't enough for this. When the next operation isn't due yet, Jepsen drops the generator's answer and asks again later, and a generator that draws random values when asked draws new ones each time. How often Jepsen asks depends on when operations complete, so with such generators the same seed gives different payments and faults; `test/jepsen/fiber/schedule_test.clj` checks that the drawn-in-advance ones don't change with latency.

### MuSig2 nonce reuse (`--nonce-check`)

Fiber signs channel transactions with two-party MuSig2. A secret nonce must sign only one signing context. With two-nonce MuSig2, partial signatures from one nonce over three different contexts reveal the signing key; two are one step away. Crash, reconnect and replay paths are where a node might sign again, which is why Channel V2 ([#1677](https://github.com/nervosnetwork/fiber/pull/1677)) gives every nonce one persisted signing context. This check tests that property under the suite's faults.

1. **Instrumented fnn** (`instrument/`). `apply_nonce_oracle.py` adds a `nonce-oracle` cargo feature to a Fiber checkout and hooks every MuSig2 partial-signing site: V1's `sign`, `sign_and_aggregate` and the channel announcement, and, when the checkout has them, V2's `guarded_sign` and `bind_context`. Each record holds a hash of the signer's *public* nonce, a hash of the signing context (aggregated key, aggregate nonce, message), the call site and the process. Secret nonces are never written. Records go to `/fiber/nonce-oracle.log` with one `write(2)` per line before the signature is returned, so they survive `SIGKILL`. The hooks only log; they don't change what is signed or sent.
2. **Checker** (`src/jepsen/fiber/nonce.clj`). It pools each node's records across restarts and fails the test if a nonce's partial signature left the node in two different contexts. It is `:unknown` if a node has no records, so an uninstrumented binary can't pass silently. So that a clean result can be weighed, it also reports per node how many processes signed (restarts), how many channel reestablishments started and how often V2 resent a stored `CommitmentSigned` or `RevokeAndAck` verbatim, how often V2 returned a stored signature and how often a later process did so, how often a later process signed the same context again, and how often V2 refused a conflicting context.

```bash
# a clean worktree of the commit under test
git -C /path/to/fiber worktree add --detach /tmp/fiber-1677 393b77fc
scripts/build-fnn-nonce-oracle.sh /tmp/fiber-1677
cp /tmp/fiber-1677/target/release/fnn docker/build/bin/fnn
scripts/repeat.sh 10 --time-limit 60 --faults kill --kill-targets all --rate 20 --nemesis-interval 8 \
  --quiesce 60 --nonce-check
scripts/nonce_report.py store/fiber-kill/<run> ...    # per node: restarts, replays, reestablishments
```

**Self-test.** `scripts/build-fnn-nonce-oracle.sh <checkout> nonce-oracle-plant` builds a binary that, each time a node restarts, appends a second `Sent` record with a made-up context for the last nonce the previous process sent. A kill run with it must fail with nonce violations; `test/jepsen/fiber/nonce_test.clj` covers the checker's rules (`lein test`).

**Limits.**
- `SIGKILL` keeps the OS page cache, so kills test "persisted before sent" at the process level, not across a power loss.
- A record is written when the signing code returns a partial signature for the peer. A crash before the message actually leaves counts it as sent, which can only add violations.
- Signatures that are only aggregated locally (V1 `sign_and_aggregate`) become visible only if that transaction is published. They are recorded as `Aggregated`, count toward `:revealable-count`, and are not violations by themselves.

### Payment kinds (`--pay-kinds`)

By default every payment is a keysend. `--pay-kinds keysend,invoice` draws each payment's kind from the list, uniformly; a kind listed twice is drawn twice as often, and `--pay-kinds invoice` makes every payment an invoice payment. For an invoice payment the receiver creates an invoice with `new_invoice` (devnet currency `Fibd`, an hour's expiry, a preimage derived from the attempt id), and the sender pays it with `send_payment`. Unlike a keysend, it needs the receiver up: when `new_invoice` fails, the payment fails without being sent (`[:new-invoice :receiver-down]`). The kinds come from a stream of their own, so the payments' amounts, destinations and ids for a seed are the same whatever the kinds; with keysend alone nothing is drawn. Invoice payments carry their attempt id like keysends, so `--ledger` counts them. A workload adds a kind with a `client/send-payment!` method; `--pay-kinds` accepts every kind that has one.

### Per-node ledger (`--ledger`)

The per-channel checks can't see a payment that is only half done. Say n1 pays n3 through n2, and n2 settles its TLC with n3 while the TLC from n1 fails. n1 reports `Failed`, both channels still conserve their totals, both ends still agree, and the payment is final, so every check above passes; yet n2 has paid n3 for nothing.

With `--ledger`, the checker compares each node's balance change, summed over the channels opened during setup, with the change its payments imply: the sender loses the amount plus the fee, the receiver gains the amount, and the node in between gains the fee. To count every payment, each one carries its attempt id in a custom record (key `0xf1be`), and the final read lists every node's payment sessions with `list_payments`. Payments whose `send_payment` reply never arrived (a timeout during a pause, say) are counted too. Amounts and destinations come from the history; statuses and fees come from the sending node.

On a graph other than a line (`--topology ring` or `mesh`) a payment can take any route, and fnn reports routes only in debug builds, so the forwarders aren't known. There each node is expected to change by its own payments only, and what remains, its forwarding income, must be between 0 and the fees of the payments it neither sent nor received; the nodes' income must add up to the fees paid. A node that settled a TLC it was never paid for falls below 0.

The result is invalid when a node's change differs from the expected one, when the status the client saw when a payment finished differs from the sender's final status (or the client was told no payment was created and one succeeded), or when one attempt produced two payments. A difference is `:unknown` rather than invalid when the final records can't show every settlement: a channel that is closed, missing or still holds TLCs, a payment that hasn't finished, or a payment the sender no longer lists. The check doesn't read the chain, so settlement after a force close is not covered. `repeat.sh` prints the outcome (`balanced`, `unbalanced`, `client-mismatch` or `incomplete`); `test/jepsen/fiber/ledger_test.clj` covers the rules.

The option is off by default, so runs without it send the same requests to the nodes and run the same checks as before.

### On-chain settlement (`--onchain`)

The plan's assertion 6: once every channel is closed, what each party got on chain matches its last channel balance. The other checks read only the nodes; this one reads the CKB node (its RPC and indexer).

A node's wallet is the cells of its default funding lock script (`node_info`): Fiber funds channels from it, pays the node's share of a closed channel to it, and takes the fees of settlement transactions from it. With `--onchain` the test records every wallet before the faults. After the final read it asks every channel that is still open to close cooperatively (and force-closes one that hasn't closed a minute later), waits up to `--settle-timeout` seconds (default 300) for the closes to settle, and traces each channel from its funding cell: the transaction that spent it (the shutdown transaction, or a commitment transaction after a force close), then every transaction that spent a non-wallet cell it created (the commitment cell, which the watchtowers settle after the commitment delay of one epoch, ten blocks on the devnet).

A party's receipt is what its wallet gained from those transactions: outputs to its lock minus inputs from it, since a settlement transaction spends wallet cells for its fee and returns the change. Fiber pays each party its balance plus the CKB it reserved at opening (a cell's occupied capacity plus 1 CKB), less fees. Both parties use the default lock script, so each reserved half of (funding capacity − local − remote). So for each party, from its own record of the closed channel:

    balance + reserve − offered TLCs − the channel's fees  ≤  receipt  ≤  balance + reserve + received TLCs

A pending TLC goes to one side or the other: to the receiver with the preimage, back to the offerer once it expires. The result is invalid when a receipt is outside its range, when another node's wallet got something from the channel, when a channel is still open, or when its cells are still unspent after the wait; that last one is `:unknown` instead while one of the channel's TLCs hasn't expired, which in a release build takes hours. It is also invalid when a wallet's change since the start differs from what the channels' funding and closing transactions explain. Per channel the result shows each party's balance, reserve, receipt and the difference; `test/jepsen/fiber/onchain_test.clj` covers the rules.

### TLC expiry (C-03, `--expiry-check`)

C-03 of the chaos-test plan: A-B-C with multi-hop payments, and B down for longer than the TLC expiry. A must get its money back and the payment must end `Failed`. A node force-closes a channel once a TLC it offered is within two thirds of the commitment delay of its expiry, and after the delay its watchtower takes the TLC back by the timeout path; Fiber then fails the payment.

A release build of fnn makes TLCs last hours: the forwarding delta defaults to 4 hours and may not go below 160 minutes, because an epoch counts as 4 hours. On top of the deltas, every route adds a random privacy delay of at least 4 hours to each hop's expiry, in debug builds too (`rand_tlc_expiry_delta` in Fiber's `graph.rs`), so no outage within a run outlasts a TLC. This suite therefore moves the clocks instead (open question 2 of the plan asked: debug build or clock jump), with the `outage` fault:

- `--faults outage --outage-targets middle --outage-jump SECONDS` takes the forwarding node down once one of its neighbours holds a TLC in flight to it, and while it is down moves every fnn's clock and the CKB node's forward past the TLCs' expiry. The chain's clock has to move too: the watchtower takes an expired TLC back with a transaction whose `since` is a timestamp, which CKB checks against the median time of the last 37 blocks.
- `--tlc-expiry-delta MS` sets every channel's forwarding delta after setup (`update_channel` at both ends), and the test waits until every node's graph shows it. A release build refuses anything under 160 minutes (9600000), and setup says so.
- `--final-tlc-expiry-delta MS` makes payments ask for that final delta (`send_payment`'s `final_tlc_expiry_delta`; invoices are made with `final_expiry_delta`). Keysend's default is 24 hours.
- `--tlc-expiry-limit MS` makes payments ask that no TLC expire later than that (`tlc_expiry_limit`). Fiber keeps 4 hours of it for the random delay, so it must cover 4 hours plus the route's deltas; just over that, the delay is exactly 4 hours.

Scenario c-03 sets 160-minute deltas and a limit of 4 hours plus two deltas plus a minute, so a TLC across n2 expires 9 h 20 min after it is sent and a direct one 6 h 40 min after, and jumps 9.5 hours (34200 s). Its neighbours then force-close their channels with n2 within a minute (they check every 60 s), and their watchtowers take the TLCs back after the commitment delay. n2 sees those closes only in a check it runs every 300 s, so c-03 waits 330 s after the outage (`--quiesce`) before the final read; until then n2's end of each channel still reads `ChannelReady`.

With `--expiry-check` (and `--onchain`), after the closes have settled the test reads every payment's status again. For every TLC that a node's record of a closed channel still lists, offered by that node for a payment of its own, it wants the payment `Success` or `Failed`, and the node's receipt from the channel to hold the TLC if the payment `Failed`:

    receipt  ≥  balance + reserve − the channel's fees − the TLCs it offered that aren't its own Failed payments

A TLC still counted in the balance goes to the peer only when the peer claims it with the preimage, which settles the payment, so a `Failed` payment whose TLC went to the peer means the payer lost it. Every payment amount is a whole multiple of 0.01 CKB and fees are a few thousand shannons, so a TLC that went the wrong way shows. The result is invalid when a payment is still in flight after settling, or when a payer is short; it is `:unknown` when no payer TLC was left on a closed channel (the kills caught no payment in flight, so the run tested nothing of C-03) or a channel hasn't settled. `test/jepsen/fiber/expiry_test.clj` covers the rules.

### Old state (C-11, `--faults old-state`)

C-11 of the chaos-test plan kills and restarts a watchtower; amended in the comment on the plan, it restarts a node from an old copy of its data instead, since killing a watchtower when nobody cheats tests nothing. The right result: the restored node never broadcasts a revoked commitment, and if it does, its peer's watchtower punishes it on chain. Every fnn here runs Fiber's built-in watchtower, which checks the chain every second on the devnet (`watchtower_check_interval_seconds: 1` in Fiber's node config template).

The `old-state` fault copies one node's store while fnn is stopped by `SIGSTOP` (a filesystem snapshot would take it so; the node doesn't restart or reconnect), lets payments move its channels on, then crashes fnn, puts the copy back and starts it. The restore op records the node's channels before the crash and after the restart (state, balances, the hash of the latest commitment it holds) and the store's fingerprint; it took effect (the `:faults` check) when the store is the saved copy and at least one channel's latest commitment moved back. After the peers have reconnected it records their views of the channels, and with `--old-state-force-close` it asks the restored node to force-close every channel it still has.

What Fiber does with it (ae6f7d34): on reconnect both ends of each channel exchange their commitment numbers; more than one apart, both ends log `reestablish channel message with invalid commitment numbers` and stay reestablishing, and the channel carries no more payments. With `--old-state-restore fiber`, `fnn --restore` marks the channels `Stale`; a Stale channel audits its peer's numbers on reestablish (`Audit Failed for Stale channel` when they are more than one apart) and refuses a force close.

The `old-state` check reads how each of the restored node's channels ended on chain (with `--onchain`): closed cooperatively, by a commitment that was settled by the latest balances, or by a commitment the peer's watchtower then revoked (`punished`: a revocation transaction, recognised by its witness, spent the commitment cell and paid the peer). It fails a channel that paid out by the restored node's old balances where they differ from its peer's latest record (a revoked commitment that nobody punished) and a revocation that paid the restored node, and reports the reestablish numbers and watchtower lines from every node's log. `--onchain` judges the restored node's channels by its peers' records, since its own are older than the chain, and counts a punished channel as settled correctly when the punisher got at least what it was owed. What the restore made the node forget (its later payments, its channels' balances, probes over them) is excused from the fiber check and listed under `:old-state-excused`. Scenario `c-11`: a raw restore of any node, then the forced close; `test/jepsen/fiber/old_state_test.clj` covers the rules.

### Availability after healing (`--availability-window`)

Assertion 5 of the chaos-test plan says that once the faults are gone, payments succeed as often as before. With `--availability-window SECONDS`, the test sends payments for that many seconds before the fault window, with no faults (the baseline), and again after healing and `--quiesce` (the recovery), at `--rate`. The `:availability` checker compares the two windows' success rates: the share of payments that ended `Success`, where a payment the node failed, refused or didn't answer in time counts against it. It is valid when the recovery's rate is at most `--availability-tolerance` percentage points (default 5) below the baseline's, and `:unknown` when either window has fewer than 10 payments.

```bash
scripts/run.sh --time-limit 120 --faults kill --kill-targets one --availability-window 30
```

The verdict uses only the success rate. Each window also reports successful payments per second and their latency (median, 95th percentile, maximum), which depend on the machine, so they are only reported, and the unsuccessful payments by sender and by error (numbers taken out, so errors of one kind count together). Not every failure is a fault: on `line` the acceptor of each channel starts with nothing on it, so n3 can't pay anyone until it has been paid, and in a run without faults most failures are n3's payments with "no path found" or "Insufficient balance". Such failures fall on both windows, and the tolerance has to absorb how they differ. The recovery starts after `--quiesce` rather than right after healing, so that restarted nodes are up and channels have reestablished: the check asks whether the network recovered, not how long that took. Both windows draw their payments from streams of their own, so the fault window's payments and faults for a seed are the same with or without the option, and their payments go through every other check (`--ledger` included). The option does nothing with `--close`, after which every channel is closed. `repeat.sh` and the CI summary print the two rates, baseline first.

### Cross-chain swaps (`--cch`)

C-15 of the plan swaps BTC on Lightning for wrapped BTC on Fiber through Fiber's cross-chain hub (CCH), while CCH is killed or LND is cut off. With `--cch`, the test needs the hosts of `docker/docker-compose.cch.yml`, which `run.sh --compose cch` adds to the cluster:

```bash
scripts/run.sh --compose cch --scenario c-15
scripts/run.sh --compose cch --cch --faults none --time-limit 60      # swaps, no faults
```

The setup follows Fiber's own `tests/deploy/lnd-init` and its e2e test `tests/bruno/e2e/cross-chain-hub-separate`. bitcoind runs regtest, `lnd-ingrid` is the hub's LND and `lnd-bob` the BTC user's, with one channel between them and half of it on each side. CCH runs on its own host as a separate service (fnn with only the `cch` and `rpc` services) that drives the hub's Fiber node, n3, over RPC; n1 is the CKB user, with two wrapped-BTC (devnet simple UDT) channels to n3, one opened by each. The keysend payments of the other scenarios keep running alongside.

Each swap is drawn from the seed (`--swap-rate`, `--swap-amount MIN-MAX` in sats, `--swap-kinds send-btc,receive-btc`). `send-btc`: bob's LND makes an invoice, CCH creates an order for it, and n1 pays the order's Fiber invoice; CCH then pays bob. `receive-btc`: n1 makes an invoice, CCH creates an order with a Lightning invoice, and bob pays it; CCH then pays n1. After the faults heal, the final read waits up to `--swap-settle-timeout` seconds until no order, payment or invoice is in flight, then reads every order from CCH and the four records of each swap: both payments and both invoices, from n1, n3 and both LNDs.

The swap checker is invalid when an order is unfinished or money is still held; when the two records of one side disagree; when one side was paid and the other wasn't (it names who lost: the hub when it paid out and wasn't paid, the user otherwise); when the order's status doesn't match what was paid; when the client saw a success that didn't happen, or a failure for a swap that was paid; when n1's wrapped-BTC balance or bob's Lightning balance moved by other than the paid swaps imply (the order amount includes the hub's fee, and the payer pays its own routing fee); and when a wrapped-BTC or Lightning channel isn't ready, holds TLCs or HTLCs, or its two ends disagree. It is unknown when a record couldn't be read, and when no swap completed. `--ledger` doesn't know about swaps; running both together is untested.

### In another repository's CI

`jepsen.yml` is a reusable workflow, so another repository's workflow can call it, pinned to a commit of this repository. A called workflow sees the caller's `github` context, so a call from Fiber's CI tests that run's Fiber commit, while the harness is checked out at the pinned commit. It needs no secrets and only read access:

```yaml
jobs:
  jepsen:
    if: contains(github.event.pull_request.body || '', '#jepsen-test')
    permissions:
      contents: read
    uses: Alephant6/fiber-jepsen/.github/workflows/jepsen.yml@<commit>
    with:
      args: --time-limit 60 --faults kill --kill-targets all --ledger
      runs: 1
```

Inputs: `fiber-repository` and `fiber-ref` (default: the caller's repository and commit), `args` for `run.sh`, `runs`, `ckb-version` and `label`. The job summary has one row per run; the job fails if any run's verdict isn't valid, after all runs have finished, and then uploads the store.

### The fault matrix (`matrix.yml`)

`matrix.yml` runs the scenarios of `scenarios.clj` against one Fiber commit, each in its own job: started by hand from the Actions tab (inputs `fiber-repository`, `fiber-ref`, `scenarios`, `runs`, `upload-store`), or called like `jepsen.yml`. `scenarios` is `all`, a priority (`p0`, `p1`, `p2`), or scenario names and priorities, comma-separated; `lein run -m jepsen.fiber.scenarios SCENARIOS` prints what it selects. A plan job turns the Fiber ref into a commit, so every job tests the same one, and lists the scenarios; a run without faults builds fnn and the devnet snapshot once into the caches; then every scenario runs `runs` times in a job of its own, all at once, and a report job puts every run's verdict and failed checks into one table, in the run's summary and in the `matrix-report` artifact. A scenario that needs extra hosts names its compose file (`:compose`), which goes to `run.sh --compose`. It doesn't run on a schedule yet: when and how the team wants it run is still open.

Each `jepsen.yml` job lists, per run, the checks whose `:valid?` isn't true (`jepsen.fiber.verdict` reads the run's `results.edn`) in its summary, and uploads the runs as tab-separated lines (artifact `<label>-runs`), which the report reads.

### A note on failed runs (Copilot)

When a Jepsen run started by hand from the Actions tab (later, a scheduled one) fails, the Failure summary workflow (`failure-summary.yml`) digests the store of each failed job, keeping the checkers' output, every close, force-close and panic line and the most frequent warnings, and asks Copilot CLI which checker failed, what the likely cause is, which known issue it looks like and what would confirm it. The note goes to that workflow's job summary, linked to the failed run, and is marked as auto-generated and not verified: the verdicts are the checkers'. It runs with the workflow's own token and is billed to the repository owner's Copilot plan (a report has cost about 0.5 AI credits), never for pull requests, and not for calls from other repositories, which keep only read access. To summarize an earlier failed run, start the workflow by hand with that run's ID. The known issues the model is given are in `.github/failure-summary/known-issues.md`; update them with `FINDINGS.md`.

### Mixed versions, and a tally over several batches

To run n1 on a different fnn build than n2 and n3, copy that build to `docker/build/bin/fnn-n1` and point `COMPOSE_FILE` at the override:

```bash
COMPOSE_FILE=docker-compose.yml:docker-compose.mixed.yml   scripts/repeat.sh 4 --time-limit 60 --faults kill --kill-targets all --rate 20 --nemesis-interval 8   --quiesce 60 --log-level 'info,fnn::fiber::channel=debug'
```

`scripts/jepsen_tally.py <repeat-log> store` summarises several `repeat.sh` batches from one log, each introduced by a line `== <label> (HH:MM UTC)`. Per batch it counts the runs the checker passed, the runs with a stalled or a closed channel, the runs with a ready channel that still holds a TLC, how often the re-sync completion ran, and bad signatures. Run it where the store directories can be read as written (it calls `stall_report.py`).
