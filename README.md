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
- a fresh payment each way over every channel (the probe) succeeds, unless the sender's balance on that channel is below the probe amount;
- every node is healthy (assertion 7 of the [chaos-test plan](https://gist.github.com/quake/c73bba93b5b1928391063038fc9fe34a)): no fnn panicked (`panicked at` in its log), and after the probe every fnn is running, answers `node_info` and can still write to its store (`new_invoice`, an invoice that is never paid). A node whose `node_info` answers but whose `new_invoice` doesn't is alive but not working. The run also fails if the kernel killed a process in a node's container for memory, or if a restart by the harness found fnn already gone although nothing had stopped it (it exited on its own, and the restart would otherwise hide that). The `:health` result lists each node's ERROR lines by kind, how many times fnn started, and its peak memory.

With `--nonce-check` and an instrumented fnn, it also checks that no MuSig2 nonce signed two different contexts, across restarts (see [below](#musig2-nonce-reuse---nonce-check)).

With `--close`, every channel is asked to close cooperatively halfway through the faults (payments carry on), and the test instead checks that every channel ends `Closed`. This exercises the closing signatures, which the other workloads never reach.

With `--ledger`, it also checks that money adds up at every node, not only on every channel (see [below](#per-node-ledger---ledger)).

With `--availability-window`, it also checks that the payment success rate after healing is back to what it was before the first fault (see [below](#availability-after-healing---availability-window)).

## Faults

| Fault | How |
|---|---|
| `kill` | `SIGKILL` on one node or all nodes, then restart. A crash can land in the middle of a write. |
| `pause` | `SIGSTOP` / `SIGCONT` on one node, by the pid in fnn's pidfile. The peer stays connected but gets no replies. The nemesis op records each node's process state afterwards (`T` = stopped). Before 2026-11-05 this fault stopped nothing on the node images, so earlier pause results are runs without a fault (see [FINDINGS.md](FINDINGS.md#reproducibility)). |
| `partition` | iptables drops traffic between one node and the others. |
| `disconnect` | `disconnect_peer` over RPC closes the connection between two neighbours; a later `connect_peer` reconnects them. Channels reestablish while both processes keep running, which a partition rarely causes (TCP keeps the connection and retransmits). |
| `packet` | tc netem delays traffic, and with `--packet-loss` drops some of it: to and from one node, between all nodes, or from the nodes to the CKB node (`--packet-targets one,all,chain`). Each fault draws its delay from `--packet-delay MIN-MAX` (ms; default 500-3000), with a jitter of a tenth of it. One shaping is active at a time. |
| `disk` | Fills the filesystem that holds one node's store (RocksDB), then frees it. With this fault each node's store is on a 256 MiB tmpfs of its own (`docker/docker-compose.yml`), so a fill can't touch the host's disk; fnn's log is not on it. Before freeing, the nemesis samples the node's health and reads what fnn logged since the fill; 10 s after, it samples again, and restarts fnn if it isn't healthy. The `disk` check passes a node that stopped or answered with an error while full and is healthy again after the space came back (by itself, or after a restart if it had stopped); it fails a node that was alive but didn't answer (`hung`, `unresponsive`) or still didn't work once space was freed. Scenario `c-13`. |
| `power` | Power loss: SIGKILL fnn on one node or every node (`--power-targets one,all`) and drop every write to its store that wasn't fsynced, then start fnn again. The store is on [LazyFS](https://github.com/dsrhaslab/lazyfs) (`jepsen.lazyfs`), which keeps writes in a cache of its own until fsync; `scripts/run.sh` builds it into the node image and gives the node containers FUSE (`docker/docker-compose.lazyfs.yml`) when its arguments mention `power`. Run it with `--store-sync-writes true` and `false` (Fiber's `fiber.store_sync_writes`, #1703: in Fiber from `4777d5d6`, off by default). The `power` check reports the mode each node logged, the power losses per node and the bytes each dropped, and fails if fnn didn't come back; lost state shows in the channel, ledger and health checks. |

Each fault is a namespace under `src/jepsen/fiber/faults/` that defines `fault`: the `--faults` names it handles, its nemesis package, and its operations, drawn from a random stream of its own so that the seed fixes them. `faults.clj` lists them; adding a fault is a new namespace and a line there, and changes no other fault's operations for a given seed (`test/jepsen/fiber/faults_test.clj` checks this).

Planned: **clock skew**. Fiber expires TLCs by wall-clock timestamp (off-chain checks use each node's local clock; on-chain claims use timestamp `since`), rather than by block height as in Lightning. Jepsen's built-in clock nemesis changes the host clock, which Docker containers share, so skew will be injected per process with libfaketime.

## Layout

- `docker/`: a CKB devnet (`chain`), three fnn nodes (`n1`–`n3`) and the Jepsen control node, on a private network `10.77.0.0/24`.
- `scripts/make-snapshot.sh`: creates the devnet with Fiber's own `tests/deploy/init-dev-chain.sh` (funded wallets, contracts, UDT), then stages it with the binaries into `docker/build/`. Every test starts from this snapshot. With `FNN_BIN` set empty it leaves out `bin/fnn`, which nothing else depends on; CI adds each commit's fnn afterwards.
- `src/jepsen/fiber/`:
  - `db.clj` (process lifecycle)
  - `topology.clj` (opens the channels of a named graph, `--topology`; `line` is n1 - n2 - n3)
  - `client.clj` (keysend payments, final read; workloads add payment kinds with `send-payment!` and operations with `invoke-op`)
  - `invoice.clj` (invoice payments, with `--pay-kinds invoice`)
  - `checker.clj` (invariants)
  - `health.clj` (process health: panics, and a sample of every node after healing)
  - `ledger.clj` (per-node money check, with `--ledger`)
  - `nonce.clj` (MuSig2 nonce reuse, with `--nonce-check`)
  - `faults.clj` and `faults/` (one namespace per fault: `process` for kill and pause, `partition`, `disconnect`, `packet`, `disk`, `power`)
  - `scenarios.clj` (named scenarios of the chaos-test plan, `--scenario`)
  - `schedule.clj` (payments, and the random streams faults draw from, fixed in advance by the run's seed)
- `instrument/`: the logging-only fnn instrumentation the nonce check needs.
- `.github/workflows/`: `ci.yml` (lint and unit tests), `smoke.yml` (a run without faults whenever the harness changes), `jepsen.yml` (any arguments against any Fiber commit; other repositories can call it, see [below](#in-another-repositorys-ci)) and `failure-summary.yml` (a Copilot note on failed manual runs, see [below](#a-note-on-failed-runs-copilot)); `.github/failure-summary/` has its digest script, prompt and known issues.

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
```

CI runs that last command on every pull request that touches the harness, and on `main` after a merge, against the Fiber commit pinned in `.github/workflows/smoke.yml`; fnn is built once per Fiber commit and cached, and its compiled dependencies are cached across commits, so a new commit compiles only Fiber's own crates. The devnet snapshot doesn't use fnn, so it is cached by the contents of Fiber's `tests/deploy` and `tests/nodes` (with the ckb version and the snapshot scripts): a commit that changes only Fiber's code reuses it. The smoke run is a check of this suite, not of Fiber, so it never injects faults. To run any arguments against another Fiber commit on a GitHub runner, start the Jepsen workflow (`jepsen.yml`) by hand from the Actions tab.

Results, including node logs and a timeline, are written to `store/`. To test another fnn build, copy it over `docker/build/bin/fnn`; `run.sh` rebuilds the node image.

The node image is Ubuntu 24.04 (glibc 2.39), the system of the GitHub `ubuntu-24.04` runners on which both Fiber's CI and this repository's CI build fnn. An fnn built on a system with glibc 2.39 or older runs in it: one from Fiber's Docker image (built on Debian 12) or a portable release package (built on Ubuntu 22.04) does too. One linked against a newer glibc doesn't start, and Jepsen then reports only that the RPC never came up. `NODE_BASE=debian:bookworm-slim scripts/run.sh ...` builds the nodes on another base.

`scripts/stall_report.py store/<test>/<run> ...` lists, for each channel with a problem, whether it is stalled on an unbalanced revocation nonce ledger ([#1561](https://github.com/nervosnetwork/fiber/issues/1561)), closed, or fine but still holding TLCs. It needs channel debug logs: add `--log-level 'info,fnn::fiber::channel=debug'` to the run.

### Scenarios of the chaos-test plan (`--scenario`)

The team's chaos-test plan ([C-01a to C-16](https://gist.github.com/quake/c73bba93b5b1928391063038fc9fe34a)) names each scenario by its topology, workload and fault. `src/jepsen/fiber/scenarios.clj` lists the ones this suite covers, each with the arguments that run it, its priority in the plan, and where it departs from the plan (three nodes in a line instead of A-B, for example). `--scenario NAME` puts those arguments right after the option, so arguments after it override them:

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

The result is invalid when a node's change differs from the expected one, when the status the client saw when a payment finished differs from the sender's final status (or the client was told no payment was created and one succeeded), or when one attempt produced two payments. A difference is `:unknown` rather than invalid when the final records can't show every settlement: a channel that is closed, missing or still holds TLCs, a payment that hasn't finished, or a payment the sender no longer lists. The check doesn't read the chain, so settlement after a force close is not covered. `repeat.sh` prints the outcome (`balanced`, `unbalanced`, `client-mismatch` or `incomplete`); `test/jepsen/fiber/ledger_test.clj` covers the rules.

The option is off by default, so runs without it send the same requests to the nodes and run the same checks as before.

### Availability after healing (`--availability-window`)

Assertion 5 of the chaos-test plan says that once the faults are gone, payments succeed as often as before. With `--availability-window SECONDS`, the test sends payments for that many seconds before the fault window, with no faults (the baseline), and again after healing and `--quiesce` (the recovery), at `--rate`. The `:availability` checker compares the two windows' success rates: the share of payments that ended `Success`, where a payment the node failed, refused or didn't answer in time counts against it. It is valid when the recovery's rate is at most `--availability-tolerance` percentage points (default 5) below the baseline's, and `:unknown` when either window has fewer than 10 payments.

```bash
scripts/run.sh --time-limit 120 --faults kill --kill-targets one --availability-window 30
```

The verdict uses only the success rate. Each window also reports successful payments per second and their latency (median, 95th percentile, maximum), which depend on the machine, so they are only reported, and the unsuccessful payments by sender and by error (numbers taken out, so errors of one kind count together). Not every failure is a fault: on `line` the acceptor of each channel starts with nothing on it, so n3 can't pay anyone until it has been paid, and in a run without faults most failures are n3's payments with "no path found" or "Insufficient balance". Such failures fall on both windows, and the tolerance has to absorb how they differ. The recovery starts after `--quiesce` rather than right after healing, so that restarted nodes are up and channels have reestablished: the check asks whether the network recovered, not how long that took. Both windows draw their payments from streams of their own, so the fault window's payments and faults for a seed are the same with or without the option, and their payments go through every other check (`--ledger` included). The option does nothing with `--close`, after which every channel is closed. `repeat.sh` and the CI summary print the two rates, baseline first.

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

### A note on failed runs (Copilot)

When a Jepsen run started by hand from the Actions tab (later, a scheduled one) fails, the Failure summary workflow (`failure-summary.yml`) digests the store of each failed job, keeping the checkers' output, every close, force-close and panic line and the most frequent warnings, and asks Copilot CLI which checker failed, what the likely cause is, which known issue it looks like and what would confirm it. The note goes to that workflow's job summary, linked to the failed run, and is marked as auto-generated and not verified: the verdicts are the checkers'. It runs with the workflow's own token and is billed to the repository owner's Copilot plan (a report has cost about 0.5 AI credits), never for pull requests, and not for calls from other repositories, which keep only read access. To summarize an earlier failed run, start the workflow by hand with that run's ID. The known issues the model is given are in `.github/failure-summary/known-issues.md`; update them with `FINDINGS.md`.

### Mixed versions, and a tally over several batches

To run n1 on a different fnn build than n2 and n3, copy that build to `docker/build/bin/fnn-n1` and point `COMPOSE_FILE` at the override:

```bash
COMPOSE_FILE=docker-compose.yml:docker-compose.mixed.yml   scripts/repeat.sh 4 --time-limit 60 --faults kill --kill-targets all --rate 20 --nemesis-interval 8   --quiesce 60 --log-level 'info,fnn::fiber::channel=debug'
```

`scripts/jepsen_tally.py <repeat-log> store` summarises several `repeat.sh` batches from one log, each introduced by a line `== <label> (HH:MM UTC)`. Per batch it counts the runs the checker passed, the runs with a stalled or a closed channel, the runs with a ready channel that still holds a TLC, how often the re-sync completion ran, and bad signatures. Run it where the store directories can be read as written (it calls `stall_report.py`).
