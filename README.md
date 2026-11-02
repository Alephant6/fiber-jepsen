# fiber-jepsen

[Jepsen](https://github.com/jepsen-io/jepsen) tests for [Fiber](https://github.com/nervosnetwork/fiber), the payment channel network on Nervos CKB.

Fiber's unit tests drive nodes in-process, and its integration tests replay scripted scenarios. This suite does something different. It runs real `fnn` binaries on separate hosts and sends concurrent payments through them while injecting faults: crashes, pauses and network partitions. It then checks invariants over the final state of every node.

**Status: early work in progress.** The first milestone is one end-to-end run (payments, `kill -9`, invariant check) against a CKB devnet.

## What it checks

After all faults are healed and the network has had time to converge:

- every channel opened during setup is still `ChannelReady`, with no pending TLCs;
- both ends of a channel agree on its balances (A's local balance equals B's remote balance);
- no channel's total (local + remote + in-flight TLCs) changed: payments move money within a channel, they never create or destroy it;
- every payment reached `Success` or `Failed`;
- no payment the client saw succeed reports a different status later.

With `--nonce-check` and an instrumented fnn, it also checks that no MuSig2 nonce signed two different contexts, across restarts (see [below](#musig2-nonce-reuse---nonce-check)).

With `--close`, every channel is asked to close cooperatively halfway through the faults (payments carry on), and the test instead checks that every channel ends `Closed`. This exercises the closing signatures, which the other workloads never reach.

## Faults

| Fault | How |
|---|---|
| `kill` | `SIGKILL` on one node or all nodes, then restart. A crash can land in the middle of a write. |
| `pause` | `SIGSTOP` / `SIGCONT` on one node. The peer stays connected but gets no replies. |
| `partition` | iptables drops traffic between one node and the others. |
| `disconnect` | `disconnect_peer` over RPC closes the connection between two neighbours; a later `connect_peer` reconnects them. Channels reestablish while both processes keep running, which a partition rarely causes (TCP keeps the connection and retransmits). |

Planned: **clock skew**. Fiber expires TLCs by wall-clock timestamp (off-chain checks use each node's local clock; on-chain claims use timestamp `since`), rather than by block height as in Lightning. Jepsen's built-in clock nemesis changes the host clock, which Docker containers share, so skew will be injected per process with libfaketime.

## Layout

- `docker/`: a CKB devnet (`chain`), three fnn nodes (`n1`–`n3`) and the Jepsen control node, on a private network `10.77.0.0/24`.
- `scripts/make-snapshot.sh`: creates the devnet with Fiber's own `tests/deploy/init-dev-chain.sh` (funded wallets, contracts, UDT), then stages it with the binaries into `docker/build/`. Every test starts from this snapshot.
- `src/jepsen/fiber/`:
  - `db.clj` (process lifecycle)
  - `topology.clj` (opens channels n1 - n2 - n3)
  - `client.clj` (keysend payments, final read)
  - `checker.clj` (invariants)
  - `nonce.clj` (MuSig2 nonce reuse, with `--nonce-check`)
  - `nemesis.clj` (the `disconnect` fault)
- `instrument/`: the logging-only fnn instrumentation the nonce check needs.

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

# several runs, one summary line each: verdict, post-heal probe, channel states, store directory
scripts/repeat.sh 4 --time-limit 60 --faults kill --kill-targets all --rate 20 --nemesis-interval 8 --quiesce 60
```

Results, including node logs and a timeline, are written to `store/`. To test another fnn build, copy it over `docker/build/bin/fnn`; `run.sh` rebuilds the node image.

`scripts/stall_report.py store/<test>/<run> ...` lists, for each channel with a problem, whether it is stalled on an unbalanced revocation nonce ledger ([#1561](https://github.com/nervosnetwork/fiber/issues/1561)), closed, or fine but still holding TLCs. It needs channel debug logs: add `--log-level 'info,fnn::fiber::channel=debug'` to the run.

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

### Mixed versions, and a tally over several batches

To run n1 on a different fnn build than n2 and n3, copy that build to `docker/build/bin/fnn-n1` and point `COMPOSE_FILE` at the override:

```bash
COMPOSE_FILE=docker-compose.yml:docker-compose.mixed.yml   scripts/repeat.sh 4 --time-limit 60 --faults kill --kill-targets all --rate 20 --nemesis-interval 8   --quiesce 60 --log-level 'info,fnn::fiber::channel=debug'
```

`scripts/jepsen_tally.py <repeat-log> store` summarises several `repeat.sh` batches from one log, each introduced by a line `== <label> (HH:MM UTC)`. Per batch it counts the runs the checker passed, the runs with a stalled or a closed channel, the runs with a ready channel that still holds a TLC, how often the re-sync completion ran, and bad signatures. Run it where the store directories can be read as written (it calls `stall_report.py`).
