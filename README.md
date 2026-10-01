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

## Faults

| Fault | How |
|---|---|
| `kill` | `SIGKILL` on one node or all nodes, then restart. A crash can land in the middle of a write. |
| `pause` | `SIGSTOP` / `SIGCONT` on one node. The peer stays connected but gets no replies. |
| `partition` | iptables drops traffic between one node and the others. |

Planned: **clock skew**. Fiber expires TLCs by wall-clock timestamp (off-chain checks use each node's local clock; on-chain claims use timestamp `since`), rather than by block height as in Lightning. Jepsen's built-in clock nemesis changes the host clock, which Docker containers share, so skew will be injected per process with libfaketime.

## Layout

- `docker/`: a CKB devnet (`chain`), three fnn nodes (`n1`–`n3`) and the Jepsen control node, on a private network `10.77.0.0/24`.
- `scripts/make-snapshot.sh`: creates the devnet with Fiber's own `tests/deploy/init-dev-chain.sh` (funded wallets, contracts, UDT), then stages it with the binaries into `docker/build/`. Every test starts from this snapshot.
- `src/jepsen/fiber/`:
  - `db.clj` (process lifecycle)
  - `topology.clj` (opens channels n1 - n2 - n3)
  - `client.clj` (keysend payments, final read)
  - `checker.clj` (invariants)

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

### Mixed versions, and a tally over several batches

To run n1 on a different fnn build than n2 and n3, copy that build to `docker/build/bin/fnn-n1` and point `COMPOSE_FILE` at the override:

```bash
COMPOSE_FILE=docker-compose.yml:docker-compose.mixed.yml   scripts/repeat.sh 4 --time-limit 60 --faults kill --kill-targets all --rate 20 --nemesis-interval 8   --quiesce 60 --log-level 'info,fnn::fiber::channel=debug'
```

`scripts/jepsen_tally.py <repeat-log> store` summarises several `repeat.sh` batches from one log, each introduced by a line `== <label> (HH:MM UTC)`. Per batch it counts the runs the checker passed, the runs with a stalled or a closed channel, the runs with a ready channel that still holds a TLC, how often the re-sync completion ran, and bad signatures. Run it where the store directories can be read as written (it calls `stall_report.py`).
