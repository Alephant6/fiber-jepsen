# Findings

## 1. A crash during in-flight payments can wedge channels permanently (revocation-nonce deadlock)

**Tested version:** fnn `develop @ ae6f7d34` (v0.10.0-rc1, 2026-09-29), ckb 0.202.0, topology n1 - n2 - n3.

**Known issue:** [nervosnetwork/fiber#1561](https://github.com/nervosnetwork/fiber/issues/1561) reports the same end state, and [#1584](https://github.com/nervosnetwork/fiber/issues/1584) reports the same mechanism. The fix for #1584 ([#1590](https://github.com/nervosnetwork/fiber/pull/1590)) was reverted in [#1628](https://github.com/nervosnetwork/fiber/pull/1628) as not covering all reconnect scenarios, and its regression test is `#[ignore]`d.

**What these runs add:**
- The deadlock is reachable through an ordinary process crash and restart. No message has to be dropped or delayed.
- In a line of channels, both channels end up wedged, and every payment in the network stops.

### What happened

Keysend payments ran at about 5/s while the nemesis sent `SIGKILL` to all three `fnn` processes and restarted them. Two payments were in flight when the nodes were killed.

After the final restart, and 120 s later, the checker reported:
- a pending TLC on each channel;
- two payments stuck in `Inflight` (the in-flight ones);
- three payments stuck in `Created`, all created within 1–2 s of a restart.

We inspected the live nodes 3–6 minutes after the last restart:
- The chain was producing blocks.
- All peers were connected.
- Every node's graph had both channels.
- Both channels were `ChannelReady` and `enabled`.

But both channels were wedged. Each end of each channel logged, every 100 ms:

```
[ack] retryable_ops_blocked waiting_ack=false send_present=false verify_present=true next_present=true   (n1, channel n1-n2)
[ack] retryable_ops_blocked waiting_ack=false send_present=true verify_present=false next_present=true   (n2, channel n1-n2)
[ack] retryable_ops_blocked waiting_ack=false send_present=true verify_present=false next_present=true   (n2, channel n2-n3)
[ack] retryable_ops_blocked waiting_ack=false send_present=false verify_present=true next_present=true   (n3, channel n2-n3)
```

On each channel, one side is missing its revocation *send* nonce and the other its *verify* nonce. That is the two-sided state described in #1561. Because `waiting_ack` is false on both sides, the peer-response timeout never fires.

### Consequences

- **No new payment from any node leaves `Created`.** The first hop rejects the TLC: `Generating TlcErr from error WaitingTlcAck to error_code TemporaryChannelFailure`. The payment is never failed back to the caller; the periodic check only logs `Payment ... is still not final after periodic check, maybe the channel is down. Status: Created`.
- **TLCs that were in flight at the crash stay `Committed` on both ends.** Neither the payee (for a direct keysend) nor the forwarding node (for n3 → n1) settles or forwards them. Their expiry is about 20 h after creation, so the funds stay locked until then.
- **Funds are not lost.** A force close recovers them. This is a liveness failure.

### How the crash leads there (n1's log, channel n1-n2)

1. Just before the kill, n1 had sent a `CommitmentSigned` and was waiting for n2's `RevokeAndAck` (`set_waiting_ack(true)`, all nonce slots present).
2. After the restart, reestablishment replays a few commitment rounds.
3. The last step is n1 sending a `RevokeAndAck` that consumes its send nonce (`send_revoke_and_ack_message ... send_present=false`). No reciprocal `CommitmentSigned` follows, because that round carried no TLC update.
4. From then on, n1's retry queue is blocked with `send_present=false` and n2's with `verify_present=false`.

This matches the root cause described in #1590 for #1584 (the post-ACK path in which the reciprocal commitment isn't sent). #1628 reverted that change.

### Reproducing

```bash
scripts/run.sh --time-limit 60 --faults kill --kill-targets all --quiesce 120 --leave-db-running \
  --log-level 'info,fnn::fiber::payment=debug,fnn::fiber::channel=debug,fnn::fiber::network=debug'
```

### Reproducibility

All runs lasted 60 s on fnn `develop @ ae6f7d34`. A run "fails" when, after healing and a quiet period, the checker finds pending TLCs, unfinished payments or balance mismatches.

| Faults | Payments | Fault interval | Failing runs |
|---|---|---|---|
| `kill` (one or all nodes) | ~5/s | 15 s | 2 of 6 |
| `kill`, all nodes | ~20/s | 8 s | **4 of 4** |
| `kill`, one node at a time | ~20/s | 8 s | **4 of 4** |
| `pause` (SIGSTOP), one node | ~10/s | 8 s | 0 of 2 |
| `partition` (iptables), one node isolated | ~10/s | 8 s | 1 of 6 |

How the failing runs were confirmed:

- **Nonce state checked directly: one run** (`kill`, all nodes, debug logs, nodes left running for inspection; the case described above).
- **Same symptoms, nonce state not checked: the other kill failures and the partition failure.** Those runs used info-level logs, which don't record nonce state. The symptoms were pending TLCs on both ends of the affected channels after the quiet period, and, in almost every case, no successful payment after the last restart. In the failing partition run, only 1 of about 600 payments succeeded.

`WaitingTlcAck` rejections are **not** a signal on their own. Passing runs log them too: a channel rejects new TLCs briefly while a commitment round is in flight.

Since these runs, the checker also sends a fresh payment each way over every channel after the quiet period (`:probe-failures`), so a wedged network is reported directly.
