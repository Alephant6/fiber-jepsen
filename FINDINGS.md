# Findings

1. [A crash during in-flight payments can wedge channels permanently](#1-a-crash-during-in-flight-payments-can-wedge-channels-permanently-revocation-nonce-deadlock) (#1561), on develop `ae6f7d34`.
2. [Deferred TLC updates survive a disconnect](#2-deferred-tlc-updates-survive-a-disconnect-1689) (#1689, fixed by #1700). Found with a schedule explorer, not with this suite.
3. [Channel V2 under the same faults](#3-channel-v2-under-the-same-faults-1677--393b77fc) (#1677 @ `393b77fc`): no nonce reuse and no stalls, but the 30-second reply deadline force-closed channels whose peer was still there.

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
- **No loss was seen, but it isn't ruled out.** The checker reported no balance mismatch, so as far as the channel records show this is a liveness failure. A force close should recover the locked funds, but the suite doesn't check on-chain settlement, so that is not verified.

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
| `partition` (iptables), one node isolated | ~10/s | 8 s | 1 of 6, a different failure (see the correction below) |

How the failing runs were confirmed:

- **Nonce state checked directly: one run** (`kill`, all nodes, debug logs, nodes left running for inspection; the case described above).
- **Same symptoms, nonce state not checked: the other kill failures.** Those runs used info-level logs, which don't record nonce state. The symptoms were pending TLCs on both ends of the affected channels after the quiet period, and, in almost every case, no successful payment after the last restart.
- **Correction (2026-10-01): the failing partition run was a different failure.** In that run (`fiber-partition/2026-09-30T01:29:33`, info-level logs), n3's existing peer-response check (`peer_does_not_reply_ack_in_time`, no reply for more than 30 s) force-closed channel n2-n3, and channel n1-n2 was left with TLCs pending on both ends, most likely waiting on the closed channel (not verified). No ready channel reestablished during the run, so the crossing described above can't have happened. An earlier version of this file counted it as the same failure.

`WaitingTlcAck` rejections are **not** a signal on their own. Passing runs log them too: a channel rejects new TLCs briefly while a commitment round is in flight.

Since these runs, the checker also sends a fresh payment each way over every channel after the quiet period (`:probe-failures`), so a wedged network is reported directly.

## 2. Deferred TLC updates survive a disconnect (#1689)

**Found with** a randomized schedule explorer (two real nodes, random payments, restarts and disconnects, with and without message loss), which is not part of this repository.

**Issue and fix:** [nervosnetwork/fiber#1689](https://github.com/nervosnetwork/fiber/issues/1689); fixed by [#1700](https://github.com/nervosnetwork/fiber/pull/1700), merged on 2026-10-19.

During a replay after a reconnect, a channel defers the peer's `AddTlc` and `RemoveTlc` and applies them just before it verifies the peer's next `CommitmentSigned`. The deferral flag and queue weren't cleared when the connection dropped. If it dropped between a deferred update and its `CommitmentSigned`, the peer replayed the update after reconnecting, the queue held it twice, and applying the second copy failed. That dropped the `CommitmentSigned`, so no `RevokeAndAck` went back, and the channel usually force-closed with `Musig2VerifyError(BadSignature)` or one side waited for an ack forever. On develop (`2ba4b25`), 18 of 6,000 random schedules ended this way; with the fix, none did.

## 3. Channel V2 under the same faults (#1677 @ `393b77fc`)

**Tested version:** [#1677](https://github.com/nervosnetwork/fiber/pull/1677) at `393b77fc`, with logging-only hooks for the nonce check (`--nonce-check`); develop at `2ba4b255` as the control. Three nodes in a line, about 60 s of faults per run (3 minutes in one batch), 2026-10-09 to 10-11. The full table is in the [review on #1677](https://github.com/nervosnetwork/fiber/pull/1677#pullrequestreview-5486167720). Nothing has been rerun on later heads.

- **No nonce reuse.** Of 919,782 V2 partial signatures (83 Jepsen runs, 6,000 randomized crash and reconnect schedules, and the unit tests), no nonce signed two different contexts. The check isn't vacuous: a build that plants one reuse per restart fails a single run with 15 violations. Only hashes of public nonces and signing contexts are recorded, never secret nonces. Power loss is not covered: `SIGKILL` keeps the page cache, and at `393b77fc` fnn's RocksDB writes were not synced.
- **No stalls.** Under the same 7 fault types, channels stalled on the revocation-nonce ledger (#1561) in 19 of 50 develop runs and in 0 of 72 V2 runs.
- **Deadline force closes.** 15 of the 72 V2 runs force-closed a channel through the 30-second peer-response deadline (`PEER_CHANNEL_RESPONSE_TIMEOUT`): kill all nodes 2 of 10, kill all nodes with 3 minutes of faults 1 of 4, partition 6 of 18, mixed faults 6 of 10. The deadline was counted from the persisted `peer_wait_started_at`, so a node's own downtime counted: after a kill, the restarted node closed the channel right after starting, while its peer had the channel ready again almost at once. Under partitions, one end was cut off for most of the 30 s. The review asked whether counting the node's own downtime was intended; quake then changed it in `c817e616` (a restart resets outstanding peer-response timers, a reconnect within the same process keeps the original deadline). The close rate after that change has not been measured.
