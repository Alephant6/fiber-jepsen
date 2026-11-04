# Known issues (summarized from this suite's FINDINGS.md)

<!-- The failure summary gives this list to the model. When a finding in FINDINGS.md is added or corrected, update it here too. -->

1. #1561, revocation-nonce deadlock after a crash during in-flight payments (develop ae6f7d34). End state: after the quiet period, channels stay ChannelReady with pending TLCs on both ends; new payments stay Created; the first hop rejects TLCs with "Generating TlcErr from error WaitingTlcAck to error_code TemporaryChannelFailure"; the post-heal probe fails. waiting_ack is false on both sides, so the peer-response timeout never fires and nothing force-closes. Confirming it needs channel debug logs (nonce state); with info-level logs, call it "same symptoms as #1561, nonce state not checked".
2. Peer-response force close. CheckActiveChannel's peer_does_not_reply_ack_in_time (no reply for more than 30 s) logs "Channel ... is inactive for a time, shutting down it forcefully" and force-closes the channel. Seen in a partition run; another channel can be left with pending TLCs waiting on the closed one. This is a different failure from #1561.
3. WaitingTlcAck rejections are not a signal on their own: passing runs log them too, while a commitment round is in flight.

