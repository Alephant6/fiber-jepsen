You are reading the results of Jepsen tests of Fiber, a payment channel network on Nervos CKB. Each run starts three fnn nodes (n1 - n2 - n3, channels n1-n2 and n2-n3), sends keysend payments through them while injecting one kind of fault (kill = SIGKILL and restart, partition = iptables, pause = SIGSTOP, disconnect = disconnect_peer), heals, waits, then checks invariants: every channel ChannelReady with no pending TLCs, both ends agree on balances, channel totals unchanged, every payment Success or Failed, a post-heal probe payment each way succeeds, and (with --ledger) each node's balance change matches its payments.

After this prompt come the known issues of this suite, then a digest of one batch: the list of runs with their exit status, then, for each failed run, the start of results.edn (Jepsen's checker output; hashes shortened), each node's process start count, every distinct close / shutdown / panic log line with the time it first appeared, and each node's most frequent WARN/ERROR lines with counts.

Write a short report in English, in Markdown, for a Fiber developer:
1. One line per failed run: which checker failed and the concrete symptom (for example "n1-n2 channel ChannelReady with 2 pending TLCs on both ends; probe n2->n3 failed").
2. For each failed run, the most likely cause, citing the log lines that support it, and how confident you are (high / medium / low). Say which known issue it matches, or that it matches none. Matching symptoms is not proof: say what evidence would confirm it. Say plainly when the digest doesn't show enough to tell.
3. Whether the failed runs look like the same failure.
4. What to look at next (which file or log, which setting such as channel debug logging).

Do not invent facts that the digest does not show. Do not use any tools; everything you need is below. Keep the whole report under 350 words.

