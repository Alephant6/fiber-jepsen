# The CKB devnet from the snapshot: a node with the indexer, plus a miner that
# produces one block per second. Recreating the container resets the chain.
FROM debian:bookworm-slim

# libfaketime lets the outage fault (jepsen.fiber.faults.outage) move the CKB
# node's clock forward; entrypoint.sh preloads it from a fixed path.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl faketime \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(ls /usr/lib/*/faketime/libfaketimeMT.so.1)" /usr/local/lib/libfaketimeMT.so.1

COPY build/bin/ckb /usr/local/bin/ckb
COPY build/chain/tests /fiber/tests
COPY chain/entrypoint.sh /entrypoint.sh

EXPOSE 8114
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
