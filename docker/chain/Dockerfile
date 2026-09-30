# The CKB devnet from the snapshot: a node with the indexer, plus a miner that
# produces one block per second. Recreating the container resets the chain.
FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

COPY build/bin/ckb /usr/local/bin/ckb
COPY build/chain/tests /fiber/tests
COPY chain/entrypoint.sh /entrypoint.sh

EXPOSE 8114
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
