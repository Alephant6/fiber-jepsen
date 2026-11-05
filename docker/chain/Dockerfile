# The CKB devnet from the snapshot: a node with the indexer, plus a miner that
# produces one block per second. Recreating the container resets the chain.

# clockshift.c lets the outage fault (jepsen.fiber.faults.outage) move the CKB
# node's clock forward; entrypoint.sh preloads it.
FROM debian:bookworm-slim AS clockshift
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc libc6-dev \
 && rm -rf /var/lib/apt/lists/*
COPY chain/clockshift.c /src/clockshift.c
RUN gcc -O2 -shared -fPIC -Wall -Wextra -Werror -o /libclockshift.so /src/clockshift.c -ldl

FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=clockshift /libclockshift.so /usr/local/lib/libclockshift.so
COPY build/bin/ckb /usr/local/bin/ckb
COPY build/chain/tests /fiber/tests
COPY chain/entrypoint.sh /entrypoint.sh

EXPOSE 8114
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
