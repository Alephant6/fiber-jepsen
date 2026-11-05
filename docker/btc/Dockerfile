# Bitcoin Core and LND for the cross-chain hub tests (docker-compose.cch.yml).
# One image, three containers: bitcoind (regtest), lnd-ingrid (the hub's LND,
# which CCH drives) and lnd-bob (the BTC user's). The containers only run sshd:
# Jepsen writes the configs and starts the daemons over SSH (jepsen.fiber.cch),
# as it does fnn on the node containers.
#
# The versions are the ones Fiber's CI runs its cross-chain-hub e2e tests with
# (.github/actions/setup-dependencies/action.yml); the checksums are from
# bitcoincore.org's SHA256SUMS and LND's release manifest.
FROM ubuntu:24.04

ARG BITCOIN_VERSION=27.0
ARG BITCOIN_SHA256=2a6974c5486f528793c79d42694b5987401e4a43c97f62b1383abf35bcee44a8
ARG LND_VERSION=v0.18.0-beta
ARG LND_SHA256=d1c4b43e6fb5bb523bc72cca3f06a12aee77bcf348c2a6a7e30e9d8318df25a9

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openssh-server sudo iproute2 iptables iputils-ping procps psmisc curl \
      ca-certificates netcat-openbsd \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /run/sshd /root/.ssh \
 && chmod 700 /root/.ssh \
 && sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config

RUN set -eu; cd /tmp \
 && curl -fsSL --retry 3 -o bitcoin.tgz \
      "https://bitcoincore.org/bin/bitcoin-core-${BITCOIN_VERSION}/bitcoin-${BITCOIN_VERSION}-x86_64-linux-gnu.tar.gz" \
 && echo "${BITCOIN_SHA256}  bitcoin.tgz" | sha256sum -c - \
 && tar -xzf bitcoin.tgz \
 && install "bitcoin-${BITCOIN_VERSION}/bin/bitcoind" "bitcoin-${BITCOIN_VERSION}/bin/bitcoin-cli" /usr/local/bin/ \
 && curl -fsSL --retry 3 -o lnd.tgz \
      "https://github.com/lightningnetwork/lnd/releases/download/${LND_VERSION}/lnd-linux-amd64-${LND_VERSION}.tar.gz" \
 && echo "${LND_SHA256}  lnd.tgz" | sha256sum -c - \
 && tar -xzf lnd.tgz \
 && install "lnd-linux-amd64-${LND_VERSION}/lnd" "lnd-linux-amd64-${LND_VERSION}/lncli" /usr/local/bin/ \
 && rm -rf /tmp/*

COPY build/ssh/id_rsa.pub /root/.ssh/authorized_keys
RUN chmod 600 /root/.ssh/authorized_keys

# bitcoind RPC and ZMQ; LND p2p, gRPC and REST.
EXPOSE 22 18443 28332 28333 9735 10009 8080
CMD ["/usr/sbin/sshd", "-D", "-e"]
