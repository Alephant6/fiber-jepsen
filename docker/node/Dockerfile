# An fnn node that Jepsen drives over SSH. All three node directories are baked in;
# node nN runs out of /fiber/tests/nodes/N (the same layout as Fiber's tests/).
#
# The base is the Ubuntu release of GitHub's ubuntu-24.04 runners, which Fiber's
# CI and ours build fnn on, so a binary built there runs here. glibc is backward
# compatible, so builds from older systems run too: Fiber's Docker image builds
# on Debian 12, its portable release packages on Ubuntu 22.04. NODE_BASE picks
# another base, e.g. NODE_BASE=debian:bookworm-slim scripts/run.sh ...
ARG NODE_BASE=ubuntu:24.04
# LAZYFS=build puts LazyFS (for --faults power, jepsen.lazyfs) at /opt/jepsen/lazyfs,
# where jepsen.lazyfs runs it from; docker-compose.lazyfs.yml sets it. The default
# skips building it.
ARG LAZYFS=none

FROM ${NODE_BASE} AS lazyfs-none
RUN mkdir -p /opt/jepsen

# LazyFS 0.3.0, the version jepsen.lazyfs (Jepsen 0.3.14) installs. Its build
# fetches spdlog and toml11.
FROM ${NODE_BASE} AS lazyfs-build
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      g++ make cmake pkg-config libfuse3-dev git ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch 0.3.0 https://github.com/dsrhaslab/lazyfs.git /opt/jepsen/lazyfs \
 && cd /opt/jepsen/lazyfs/libs/libpcache && ./build.sh \
 && cd /opt/jepsen/lazyfs/lazyfs && ./build.sh

FROM lazyfs-${LAZYFS} AS lazyfs

FROM ${NODE_BASE}

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openssh-server sudo iproute2 iptables iputils-ping procps psmisc curl \
      ca-certificates netcat-openbsd faketime fuse3 \
 && rm -rf /var/lib/apt/lists/* \
 && { [ -e /usr/bin/fusermount ] || ln -s fusermount3 /usr/bin/fusermount; } \
 && mkdir -p /run/sshd /root/.ssh \
 && chmod 700 /root/.ssh \
 && sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config

COPY --from=lazyfs /opt/jepsen /opt/jepsen
COPY build/bin/fnn /usr/local/bin/fnn
COPY build/chain/tests/deploy/contracts /fiber/tests/deploy/contracts
COPY build/nodes /fiber/tests/nodes
COPY build/ssh/id_rsa.pub /root/.ssh/authorized_keys
RUN chmod 600 /root/.ssh/authorized_keys

EXPOSE 22 8227 8228
CMD ["/usr/sbin/sshd", "-D", "-e"]
