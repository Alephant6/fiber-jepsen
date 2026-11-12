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
# SLOWIO=build puts unreliablefs (for --faults slow-io, jepsen.fiber.slowfs) at
# /opt/unreliablefs; docker-compose.slowio.yml sets it. The default skips building it.
ARG SLOWIO=none

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

FROM ${NODE_BASE} AS slowio-none
RUN mkdir -p /opt/unreliablefs

# unreliablefs at a fixed commit of its master branch (2024-04-02, newer than its
# release 0.2.0). It is built against FUSE 2, and the final image has only FUSE 3,
# so the library it loads is copied beside it; jepsen.fiber.slowfs starts it with
# LD_LIBRARY_PATH=/opt/unreliablefs.
FROM ${NODE_BASE} AS slowio-build
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      gcc g++ make cmake pkg-config libfuse-dev git ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/ligurio/unreliablefs.git /tmp/unreliablefs \
 && cd /tmp/unreliablefs \
 && git checkout --detach fa432252c117e82a0a36343895936f4fae246b56 \
 && cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
 && cmake --build build --target unreliablefs \
 && mkdir -p /opt/unreliablefs \
 && cp build/unreliablefs/unreliablefs /opt/unreliablefs/ \
 && cp -L "$(ldd build/unreliablefs/unreliablefs | awk '/libfuse/ {print $3}')" /opt/unreliablefs/ \
 && ls -l /opt/unreliablefs/unreliablefs /opt/unreliablefs/libfuse.so.2

FROM slowio-${SLOWIO} AS slowio

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

# The clock fault (jepsen.fiber.faults.clock) preloads libfaketime into fnn at a
# fixed path, and starts it with no offset.
RUN ln -s "$(ls /usr/lib/*/faketime/libfaketimeMT.so.1)" /usr/local/lib/libfaketimeMT.so.1 \
 && mkdir -p /fiber && echo +0 >/fiber/faketime

COPY --from=lazyfs /opt/jepsen /opt/jepsen
COPY --from=slowio /opt/unreliablefs /opt/unreliablefs
COPY build/bin/fnn /usr/local/bin/fnn
COPY build/chain/tests/deploy/contracts /fiber/tests/deploy/contracts
COPY build/nodes /fiber/tests/nodes
COPY build/ssh/id_rsa.pub /root/.ssh/authorized_keys
RUN chmod 600 /root/.ssh/authorized_keys

EXPOSE 22 8227 8228
CMD ["/usr/sbin/sshd", "-D", "-e"]
