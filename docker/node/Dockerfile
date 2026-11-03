# An fnn node that Jepsen drives over SSH. All three node directories are baked in;
# node nN runs out of /fiber/tests/nodes/N (the same layout as Fiber's tests/).
#
# The base is the Ubuntu release of GitHub's ubuntu-24.04 runners, which Fiber's
# CI and ours build fnn on, so a binary built there runs here. glibc is backward
# compatible, so builds from older systems run too: Fiber's Docker image builds
# on Debian 12, its portable release packages on Ubuntu 22.04. NODE_BASE picks
# another base, e.g. NODE_BASE=debian:bookworm-slim scripts/run.sh ...
ARG NODE_BASE=ubuntu:24.04
FROM ${NODE_BASE}

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openssh-server sudo iproute2 iptables iputils-ping procps psmisc curl \
      ca-certificates netcat-openbsd faketime \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /run/sshd /root/.ssh \
 && chmod 700 /root/.ssh \
 && sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config

COPY build/bin/fnn /usr/local/bin/fnn
COPY build/chain/tests/deploy/contracts /fiber/tests/deploy/contracts
COPY build/nodes /fiber/tests/nodes
COPY build/ssh/id_rsa.pub /root/.ssh/authorized_keys
RUN chmod 600 /root/.ssh/authorized_keys

EXPOSE 22 8227 8228
CMD ["/usr/sbin/sshd", "-D", "-e"]
