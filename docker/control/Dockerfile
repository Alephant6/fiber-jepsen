# The Jepsen control node. The project is bind-mounted at /jepsen.
FROM clojure:temurin-21-lein-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openssh-client gnuplot-nox git \
 && rm -rf /var/lib/apt/lists/*

COPY build/ssh/id_rsa /root/.ssh/id_rsa
RUN chmod 700 /root/.ssh && chmod 600 /root/.ssh/id_rsa

WORKDIR /jepsen
CMD ["sleep", "infinity"]
