FROM postgres:18.6-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && pip3 install --no-cache-dir --break-system-packages "patroni[kubernetes,psycopg3]==4.1.5" \
    && rm -rf /var/lib/apt/lists/*

COPY bootstrap.sh /usr/local/bin/bootstrap-netflow.sh
RUN chmod 0755 /usr/local/bin/bootstrap-netflow.sh

ENV MALLOC_ARENA_MAX=1

USER postgres
WORKDIR /var/lib/postgresql

STOPSIGNAL SIGTERM
ENTRYPOINT ["patroni"]
CMD ["/etc/patroni/patroni.yml"]
