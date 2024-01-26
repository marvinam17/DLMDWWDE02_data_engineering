FROM docker.io/bitnami/cassandra:latest

ENV LANG C.UTF-8

COPY *.cql /docker-entrypoint-initdb.d/

# COPY entrypoint.sh /

# ENTRYPOINT ["/entrypoint.sh"]
# CMD ["cassandra", "-f"]