FROM docker.io/bitnami/spark:3.3

WORKDIR /src

CMD ["spark-submit","--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0", "read.py"]