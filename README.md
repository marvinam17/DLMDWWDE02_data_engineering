# DLMDWWDE02_data_engineering

## DATA Description:
https://isabel.dwd.de/DWD/klima/national/kldaten/formate_mq.html
Invalid values: -999

## How Kafka listeners work:
- on the local machine: localhost:9092
- in the docker network: kafka:29092

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /src/streaming/read.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 /src/streaming/read.py


kafka-console-consumer --topic temperature --from-beginning --bootstrap-server kafka:29092



spark-submit --jars jars/kafka-clients-3.4.0.jar,jars/spark-sql-kafka-0-10_2.12-3.3.0.jar,jars/spark-streaming-kafka-0-10-assembly_2.12-3.3.0.jar,jars/commons-pool2-2.11.1.jar,jars/spark-cassandra-connector_2.12-3.3.0.jar,jars/jsr166e-1.1.0.jar,jars/spark-cassandra-connector-assembly_2.12-3.3.0.jar,jars/mysql-connector-java-8.0.28.jar /src/streaming/read.py



spark-submit --master spark://$SPARK_MASTER_HOST:7077 --driver-class-path driver --jars driver/spark-sql-kafka-0-10_2.12-3.3.0.jar,driver/spark-cassandra-connector_2.12-3.3.0.jar,driver/postgresql-42.7.1.jar,driver/kafka-clients-3.4.0.jar,driver/commons-pool2-2.11.1.jar,driver/jsr166e-1.1.0.jar,driver/spark-cassandra-connector-assembly_2.12-3.3.0.jar,driver/spark-streaming-kafka-0-10-assembly_2.12-3.3.0.jar main.py

Works:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,org.postgresql:postgresql:42.2.18 /src/main.py



old:
docker-compose exec spark sh -c "SPARK_MASTER_HOST=`hostname`; spark-submit --master spark://$SPARK_MASTER_HOST:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,org.postgresql:postgresql:42.2.18 /src/main.py"