docker-compose up -d --build

docker-compose exec spark sh -c "SPARK_MASTER_HOST=`hostname`; spark-submit --master spark://$SPARK_MASTER_HOST:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 /src/main.py"