docker-compose --file docker-compose-debug.yaml up -d --build


docker-compose exec spark sh -c "SPARK_MASTER_HOST=`hostname`; spark-submit --master spark://$SPARK_MASTER_HOST:7077 --deploy-mode client /src/main.py"