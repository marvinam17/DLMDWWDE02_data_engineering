# DLMDWWDE02_data_engineering

## Architecture
![alt text](https://github.com/marvinam17/DLMDWWDE02_data_engineering/blob/main/architecture/architecture.png?raw=true)

## Project Description
The streaming data pipeline processes (dew point) temperature, air pressure and humidity data from the German Weather Service (Deutscher Wetterdienst - DWD) for a selected measuring station (can be selected via STATION_ID environment variable of producer service in docker-compose.yaml; respective station ids can be found here: https://www.dwd.de/DE/leistungen/klimadatendeutschland/stationsuebersicht.html) in order to simulate real-time sensors. A Kafka producer iteratively writes this data into two Kafka Topics (temperature / others) at a selectable speed (via STREAMING_SPEED environment_variable).\
The data is then aggregated using the distributed Spark Engine (streaming) and enriched with static information. The aggregated data streams are persisted in a Cassandra database, which ensures highly available data storage. Highly aggregated data is persisted in a Postgres instance to improve the data access performance (Queries of Superset via Presto to Cassandra are quiet slow due to the Cassandra Connector and in detail because aggregations are not done on Database level but by Presto).
Apache Superset is used to visualize the aggregated data. Since Superset does not directly support interaction with Cassandra, PrestoDB is used as a connector. The use of Cassandra as a data store justifies the use of an additional component such as PrestoDB. \
In order to realize maintainability and reproducibility, the application is based on a microservice architecture, which is implemented using Docker-Compose in a separate Docker network. As all the components used are distributed systems, horizontal scaling is possible and this ensures high availability of the overall system. In combination with tested code, the reliability of the application is achieved.\
To ensure data governance and data protection, (notionally) sensitive data, such as metadata from the measuring station, is only added during the aggregation process. Furthermore, access to the visualizations can be restricted by the integrated user management of Apache Superset.

## Dashboard Preview
![alt text](https://github.com/marvinam17/DLMDWWDE02_data_engineering/blob/main/architecture/dashboard_example.png?raw=true)

## Data Description
Reference: https://isabel.dwd.de/DWD/klima/national/kldaten/formate_mq.html \
Invalid values are marked by: -999.0

## Requirements:
Docker-Compose installed
- min 8 GB RAM available
- ~ 6 Cores available
- min 20 GB disk space available

## Start Command
`docker-compose up -d`
After pulling all images the startup takes around 90 seconds. Afterwards, the dashboard is accessible at http://localhost:8088. You can login with the Superset credentials that were set in .env file.
After that you can select the Weather Dashboard, go to settings and set the refresh intervall to 30 seconds. And then have a look at the Weather history of your choice.
The total time until completion takes around 20 minutes. 

## Usage in production environments
In the main directory there is a .env file that defines all user access values. These should be changed in a productive environment, as they are defined with default variables. In superset_config.py you should change the SECRET_KEY.
If you change the cassandra credentials in .env you have to adjust the credetials in db_connector/etc/catalog/cassandra.properties as well.
Furthermore, a user without admin rights should be created in Superset to ensure non root access to the Dashboard and its data. As all containers are in a separate "backend" network and no ports except supersets main port are exposed there is only on access point to the pipeline. 

## Execute the Tests
In order to execute the tests a new Spark container is created and the tests are executed using the following command:

`docker build -t spark-testing ./data_aggregation; docker run -it --rm --mount=type=bind,source=${PWD}/data_aggregation,target=/src spark-testing /bin/bash -c 'python -m pytest --doctest-modules /src'`

## What I learned during Implementation
 * Spark is very ressource hungry. Even if the ressources are not required. Therefore, it is a good idea to restrict the available ressources and review the performance. In my case the performance was almost equal but only half of the ressources were consumed.
 * Cluster Setups of Kafka, Spark, Presto and Cassandra in such big applications are not recommended on a regular PC (I tried it out and it crashed my Docker Kernel many times -> not enough RAM). They consume huge computational ressources. Therefore, I implemented standalone versions of all services.
 * The Presto Cassandra Connector is slow because all aggregations are done by Presto and not by Cassandra. This should be considered in future projects. Therefore, I added a Postgres Database to store highly aggregated data and enable an acceptable refresh time of the dashboard.
 * The order of service startup is very important. If Cassandra or Kafka are not available on Spark start-up it will crash. Therefore, I added healthchecks to ensure the correct startup order. 
 * Docker Networks are relatively easy to handle and offer a great service separation.
 * Spark requires drivers for the communication with Postgres, Cassandra and Kafka. Finding the correct version and the handling of that drivers is not intuitive from my perspective. I had to read much documentation and examples. In the implementation process I switched from a download on startup strategy to a local provision of the drivers.
 * SparkStreaming does not support Upsert Data Writing with all database drivers. Cassandra is supported but Postgres not. This was one reason why I persisted the daily aggregated data in Cassandra and highly aggregated data in Postgres. Even though one could overwrite the whole Postgres Table on each batch, this is a huge overhead and results in windows where no data is available in the visualization tool.
 * Superset does not support a dashboard import with database passwords via CLI. Therefore I had to use the Superset API. Which is pretty amazing to be honest. It provides full control over Superset.


## Open Questions
 * Why is Presto not accepting environment variables in the cassandra.properties file? According to the Presto Github Repo Discussions and also the Trino (was forked from Presto) docs it should work with `${ENV:VARIABLE_NAME}`. I tried many different references to the environment vars (`${ENV:VARIABLE_NAME}`, `${VARIABLE_NAME}`, ...) but it did not work in any of them. Therefore a manual adjustment of the cassandra-properties file is still necessary.

