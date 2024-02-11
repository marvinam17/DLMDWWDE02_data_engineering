# DLMDWWDE02_data_engineering

## Architecture
![alt text](https://github.com/marvinam17/DLMDWWDE02_data_engineering/blob/main/architecture/architecture.png?raw=true)

## Project Description
The streaming data pipeline processes (dew point) temperature, air pressure and humidity data from the German Weather Service (Deutscher Wetterdienst - DWD) for a selected measuring station (can be selected via STATION_ID environment variable in docker-compose.yaml; respective station ids can be found here: https://www.dwd.de/DE/leistungen/klimadatendeutschland/stationsuebersicht.html) in order to simulate real-time sensors. A Kafka producer iteratively writes this data into Kafka Topics at a selectable speed.\
The data is then aggregated using the distributed Spark Engine (streaming) and enriched with static information. The aggregated data streams are persisted in a Cassandra database, which ensures highly available data storage. Highly aggregated data is persisted in a Postgres instance to improve the data access performance (Queries from Presto to Cassandra are quiet slow for small tables).
Apache Superset is used to visualize the aggregated data. Since Superset does not directly support interaction with Cassandra, PrestoDB is used as a connector. The use of Cassandra as a data store justifies the use of an additional component such as PrestoDB. \
In order to realize maintainability and reproducibility, the application is based on a microservice architecture, which is implemented using Docker-Compose in a separate Docker network. As all the components used are distributed systems, horizontal scaling is possible and this ensures high availability of the overall system. In combination with tested code, the reliability of the application is achieved.\
To ensure data governance and data protection, (notionally) sensitive data, such as metadata from the measuring station, is only added during the aggregation process. Furthermore, access to the visualizations is restricted by the integrated user management of Apache Superset.

## Data Description:
Reference: https://isabel.dwd.de/DWD/klima/national/kldaten/formate_mq.html \
Invalid values are marked by: -999.0

## Requirements:
Docker installed
- min 6 GB RAM available
- ~ 4 Cores available
- min 20 GB disk space available

## Start Command
`docker-compose up -d`

The startup takes around 90-120 seconds. Afterwards, the dashboard is accessible via http://localhost:8088. You can login with the Superset credentials that were set in .env file.
After that you can select the Weather Dashbaord, go to settings and set the refresh intervall to 30 seconds. And then have a look at the Weather history of your choice.
The total time until completion takes around 20 minutes. 

## Usage in production environments
In the main directory there is a .env file that defines all user access values. These should be changed in a productive environment, as they are defined with default variables.
Furthermore, a user without admin rights should be created in Superset to ensure non root access to the Dashboard and its data. As all containers are in a separate "backend" network and no ports except supersets main port are exposed there is only on access point to the pipeline. Furthermore, in superset_config.py you should change the SECRET_KEY

## Execute the Tests
In order to execute the tests a new Spark container is created and the tests are executed using the following command:
`docker build -t spark-testing ./data_aggregation; docker run -it --rm --mount=type=bind,source=${PWD}/data_aggregation,target=/src spark-testing /bin/bash -c 'python -m pytest --doctest-modules /src'`

