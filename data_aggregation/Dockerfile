FROM docker.io/bitnami/spark:3.3

ADD driver /opt/bitnami/spark/jars/

RUN python -m pip install --upgrade pip;\
    pip install pyspark==3.3.0;\
    pip install pytest;

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
ENV PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip