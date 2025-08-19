#!/bin/bash
SPARK_RUN="project_cdc_mysql_to_postgres-spark-1"
docker exec -it $SPARK_RUN spark-submit \
  --jars /opt/bitnami/spark/jars/postgresql-42.7.3.jar,/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/bitnami/spark/jars/kafka-clients-3.5.1.jar,/opt/bitnami/spark/jars/spark-streaming-kafka-0-10_2.12-3.5.1.jar \
  /spark-jobs/spark_test.py
