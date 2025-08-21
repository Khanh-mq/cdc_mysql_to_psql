#!/bin/bash
docker exec -it spark spark-submit \
  --jars /extra-jars/postgresql-42.7.3.jar,/extra-jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/extra-jars/kafka-clients-3.5.1.jar \
  /spark-jobs/spark_test.py
