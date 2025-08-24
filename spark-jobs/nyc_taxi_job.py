from math import log
from psycopg2 import pool
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import yaml
import logging



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
with open('/config/cdc_config.yml', 'r') as file:
    config = yaml.safe_load(file)

kafka_conf  = config['kafka']
postgres_conf = config['postgres']
spark_conf = config['spark']
schema_conf = config['schema_nyc_taxi']


postgeres_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=20,
    dsn=postgres_conf['url'].replace("jdbc:", ""),
    user=postgres_conf['user'],
    password=postgres_conf['password']
)