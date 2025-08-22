
from psycopg2 import pool
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import yaml
import logging
#------logging setup------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


#open config file 
with open('/config/cdc_config.yml', 'r') as file:
    config = yaml.safe_load(file)

kafka_conf  = config['kafka']
postgres_conf = config['postgres']
spark_conf = config['spark']
schema_conf = config['schema']


postgeres_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=20,
    dsn=postgres_conf['url'].replace("jdbc:", ""),
    user=postgres_conf['user'],
    password=postgres_conf['password']
)


# ---build schema dynamically from config---

type_map = {"IntegerType": IntegerType(),
            "StringType": StringType(),
            "TimestampType": TimestampType()}

fields =  [StructField(col['name'], type_map[col['type']]) for col in schema_conf['columns']]
schema =  StructType([
    StructField("before", StructType(fields), True),
    StructField("after", StructType(fields), True),
    StructField("op", StringType()),
    StructField("ts_ms", StringType())
])

spark = SparkSession.builder \
    .appName(spark_conf['appName']) \
    .master(spark_conf['master']) \
    .config("spark.executor.memory", spark_conf['executorMemory']) \
    .config("spark.executor.cores", spark_conf['exectutorCores']) \
    .config("spark.default.parallelism", spark_conf['defaultParallelism']) \
    .config('spark.sql.shuffle.partitions', spark_conf['shufflePartitions']) \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
    .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

logging.info("spark seeesion initialized")



# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("maxOffsetsPerTrigger", kafka_conf["maxOffsetsPerTrigger"]) \
    .option("minPartitions", kafka_conf['minPartitions']) \
    .option("kafka.bootstrap.servers", kafka_conf['bootstrap_servers']) \
    .option("subscribe", kafka_conf['topic']) \
    .option("startingOffsets", kafka_conf['startingOffsets']) \
    .load()

# Parse JSON from Kafka
df_json = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Process CDC operations
def process_batch(batch_df, batch_id):
    logging.info(f'processing batch {batch_id} with {batch_df.count()} records')

    # Handle INSERT + UPDATE
    inserts_updates = batch_df.filter(col("op").isin("c", "u")) \
        .select("after.*")

   
    inserts_updates.write \
            .format("jdbc") \
            .option("url", postgres_conf['url']) \
            .option("dbtable", postgres_conf['table']) \
            .option("user", postgres_conf['user']) \
            .option("password", postgres_conf['password']) \
            .option("driver", postgres_conf['driver']) \
            .option("batchsize", postgres_conf['batchSize']) \
            .mode("append") \
            .save()
    logging.info(f'Inserted/Updated {inserts_updates.count()} records')

    # Handle DELETE
    deletes = batch_df.filter(col("op") == "d") \
        .select("before.id")

    print(f'deletes count: {deletes.count()}')
    ids  = [row['id'] for row in deletes.collect()]

    if not ids:
        return

    logging.info(f'Preparing to delete {len(ids)} records with IDs: {ids}')
    conn = None
    try:
        conn = postgeres_pool.getconn()
        conn.autocommit = False
        with conn.cursor() as cur:
            # dùng ANY cho list
            cur.execute("DELETE FROM public.users WHERE id = ANY(%s)", (ids,))
        conn.commit()
        logging.info(f'Deleted {len(ids)} records')
    except Exception as e:
        if conn:
            conn.rollback()
        logging.exception("Delete failed")
        raise
    finally:
        if conn:
            postgeres_pool.putconn(conn)  



# Write stream with foreachBatch
query = df_json.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", spark_conf['checkpointLocation']) \
    .trigger(processingTime="3 seconds") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
