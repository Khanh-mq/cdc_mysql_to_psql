from math import log
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import yaml
import logging
#------logging setup------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')




# Initialize Spark session
spark = SparkSession.builder \
    .appName("cdc_mysql_to_postgres") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar,/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/bitnami/spark/jars/kafka-clients-3.5.1.jar,/opt/bitnami/spark/jars/spark-streaming-kafka-0-10_2.12-3.5.1.jar") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.master", "spark://0.0.0.0:7077") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.default.parallelism", "8") \
    .getOrCreate()

# Define schema for JSON data from Kafka
schema = StructType([
    StructField("before", StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("created_at", TimestampType())
    ]), True),
    StructField("after", StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("created_at", TimestampType())
    ]), True),
    StructField("op", StringType()),
    StructField("ts_ms", StringType())
])

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("maxOffsetsPerTrigger", "10000") \
    .option("minPartitions", "8") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "mysql_server.source_db.users") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON from Kafka
df_json = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Process CDC operations
def process_batch(batch_df, batch_id):
    print(f"Processing batch {batch_id}")

    # Handle INSERT + UPDATE
    inserts_updates = batch_df.filter(col("op").isin("c", "u")) \
        .select("after.*")

    if inserts_updates.count() > 0:
        inserts_updates.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/target_db") \
            .option("dbtable", "public.users") \
            .option("user", "postgresuser") \
            .option("password", "postgrespass") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

    # Handle DELETE
    deletes = batch_df.filter(col("op") == "d") \
        .select("before.id")

    if deletes.count() > 0:
        # Tạo câu SQL delete động
        ids = [str(row["id"]) for row in deletes.collect()]
        where_clause = ",".join(ids)
        delete_query = f"DELETE FROM public.users WHERE id IN ({where_clause})"

        # Dùng JDBC connection trong Spark để gửi query delete
        spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/target_db") \
            .option("user", "postgresuser") \
            .option("password", "postgrespass") \
            .option("driver", "org.postgresql.Driver") \
            .option("query", delete_query) \
            .load()

# Write stream with foreachBatch
query = df_json.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/checkpoint") \
    .trigger(processingTime="1 seconds") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
