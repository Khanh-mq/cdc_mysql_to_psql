from pyspark.sql import SparkSession , DataFrame
from src.models.config_models import KafkaConfig


def create_kafka_stream(spark: SparkSession , kafka_config: KafkaConfig) -> DataFrame:
    """create va configure kafka streaming dataframe"""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers) \
        .option("subscribe", kafka_config.topic_nyc_taxi) \
        .option("startingOffsets", kafka_config.starting_offsets) \
        .option("maxOffsetsPerTrigger", kafka_config.max_offsets_per_trigger) \
        .option("minPartitions", kafka_config.min_partitions) \
        .option("failOnDataLoss", kafka_config.fail_on_data_loss) \
        .option("groupIdPrefix", kafka_config.group_id) \
        .load()