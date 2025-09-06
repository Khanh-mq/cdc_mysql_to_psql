from dataclasses import dataclass
from typing import Dict , Any , List ,  Optional



@dataclass
class KafkaConfig:
    bootstrap_servers: str 
    topic_user: str
    topic_nyc_taxi: str
    max_offsets_per_trigger: int
    min_partitions: int
    starting_offsets: str 
    group_id: str
    fail_on_data_loss: bool 

@dataclass
class PostgresConfig:
    url : str
    user : str
    password : str
    table_user: str
    table_nyc_taxi: str
    driver: str 
    batch_size: int


@dataclass
class SparkConfig:
    master: str
    app_name: str
    executor_memory: str
    executor_cores: int
    shuffle_partitions: int
    default_parallelism: int
    checkpoint_location: str

@dataclass
class SchemaColums:
    name: str
    type: str

@dataclass
class AppConfig:
    kafka: KafkaConfig
    postgres: PostgresConfig
    spark: SparkConfig
    schema_nyc_taxi: List[SchemaColums]
    schema_user: List[SchemaColums]