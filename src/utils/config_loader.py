from typing import Any, Dict
import yaml
from src.models.config_models import AppConfig, KafkaConfig, PostgresConfig, SparkConfig, SchemaColums


def load_config(config_path: str) -> Dict[str, Any]:
    """_summary_

    Args:
        config_path (str): dùng để load file config từ YAML file

    Returns:
        Dict[str, Any]: trả về một dictionary chứa cấu hình
    """

    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config


def create_app_config(raw_config: Dict[str, Any]) -> AppConfig:
    """craete appconfig  từ raw config dict 

    Args:
        raw_config (Dict[str, Any]): _description_

    Returns:
        AppConfig:trả về một instance của AppConfig
    """
    return AppConfig(
        kafka = KafkaConfig(
            bootstrap_servers=raw_config['kafka']['bootstrap_servers'],
            topic_user=raw_config['kafka']['topic_user'],
            topic_nyc_taxi=raw_config['kafka']['topic_nyc_taxi'],
            max_offsets_per_trigger=raw_config['kafka']['max_offsets_per_trigger'],
            min_partitions=raw_config['kafka']['min_partitions'],
            starting_offsets=raw_config['kafka']['starting_offsets'],
            group_id=raw_config['kafka']['group_id'],
            fail_on_data_loss=raw_config['kafka']['fail_on_data_loss']
        ),
        postgres=PostgresConfig(
            url = raw_config['postgres']['url'],
            user = raw_config['postgres']["user"],
            password = raw_config['postgres']["password"],
            table_user= raw_config['postgres']["table_user"],
            table_nyc_taxi= raw_config['postgres']["table_nyc_taxi"],
            driver= raw_config['postgres']["driver"], 
            batch_size= raw_config['postgres']["batch_size"],
        ),
        spark=SparkConfig(
            master=raw_config['spark']['master'],
            app_name=raw_config['spark']['app_name'],
            executor_memory=raw_config['spark']['executor_memory'],
            executor_cores=raw_config['spark']['executor_cores'],
            shuffle_partitions=raw_config['spark']['shuffle_partitions'],
            default_parallelism=raw_config['spark']['default_parallelism'],
            checkpoint_location=raw_config['spark']['checkpoint_location']
        ),
        schema_nyc_taxi=[SchemaColums(**col) for col in raw_config['schema_nyc_taxi']],
        schema_user=[SchemaColums(**col) for col in raw_config['schema_user']] 
    )