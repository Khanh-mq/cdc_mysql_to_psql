import logging
from os import spawnlp 
import kafka
from pyspark.sql  import SparkSession 
from src.utils.config_loader import load_config , create_app_config
from src.utils.logging_utils import setup_logger
from src.utils.monitoring import PipelineMetrics
from src.utils.error_handling import PipelineError
from src.connectors.connection_pool import ConnectionPoolManager
from src.processors.data_transformer import transform_kafka_data
from src.processors.cdc_processor import CDCProcessor
from src.schemas.nyc_taxi_schema import     create_nyc_taxi_schema
from src.connectors.kafka_connector import create_kafka_stream


class NYCTaxiCDCJob:
    """main nyc taxi cdc  pipeline job """
    def __init__(self , config_path: str = '/spark-jobs/config/cdc-config.yml'):
        self.config_path =  config_path
        self.config = None 
        self.spark =  None
        self.connection_pool = None 
        self.kafka_connector =  None 
        self.cdc_processor =  None 
        self.pipeline_metrics =  PipelineMetrics()
    

    def initialize(self):
        """khoi tạo tất cả các thành phần"""
        logger =  logging.getLogger(__name__)


        logger.info("loading configuration....")
        raw_config = load_config(self.config_path)
        self.config =  create_app_config(raw_config)

        #  khoi tao mot spark 
        logger.info("khoi tao phien ban cho spark")
        self.spark =  self._create_spark_session(self.config.spark)

        #  khoi tao connection 
        logger.info("khoi tao connection den postgres pool")
        self.connection_pool = ConnectionPoolManager(self.config.postgres)


        logger.info("creating kafka connector.......")
        self.kafka_connector = create_kafka_stream(self.spark , self.config.kafka)

        logger.info("creatting cdc processor....")
        self.cdc_processor =  CDCProcessor(
            self.connection_pool , 
            self.config.postgres , 
        )

    def _create_spark_session(self , spark_config):
        """craeting spark session 

        Args:
            spark_config (_type_): dữ liệu đầu vào là spark_config 
        """
        return (SparkSession.builder
                .appName(spark_config.app_name)
                .master(spark_config.master)
                .config("spark.executor.memory", spark_config.executor_memory )
                .config("spark.executor.cores", spark_config.executor_cores) \
                .config("spark.default.parallelism", spark_config.default_parallelism) \
                .config('spark.sql.shuffle.partitions', spark_config.shuffle_partitions) \
                .config("spark.streaming.stopGracefullyOnShutdown", "true") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
                .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
                .config("spark.streaming.stopGracefullyOnShutdown", "true") \
                .getOrCreate())
    


    def run(self):
        logger=  logging.getLogger(__name__)

       

        # Ensure self.config is initialized after calling self.initialize()
        if self.config is None:
            logger.error("Failed to initialize configuration. Exiting run method.")
            return

        try:
            #create schema 
            schema = create_nyc_taxi_schema(self.config.schema_nyc_taxi)
            kafka_stream = self.kafka_connector