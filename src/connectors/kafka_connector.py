from pyspark.sql import SparkSession , DataFrame
from src.models.config_models import KafkaConfig
from src.utils.error_handling import PipelineError, handle_spark_errors, retry
import logging
@retry(max_retries=3 , delay=2)
@handle_spark_errors
def create_kafka_stream(spark: SparkSession , kafka_config: KafkaConfig) -> DataFrame:
    """tạo và cấu hình kafak streaming dataframe với validation đầy đủ 

    Args:
        spark (SparkSession): 
        kafka_config (KafkaConfig): 

    Returns:
        DataFrame: kafka streaming dataframe 
    Raises :
         piplineError : neeu  có lỗi kết nối với kafka  
    """
    logger =  logging.getLogger(__name__)
    if not kafka_config.bootstrap_servers:
        raise ValueError("bootrap_server cannot be empty")
    if not kafka_config.topic_nyc_taxi:
        raise ValueError('topic_nyc_taxi cannot be empty')
    
    logger.info(f'craeting kafka stream for topic;{kafka_config.topic_nyc_taxi}')
    logger.info(f'bootrap server : {kafka_config.bootstrap_servers}')

    try:
        stream_df = ( spark.readStream 
        .format("kafka") 
        .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers) 
        .option("subscribe", kafka_config.topic_nyc_taxi) 
        .option("startingOffsets", kafka_config.starting_offsets) 
        .option("maxOffsetsPerTrigger", kafka_config.max_offsets_per_trigger) 
        .option("minPartitions", kafka_config.min_partitions) 
        .option("failOnDataLoss", kafka_config.fail_on_data_loss) 
        .option("group.id", kafka_config.group_id) 
        )

        test_df =  stream_df.load()

        logger.info('kafka streaming created sucessfully' \
        '')
        return test_df
    except Exception as e:
        logger.error(f'Fail to craete kafka stream :{str(e)}' ,  exc_info=True)
        raise PipelineError(f'kafka connection faild :{str(e)}' , e )  from e 



def validate_kafak_connection(spark: SparkSession , kafka_config: KafkaConfig) -> bool:
    """validate kafka connect banwg cach thu list topic 

    Args:
        spark (SparkSession): SparkSession
        kafka_config (KafkaConfig): cấu hình kafka 

    Returns:
        bool: true nếu  kết nối thành công
    """
    logger =  logging.getLogger(__name__)
    try:
        # thu list topic de validate connection
        topic_df =  (spark.read
                     .format("kafka")
                     .option("kafka.bootstrap.servers" , kafka_config.bootstrap_servers) 
                     .option("subscribe" , kafka_config.topic_nyc_taxi)
                     .option("group.id" ,  f'validate-{kafka_config.group_id}')
                     .load()
                     .selectExpr('explode(distincr(topic) as topics)'))
        topics = [row['topics']  for row in topic_df.collect()]
        logger.info(f'Available topics : {topics}')

        if kafka_config.topic_nyc_taxi not in topics:
            logger.warning(f'Topic {kafka_config.topic_nyc_taxi}  not found in available topics')
            return False 

        return True 
    except Exception as e :
        logger.error(f'kafka validate failed :{str(e)}')
        return False 