import re
from pyspark.sql import DataFrame , functions as F
from typing import List, Optional
from src.connectors.connection_pool import ConnectionPoolManager
from src.utils.monitoring import record_batch_metrics
from src.utils.error_handling import retry
import logging 

logger = logging.getLogger(__name__)

class CDCProcessor:
    def __init__(self , connection_pool: ConnectionPoolManager  , postgres_config) :
        self.connection_pool = connection_pool
        self.postgres_config = postgres_config


    @record_batch_metrics
    def process_cdc_operations(self , batch_df: DataFrame , batch_id :int , metrics: dict):
        """su lý cac thao tác cdc (insert , update , delete)

        Args:   
            batch_df (DataFrame): dữ liệu trong batch
            batch_id (int): id của batch
        """
        try:
            self._process_upserts(batch_df)
            self.__process_deletes(batch_df)
            logger.info(f"Batch {batch_id} processed successfully.")
        except Exception as e:
            metrics['errors'] += 1 
            logger.error(f"Error processing batch {batch_id}: {str(e)}", exc_info=True)
            raise
    

    def _process_upserts(self , batch_df :DataFrame):
        """su lý riêng thao tác về insert và update

        Args:
            batch_df (DataFrame): dữ liệu đầu vào là một batch 
        """
        upsert_df =  batch_df.filter(F.col("op").isin('c','u')).select("after.*")

        if upsert_df.count() > 0 :
            (upsert_df.write
                .format('jdbc')
                .option('url' , self.postgres_config.url)
                .option('dbtable' , self.postgres_config.table_nyc_taxi)
                .option('user' , self.postgres_config.user)
                .option('password' , self.postgres_config.password)
                .option('driver' , self.postgres_config.driver)
                .option('batchsize' , self.postgres_config.batch_size)
                .mode('append')
                .save())

    

    @retry(max_retries=3 , delay=1)
    def __process_deletes(self , batch_df :DataFrame):
        """ su lý riêng thao tác về delete

        Args:
            batch_df (DataFrame): dữ liệu đầu vào là một batch
        """

        delete_df =  batch_df.filter(F.col('op') == 'd').select("before.id")
        record_ids =  [row['id'] for row in delete_df.collect()]

        if not record_ids:
            return  
        

        with self.connection_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    update public.nyc_taxi 
                               set status_delete = True , deleted_at = current_timestamp
                        where id = ANY(%s)
                    """, (record_ids,))
            conn.commit()

            





