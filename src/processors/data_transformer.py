from ast import Str
import re
import select
from tarfile import data_filter
from pyspark.sql import DataFrame , functions as F
from pyspark.sql.types import StructType
from src.utils.error_handling import retry



@retry(max_retries=3 , delay=2)
def transform_kafka_data(df : DataFrame , schema : StructType) -> DataFrame:
    """chuyển đổi dữ liệu từ kafka data thành dataframe với schema đã cho

    Args:
        df (DataFrame): dữ liệu từ kafka
        schema (StructType): schema của dataframe mong muốn

    Returns:
        DataFrame: _description_
    """
    return (df
            .selectExpr("CAST(value AS STRING ) as json")
            .select(F.from_json(F.col("json") , schema).alias("data"))
            .select("data.*")
            .repartition(12))