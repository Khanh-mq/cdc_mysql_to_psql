from typing import List
from pyspark.sql.types import StructType , StructField , StringType , IntegerType,  BooleanType , TimestampType , FloatType
from src.models.config_models  import SchemaColumn



def create_nyc_taxi_schema(columns: List[SchemaColumn]) -> StructType:
    """tạo schema chp data nyc_taxi 

    Args:
        colnums (List[SchemaColums]): các cột schema

    Returns:
        StructType: chuyển đổi thahf các kiểu dữu liệu tương uwnngs trong spark  
    """

    type_mapping = {
    "StringType": StringType(),
    "IntegerType": IntegerType(),
    "FloatType": FloatType(),
    "TimestampType": TimestampType(),
    "BooleanType": BooleanType()
    }

    fields = [
        StructField(col.name , type_mapping[col.type])
        for col in columns
    ]
    return StructType([
        StructField("before" , StructType(fields=fields) ,  True),
        StructField('after' , StructType(fields) , True), 
        StructField('op' , StringType()), 
        StructField('ts_ms' ,StringType())
    ])


