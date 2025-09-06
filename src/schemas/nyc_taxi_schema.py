from typing import List
from pyspark.sql.types import StructType , StructField , StringType , IntegerType,  BooleanType , TimestampType , FloatType
from sqlalchemy import true 
from src.models.config_models  import SchemaColums



def create_nyc_taxi_schema(columns: List[SchemaColums]) -> StructType:
    """tạo schema chp data nyc_taxi 

    Args:
        colnums (List[SchemaColums]): các cột schema

    Returns:
        StructType: chuyển đổi thahf các kiểu dữu liệu tương uwnngs trong spark  
    """

    type_mapping = {
        "IntegerType" : IntegerType(),
        "StringType":StringType(),
        "TimetampType":TimestampType(),
        "FloatType": FloatType(),
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


