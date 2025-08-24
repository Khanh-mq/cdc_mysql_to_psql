import pandas as pd
from sqlalchemy import create_engine


df  = pd.read_parquet("/mnt/winpart/Khanh/project_cdc_mysql_to_postgres/scripts/crawl/data/raw/yellow_tripdata_2024-02.parquet" , engine="pyarrow")

engine = create_engine("mysql+pymysql://mysqluser:mysqlpass@localhost:3306/source_db")

print(df.dtypes)

# ghi vào MySQL (append hoặc replace)
df.to_sql("nyc_taxi", engine, if_exists="append", index=False, chunksize=10000)