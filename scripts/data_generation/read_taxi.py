import pandas as pd
import pymysql

# Đọc Parquet
df = pd.read_parquet(
    r"./data/raw/nyc_taxi/historical/yellow_tripdata_2024-03.parquet",
    engine="pyarrow"
)
print(f"✅ Đọc Parquet thành công, shape={df.shape}")

# Xuất CSV tạm với \N để MySQL hiểu là NULL
csv_file = "/tmp/taxi.csv"
df.to_csv(
    csv_file,
    index=False,
    encoding="utf-8",
    na_rep="\\N",         # thay NaN bằng \N
    lineterminator="\n"   # tránh lỗi xuống dòng
)

# Kết nối MySQL
conn = pymysql.connect(
    host="localhost",
    user="mysqluser",
    password="mysqlpass",
    database="source_db",
    local_infile=1  # bật LOCAL INFILE
)
cursor = conn.cursor()

# Danh sách cột cần insert (phải khớp với MySQL table nyc_taxi)
columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
]

# Câu lệnh LOAD DATA
sql = f"""
LOAD DATA LOCAL INFILE '{csv_file}'
INTO TABLE nyc_taxi
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\\n'
IGNORE 1 ROWS
({", ".join(columns)});
"""

# Thực thi
cursor.execute(sql)
conn.commit()

cursor.close()
conn.close()
print("✅ Load CSV vào MySQL thành công!")
