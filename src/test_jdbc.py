from pyspark.sql import SparkSession

# # Khởi tạo SparkSession
# spark = SparkSession.builder \
#     .appName("JDBC Test MySQL & PostgreSQL") \
#     .master("spark://spark:7077") \
#     .getOrCreate()

spark = SparkSession.builder \
        .appName("JDBC test mysql & postgres") \
        .master("local[*]")\
        .getOrCreate()


# =========================
# 1. Test kết nối MySQL
# =========================
mysql_url = "jdbc:mysql://mysql:3306/source_db"
mysql_properties = {
    "user": "mysqluser",
    "password": "mysqlpass",
    "driver": "com.mysql.cj.jdbc.Driver"
}

print("\n===== Đọc dữ liệu từ MySQL =====")
try:
    mysql_df = spark.read.jdbc(url=mysql_url, table="users", properties=mysql_properties)
    mysql_df.show()
except Exception as e:
    print("Lỗi kết nối MySQL:", e)

# =========================
# 2. Test kết nối PostgreSQL
# =========================
postgres_url = "jdbc:postgresql://postgres:5432/target_db"
postgres_properties = {
    "user": "postgresuser",
    "password": "postgrespass",
    "driver": "org.postgresql.Driver"
}

print("\n===== Đọc dữ liệu từ PostgreSQL =====")
try:
    postgres_df = spark.read.jdbc(url=postgres_url, table="users", properties=postgres_properties)
    postgres_df.show()
except Exception as e:
    print("Lỗi kết nối PostgreSQL:", e)

spark.stop()
