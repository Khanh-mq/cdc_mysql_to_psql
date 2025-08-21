import pymysql
import time

start = time.time()

# Kết nối MySQL bằng PyMySQL
conn = pymysql.connect(
    host='project_cdc_mysql_to_postgres-mysql-1',  # chú ý: dùng '-' chứ không phải '_'
    database='source_db',
    user='root',
    password='root',
    port=3306
)

cursor = conn.cursor()

batch_size = 1000
total_records = 10_000

for i in range(1, total_records + 1, batch_size):
    values = [(f'user{j}', f'user{j}@email.com') for j in range(i, min(i + batch_size, total_records + 1))]
    cursor.executemany(
        "INSERT INTO users (name, email) VALUES (%s, %s)", values
    )
    conn.commit()
    print(f'Inserted up to record {i + batch_size - 1}')

print(f'Total time taken: {time.time() - start} seconds')

cursor.close()
conn.close()
