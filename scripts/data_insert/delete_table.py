import argparse
import pymysql


def delete_records(start, end ,  cursor, conn):
    cursor.execute(
        "DELETE FROM users WHERE id >= %s AND id < %s", (start, end)
    )
    conn.commit()
    print(f'Deleted records from {start} to {end - 1}')

if __name__ == "__main__":
    conn = pymysql.connect(
        host="project_cdc_mysql_to_postgres-mysql-1",
        database="source_db",
        user="root",
        password="root",
        port=3306,
    )
    cursor = conn.cursor()
    
    parser = argparse.ArgumentParser(description="Delete records from MySQL table.")
    parser.add_argument("--start", type=int, default=1, help="Start ID for deletion")
    parser.add_argument("--end", type=int, default=100_000, help="End ID for deletion")
    
    args = parser.parse_args()
    
    delete_records(args.start, args.end ,  cursor, conn)

    cursor.close()
    conn.close()
