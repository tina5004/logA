import pymysql
import time

# MySQL 데이터베이스 연결 설정
def connect_to_database():
    connection = pymysql.connect(
        host='localhost',        # 호스트 주소
        database='young',     # 데이터베이스 이름
        user='root',       # 사용자 이름
        password='young'    # 비밀번호
    )
    return connection

# 로그 파일에서 정보를 읽어와 MySQL에 삽입
def insert_logs_to_database(connection, log_file_path, batch_size=10000):
    try:
        with open(log_file_path, 'r') as log_file:
            logs = [line.strip().split(',') for line in log_file]

        total_rows = len(logs)
        estimated_time_per_batch = 2.0  # 예상 시간 (초) per 배치

        total_inserted = 0
        batch_start = 0

        with connection.cursor() as cursor:
            query = """
            INSERT INTO log_data 
            (log_time, employee_number, source_ip, source_mac, 
            source_port, destination_ip, destination_mac, destination_port, packet_size) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            while batch_start < total_rows:
                batch_end = min(batch_start + batch_size, total_rows)
                batch_logs = [
                    (
                        int(log[1]), int(log[2]), int(log[3]),
                        int(log[4]), int(log[5]), int(log[6]),
                        int(log[7]), int(log[8]), int(log[9])
                    )
                    for log in logs[batch_start:batch_end]
                ]

                estimated_time = estimated_time_per_batch * (batch_end - batch_start) / batch_size
                print(f"Estimated time for batch ({batch_start+1} - {batch_end}) : {estimated_time:.2f} seconds")

                start_time = time.time()
                cursor.executemany(query, batch_logs)
                connection.commit()
                end_time = time.time()

                inserted_count = batch_end - batch_start
                total_inserted += inserted_count

                print(f"Inserted {inserted_count} logs in {end_time - start_time:.2f} seconds. Total inserted: {total_inserted}/{total_rows}")

                batch_start = batch_end

        print("All logs inserted into the database")

    except pymysql.Error as e:
        print("Error while inserting logs:", e)

# 메인 함수
def main():
    log_file_path = 'batch_2.log'
    connection = connect_to_database()

    if connection:
        insert_logs_to_database(connection, log_file_path)
        connection.close()
        print("Connection closed")

if __name__ == "__main__":
    main()
