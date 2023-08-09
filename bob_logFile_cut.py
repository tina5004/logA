# split_log.py

# 로그 파일을 작은 파일로 분할하여 생성
def split_log_file(log_file_path, batch_size=20000000):
    try:
        with open(log_file_path, 'r') as log_file:
            logs = log_file.readlines()

        total_rows = len(logs)
        num_batches = (total_rows + batch_size - 1) // batch_size

        for i in range(num_batches):
            batch_start = i * batch_size
            batch_end = min(batch_start + batch_size, total_rows)
            batch_logs = logs[batch_start:batch_end]

            new_file_path = f'batch_{i + 1}.log'
            with open(new_file_path, 'w') as new_log_file:
                new_log_file.writelines(batch_logs)

            print(f"Batch {i + 1} created: {batch_start + 1} - {batch_end}")

    except Exception as e:
        print("Error while splitting log file:", e)

def main():
    log_file_path = 'security (2).log' # 로그파일 넣기
    split_log_file(log_file_path)

if __name__ == "__main__":
    main()
