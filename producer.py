from confluent_kafka import Producer
import csv
import random
import time


def read_csv(file_path):
    """Hàm đọc file CSV"""
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        data = [row for row in csv_reader]  # Lưu tất cả các dòng trong file CSV vào list
    return data


def delivery_report(err, msg):
    """Callback báo cáo kết quả gửi tin nhắn"""
    if err is not None:
        print(f"Lỗi khi gửi tin nhắn: {err}")
    else:
        print(f"Tin nhắn gửi thành công tới Topic {msg.topic()} Partition {msg.partition()} Offset {msg.offset()}")


def main():
    # Cấu hình producer
    producer_config = {
        'bootstrap.servers': 'localhost:9092'  # Địa chỉ Kafka broker
    }
    producer = Producer(producer_config)

    topic_name = "csv_topic"  # Tên topic
    csv_file_path = "data.csv"  # Đường dẫn file CSV

    # Đọc dữ liệu từ file CSV
    rows = read_csv(csv_file_path)
    print(f"Đã đọc {len(rows)} dòng từ file CSV.")

    print("Producer bắt đầu gửi tin nhắn...")
    while rows:
        # Chọn số dòng ngẫu nhiên (từ 1 đến 5) để gửi
        num_rows = random.randint(1, 5)
        selected_rows = rows[:num_rows]
        rows = rows[num_rows:]  # Cập nhật danh sách dữ liệu, loại bỏ phần đã gửi

        # Tạo thông điệp để gửi
        message = "\n".join([",".join(row) for row in selected_rows])

        # Gửi tin nhắn với Producer và callback
        producer.produce(topic_name, value=message, callback=delivery_report)

        # Đợi thời gian ngẫu nhiên trước khi gửi tin nhắn tiếp theo
        time_to_sleep = random.uniform(1, 5)
        time.sleep(time_to_sleep)

    # Đảm bảo các tin nhắn đã gửi xong
    producer.flush()
    print("Tất cả dữ liệu đã được gửi.")


if __name__ == "__main__":
    main()
