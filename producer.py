from confluent_kafka import Producer
import csv
import random
import time
import threading


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


def producer_thread(topic_name, data_rows):
    """Hàm producer gửi dữ liệu trên một topic"""
    producer_config = {
        'bootstrap.servers': 'localhost:9092'  # Địa chỉ Kafka broker
    }
    producer = Producer(producer_config)

    print(f"Producer cho topic '{topic_name}' bắt đầu gửi tin nhắn...")

    while data_rows:
        # Chọn số dòng ngẫu nhiên (từ 1 đến 5) để gửi
        num_rows = random.randint(1, 5)
        selected_rows = data_rows[:num_rows]
        data_rows = data_rows[num_rows:]  # Cập nhật danh sách dữ liệu, loại bỏ phần đã gửi

        # Tạo thông điệp để gửi
        message = "\n".join([",".join(row) for row in selected_rows])

        # Gửi tin nhắn tới topic
        producer.produce(topic_name, value=message, callback=delivery_report)

        # Đợi thời gian ngẫu nhiên trước khi gửi tiếp
        time_to_sleep = random.uniform(1, 5)
        time.sleep(time_to_sleep)

    producer.flush()
    print(f"Producer cho topic '{topic_name}' đã gửi xong dữ liệu.")


def main():
    csv_file_path = "data.csv"  # Đường dẫn file CSV
    rows = read_csv(csv_file_path)  # Đọc tất cả các dòng từ file CSV
    print(f"Đã đọc {len(rows)} dòng từ file CSV.")

    # Khởi tạo luồng producer cho hai topic
    topic_1 = "csv_topic_1"
    topic_2 = "csv_topic_2"

    thread_1 = threading.Thread(target=producer_thread, args=(topic_1, rows.copy()))
    thread_2 = threading.Thread(target=producer_thread, args=(topic_2, rows.copy()))

    # Bắt đầu các luồng
    thread_1.start()
    thread_2.start()

    # Chờ các luồng kết thúc
    thread_1.join()
    thread_2.join()

    print("Tất cả các producer đã hoàn tất gửi dữ liệu.")


if __name__ == "__main__":
    main()
