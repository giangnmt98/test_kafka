import csv
import random
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


# Hàm để serialize dữ liệu sang dạng chuỗi UTF-8
def utf8_serializer(data):
    return data.encode('utf-8')


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Địa chỉ Kafka của bạn
    value_serializer=utf8_serializer
)

topic_name = "csv_topic"  # Tên topic Kafka


# Hàm đăng ký (tạo) topic Kafka nếu chưa tồn tại
def create_topic(topic_name, num_partitions=1, replication_factor=1):
    admin_client = KafkaAdminClient(
        bootstrap_servers=['localhost:9092'],  # Địa chỉ Kafka của bạn
    )
    try:
        # Tạo topic mới
        topic_list = [
            NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        ]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{topic_name}' đã được tạo thành công.")
    except Exception as e:
        # Nếu topic đã tồn tại hoặc có lỗi khác
        print(f"Không thể tạo topic '{topic_name}': {e}")
    finally:
        admin_client.close()


# Hàm để đọc dữ liệu từ file CSV
def read_csv(file_path):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        data = [row for row in csv_reader]  # Lưu tất cả các dòng trong file CSV vào list
    return data


if __name__ == "__main__":
    csv_file_path = "data.csv"  # Đường dẫn tới file CSV
    rows = read_csv(csv_file_path)  # Đọc tất cả các dòng từ file CSV
    print(f"Đã đọc {len(rows)} dòng từ file CSV.")

    # # Đăng ký hoặc kiểm tra topic
    # create_topic(topic_name)

    print("Producer bắt đầu gửi tin nhắn...")
    while rows:  # Tiếp tục gửi cho đến khi hết dữ liệu
        # Chọn số dòng ngẫu nhiên (1 dòng hoặc nhiều dòng)
        num_rows = random.randint(1, 5)  # Gửi ngẫu nhiên từ 1 đến 5 dòng
        selected_rows = rows[:num_rows]  # Lấy ra những dòng sẽ gửi
        rows = rows[num_rows:]  # Cập nhật lại danh sách, xóa những dòng đã gửi

        # Tạo thông điệp
        message = "\n".join([",".join(row) for row in selected_rows])
        producer.send(topic_name, message)  # Gửi thông điệp tới Kafka
        # print(f"Đã gửi {num_rows} dòng:\n{message}")

        # Đợi thời gian ngẫu nhiên trước khi gửi tiếp
        time_to_sleep = random.uniform(1, 5)  # Khoảng thời gian ngẫu nhiên từ 1 đến 5 giây
        # print(f"Đợi {time_to_sleep:.2f} giây trước khi gửi tiếp...")
        time.sleep(time_to_sleep)  # Tạm dừng trong khoảng thời gian ngẫu nhiên

    print("Tất cả dữ liệu đã được gửi.")
