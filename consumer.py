from confluent_kafka import Consumer

# Cấu hình Kafka Consumer
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Địa chỉ Kafka broker
    'group.id': 'utf8-group',  # Tên group của Consumer
    'auto.offset.reset': 'earliest'  # Đọc từ đầu topic nếu chưa có offset
}

consumer = Consumer(consumer_config)
consumer.subscribe(['csv_topic_1', 'csv_topic_2'])  # Đăng ký lắng nghe 2 topic

print("Consumer bắt đầu lắng nghe tin nhắn từ csv_topic_1 và csv_topic_2...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue  # Không có tin nhắn nào trong khoảng thời gian timeout
        if msg.error():
            print(f"Lỗi: {msg.error()}")
            continue

        # Hiển thị tin nhắn với phân cách
        print("=" * 50)  # Đánh dấu tin nhắn mới
        print(f"Topic: {msg.topic()}")  # Hiển thị topic gửi tin nhắn
        print(f"Message:\n{msg.value().decode('utf-8')}")  # Nội dung tin nhắn
        print("=" * 50)
        print("\n")

except KeyboardInterrupt:
    print("\nConsumer đã dừng.")

finally:
    consumer.close()  # Đóng kết nối khi hoàn tất
