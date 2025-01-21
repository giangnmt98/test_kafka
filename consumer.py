from confluent_kafka import Consumer

# Cấu hình Kafka Consumer
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Địa chỉ Kafka broker
    'group.id': 'utf8-group',  # Tên group của Consumer
    'auto.offset.reset': 'earliest'  # Đọc từ đầu topic nếu chưa có offset
}

consumer = Consumer(consumer_config)
consumer.subscribe(['csv_topic'])  # Đăng ký topic Kafka để lắng nghe

print("Consumer bắt đầu lắng nghe tin nhắn...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Lấy tin nhắn từ topic
        if msg is None:
            continue  # Không có tin nhắn nào trong khoảng thời gian timeout
        if msg.error():
            print(f"Lỗi: {msg.error()}")
            continue

        # Hiển thị tin nhắn với dấu phân tách "="
        print("=" * 50)  # In ra 50 dấu "=" để phân cách giữa các tin nhắn
        print(f"Partition: {msg.partition()}")  # Partition nhận tin nhắn
        print(f"Offset: {msg.offset()}")  # Offset của tin nhắn
        print(f"Message:\n{msg.value().decode('utf-8')}")  # Nội dung tin nhắn
        print("=" * 50)  # Kết thúc phân cách
        print("\n")  # In thêm dòng trống để ngăn cách rõ ràng hơn

except KeyboardInterrupt:
    print("\nConsumer đã dừng.")

finally:
    consumer.close()  # Đóng kết nối khi hoàn tất
