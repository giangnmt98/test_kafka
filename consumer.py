from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'csv_topic',  # Tên topic Kafka
    bootstrap_servers=['localhost:9092'],  # Địa chỉ Kafka
    auto_offset_reset='earliest',  # Đọc từ đầu topic nếu chưa có offset
    group_id='utf8-group',  # Group ID của consumer
    value_deserializer=lambda v: v.decode('utf-8'),  # Convert byte sang UTF-8
    consumer_timeout_ms=1000  # Thời gian chờ nếu không nhận thấy tin nhắn
)


def main():
    """
    Hàm này khởi tạo một Kafka consumer để lắng nghe tin nhắn từ một topic nhất định
    và in ra các tin nhắn nhận được kèm theo thông tin như partition và offset.

    Hàm liên tục poll các tin nhắn từ topic Kafka trong vòng lặp và xử lý ngắt (Ctrl+C) 
    một cách an toàn bằng cách dừng consumer và giải phóng tài nguyên.
    """
    print("Consumer bắt đầu lắng nghe tin nhắn...")
    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=1000)  # Poll tin nhắn mỗi giây
            for tp, messages in msg_pack.items():
                for message in messages:
                    print(
                        f"Nhận được tin nhắn từ Partition {message.partition}, Offset {message.offset}: {message.value}"
                    )
    except KeyboardInterrupt:
        print("\nConsumer đã dừng!")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
