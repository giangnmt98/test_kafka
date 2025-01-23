# Dự Án Kafka với Docker và Python

## Mô Tả Dự Án

Dự án này cung cấp một cách thức sử dụng Apache Kafka để xử lý luồng dữ liệu giữa các thành phần khác nhau trong hệ
thống. Bạn sẽ học cách chạy Kafka trên môi trường Docker, cũng như cách tạo **Producer** (thành phần gửi dữ liệu) và *
*Consumer** (thành phần nhận dữ liệu) bằng Python.

---

## Tìm Hiểu Về Kafka

### Apache Kafka Là Gì?

Apache Kafka là một nền tảng streaming dữ liệu phân tán, được thiết kế cho các ứng dụng yêu cầu xử lý dữ liệu theo thời
gian thực. Kafka thường được ứng dụng trong:

- Lưu và xử lý logs của hệ thống.
- Truyền tải các sự kiện giữa các ứng dụng.
- Xây dựng data pipelines và hệ thống phân tích dữ liệu theo thời gian thực.

### Các Thành Phần Chính:

1. **Topic**: Là nơi lưu trữ dữ liệu. Topic được chia thành nhiều **partition** để tăng khả năng xử lý song song và tốc
   độ.
2. **Producer**: Gửi dữ liệu vào topic.
3. **Consumer**: Lấy dữ liệu từ topic để xử lý.
4. **Broker**: Cụm Kafka chịu trách nhiệm lưu trữ và quản lý dữ liệu.

---

## Tác Dụng Của Producer và Consumer

### Producer

Producer là thành phần gửi dữ liệu đến Kafka topics. Nó giúp truyền tải thông tin từ một thành phần của hệ thống (như
sensor, ứng dụng log) tới topic để lưu và xử lý sau.

### Consumer

Consumer là thành phần đọc dữ liệu từ Kafka topics. Consumer nhận thông điệp từ topic và xử lý chúng, ví dụ như phân
tích logs, phân loại dữ liệu theo điều kiện, hoặc hiển thị lên giao diện người dùng.

---

## Hướng Dẫn Khởi Chạy Kafka

### Bước 1: Chạy Kafka bằng Docker Compose

Đảm bảo file `docker-compose.yml` đã sẵn sàng. Chạy lệnh:

```bash
docker-compose up -d
```

Kiểm tra Kafka và Zookeeper đã chạy:

```bash
docker ps
```

---

---

## Cài Đặt Thư Viện Từ File `requirements.txt`

Trước khi chạy Producer và Consumer, bạn cần cài đặt các thư viện cần thiết được liệt kê trong file `requirements.txt`.
Để cài đặt, chạy lệnh sau:

```bash
pip install -r requirements.txt
```

Đảm bảo bạn đã kích hoạt môi trường Python (virtual environment) nếu cần thiết trước khi chạy lệnh trên.
## Hướng Dẫn Chạy Producer và Consumer

### Lấy dữ liệu bằng Consumer

1. Chạy lệnh:
   ```bash
   python3 consumer.py
   ```
   Kết quả: Consumer sẽ nhận và hiển thị từng tin nhắn được gửi từ topic `csv_topic`.

---
### Tạo dữ liệu bằng Producer

1. Chạy lệnh:
   ```bash
   python3 producer.py
   ```
   Kết quả: Producer sẽ gửi tin nhắn vào topic `csv_topic` trên Kafka.
