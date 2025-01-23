# Dự án Giả lập luông gửi và nhận Antispam data thông qua Kafka

## Yêu Cầu Hệ Thống

- **Python phiên bản**: 3.11 trở lên.
- **Docker và Docker Compose**: Phiên bản mới nhất.
- **Hệ điều hành được khuyến nghị**: Linux hoặc macOS.

---

## Cấu Trúc Dự Án

Dưới đây là cấu trúc thư mục của dự án:

```plaintext
.
├── docker-compose.yml   # Cấu hình Docker để chạy Kafka và Zookeeper
├── requirements.txt     # Danh sách các package cần cài đặt
├── pyproject.toml       # Cấu hình để cài đặt dự án dưới dạng package
├── README.md            # Tài liệu hướng dẫn
└── antispamkafka/                 # Thư mục chứa mã nguồn
    ├── __init__.py
    ├── utils            # Chứa các file logger, helpers và utils phục vụ chạy các phần code chính
    ├── producer.py      # Tập lệnh Producer gửi dữ liệu lên Kafka
    ├── consumer.py      # Tập lệnh Consumer nhận dữ liệu từ Kafka
    └── handlers.py      # Tập lệnh chứa các function xử lý dữ liệu cho mỗi kafka topic khác nhau
    
```

---

## Cài Đặt Dự Án

Dự án đã được cài đặt dưới dạng package Python. Nếu chưa có môi trường ảo, tạo một môi trường mới:

```bash
make venv
```


## Hướng Dẫn Chạy Kafka Bằng Docker

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

## Hướng Dẫn Chạy Producer và Consumer

### Chạy Consumer

1. Thực hiện lệnh:

   ```bash
   python3 antispamkafka/consumer.py
   ```
   

---

### Chạy Producer

1. Thực hiện lệnh:

   ```bash
   python3 antispamkafka/producer.py
   ```
---
## Thực hiện format code và các check case

### Chạy format code

   ```bash
    make style
   ```

### Chạy check format code, mypy, docstrings

   ```bash
    chmod +x run_check.sh # Nếu là lần đầu chạy, các lần sau chỉ cần chạy lệnh dưới đây
    ./run_check.sh
   ```