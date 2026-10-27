#!/bin/bash

# Bash script to automate starting the Traffic Monitoring Pipeline

echo -e "\033[0;36m🚦 BẮT ĐẦU KHỞI ĐỘNG TRAFFIC MONITORING PIPELINE...\033[0m"

# 1. Kiểm tra Docker
echo -e "\n\033[0;34m1. Kiểm tra Docker...\033[0m"
if ! docker info >/dev/null 2>&1; then
    echo -e "\033[0;31m❌ Lỗi: Docker Desktop chưa được khởi chạy. Vui lòng mở Docker Desktop.\033[0m"
    exit 1
fi
echo -e "\033[0;32m✅ Docker đang hoạt động.\033[0m"

# 2. Khởi động Docker Compose
echo -e "\n\033[0;34m2. Khởi chạy các container nền (ClickHouse, MinIO, RabbitMQ, Spark)...\033[0m"
docker-compose up -d
if [ $? -ne 0 ]; then
    echo -e "\033[0;31m❌ Lỗi khởi động docker-compose.\033[0m"
    exit 1
fi

# 3. Đợi các dịch vụ
echo -e "\n\033[0;34m3. Chờ đợi ClickHouse và RabbitMQ khởi động hoàn tất (10 giây)...\033[0m"
sleep 10

# 4. Cài đặt thư viện trong Spark Master
echo -e "\n\033[0;34m4. Cài đặt các thư viện cần thiết vào Spark Master container...\033[0m"
docker exec -u 0 -t image_pipeline_spark_master pip install minio pika clickhouse-connect Pillow
if [ $? -ne 0 ]; then
    echo -e "\033[0;31m❌ Thất bại khi cài đặt thư viện vào Spark container.\033[0m"
else
    echo -e "\033[0;32m✅ Cài đặt thư viện Spark thành công.\033[0m"
fi

# 5. Khởi động gRPC Server
echo -e "\n\033[0;34m5. Đang chạy gRPC Server (ingestion/server.py) trong nền...\033[0m"
if [ -f "./venv/bin/python" ]; then
    ./venv/bin/python ingestion/server.py > ingestion_server.log 2>&1 &
    echo -e "\033[0;32m✅ gRPC Server đã được khởi động trong nền.\033[0m"
elif [ -f "./venv/Scripts/python.exe" ]; then
    ./venv/Scripts/python.exe ingestion/server.py > ingestion_server.log 2>&1 &
    echo -e "\033[0;32m✅ gRPC Server đã được khởi động trong nền (Windows path inside Bash).\033[0m"
else
    python3 ingestion/server.py > ingestion_server.log 2>&1 &
    echo -e "\033[0;33m⚠️ Sử dụng python3 hệ thống để chạy gRPC Server trong nền.\033[0m"
fi

# 6. Khởi động Web Dashboard
echo -e "\n\033[0;34m6. Đang mở Web Dashboard (dashboard.py) trên trình duyệt...\033[0m"
if [ -f "./venv/bin/python" ]; then
    ./venv/bin/python -m streamlit run dashboard.py &
    echo -e "\033[0;32m✅ Web Dashboard đang được mở tại http://localhost:8501.\033[0m"
elif [ -f "./venv/Scripts/python.exe" ]; then
    ./venv/Scripts/python.exe -m streamlit run dashboard.py &
    echo -e "\033[0;32m✅ Web Dashboard đang được mở tại http://localhost:8501.\033[0m"
else
    python3 -m streamlit run dashboard.py &
    echo -e "\033[0;33m⚠️ Sử dụng python3 hệ thống để khởi chạy Streamlit.\033[0m"
fi

echo -e "\n\033[0;36m========================================================\033[0m"
echo -e "\033[0;32m🎉 HỆ THỐNG ĐÃ SẴN SÀNG!\033[0m"
echo -e "Bây giờ bạn có thể:"
echo -e "  1. Chạy mô phỏng gửi ảnh từ camera: "
echo -e "     \033[0;33mpython ingestion/client.py\033[0m"
echo -e "  2. Kích hoạt Spark Job để xử lý dữ liệu: "
echo -e "     \033[0;33mdocker exec -e MINIO_ENDPOINT=minio:9000 -e RABBITMQ_HOST=rabbitmq -e CLICKHOUSE_HOST=clickhouse -t image_pipeline_spark_master spark-submit /opt/spark-jobs/spark_job.py\033[0m"
echo -e "\033[0;36m========================================================\033[0m"
