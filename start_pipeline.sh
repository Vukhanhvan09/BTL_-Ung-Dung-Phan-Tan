#!/bin/bash

echo -e "\033[0;36m🚦 BẮT ĐẦU KHỞI ĐỘNG TRAFFIC MONITORING PIPELINE...\033[0m"

cd "$(dirname "$0")" || exit 1

fail() { echo -e "\033[0;31m❌ $1\033[0m"; exit 1; }

port_open() { (echo > /dev/tcp/127.0.0.1/"$1") >/dev/null 2>&1; }

wait_port() {
    local port=$1 name=$2 timeout=${3:-120} waited=0
    echo "   Đang chờ $name (cổng $port)..."
    until port_open "$port"; do
        sleep 2
        waited=$((waited + 2))
        [ "$waited" -ge "$timeout" ] && fail "$name không phản hồi trên cổng $port sau ${timeout} giây. Kiểm tra: docker compose logs"
    done
    echo -e "\033[0;32m✅ $name sẵn sàng (cổng $port).\033[0m"
}

wait_http() {
    local url=$1 name=$2 timeout=${3:-120} waited=0
    echo "   Đang chờ HTTP $name ($url)..."
    until curl -sf "$url" >/dev/null 2>&1; do
        sleep 3
        waited=$((waited + 3))
        [ "$waited" -ge "$timeout" ] && fail "$name không trả lời HTTP tại $url"
    done
    echo -e "\033[0;32m✅ $name HTTP sẵn sàng.\033[0m"
}

assert_running() {
    local name=$1
    local running
    running=$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null || echo false)
    [ "$running" = "true" ] || fail "Container $name chưa Running"
    echo -e "\033[0;32m✅ Container $name đang Running.\033[0m"
}

PYTHON=""
for candidate in ./venv/bin/python ./venv/Scripts/python.exe ../venv/bin/python ../venv/Scripts/python.exe; do
    if [ -f "$candidate" ]; then PYTHON="$candidate"; break; fi
done
[ -z "$PYTHON" ] && fail "Không tìm thấy venv. Hãy tạo bằng: python -m venv venv"
"$PYTHON" -c "import grpc, minio, pika, clickhouse_connect, streamlit, plotly, pandas" 2>/dev/null \
    || fail "Thiếu thư viện Python trong venv. Chạy: $PYTHON -m pip install -r requirements.txt"
echo -e "\033[0;32m✅ venv hợp lệ: $PYTHON\033[0m"

echo -e "\n\033[0;34m1. Kiểm tra Docker...\033[0m"
docker info >/dev/null 2>&1 || fail "Docker Desktop chưa được khởi chạy."
echo -e "\033[0;32m✅ Docker đang hoạt động.\033[0m"

[ -f .env ] || fail "Thiếu file .env (cần DETECTION_MODE=mock hoặc yolo)"

echo -e "\n\033[0;34m2. Khởi chạy các container nền...\033[0m"
WORKER_REPLICAS=$(grep -E '^\s*SPARK_WORKER_REPLICAS=' .env 2>/dev/null | cut -d= -f2 | tr -d ' \r')
WORKER_REPLICAS=${WORKER_REPLICAS:-3}
echo "   Spark workers: $WORKER_REPLICAS"
docker compose up -d --scale "spark-worker=${WORKER_REPLICAS}" || fail "Lỗi khởi động docker compose."

echo -e "\n\033[0;34m3. Chờ toàn bộ service sẵn sàng...\033[0m"
wait_port 8123 "ClickHouse"
wait_http "http://localhost:8123/ping" "ClickHouse ping"
wait_port 5672 "RabbitMQ AMQP"
wait_port 15672 "RabbitMQ Management"
wait_port 9000 "MinIO API"
wait_http "http://localhost:9000/minio/health/live" "MinIO health"
wait_port 7077 "Spark Master RPC"
wait_port 8082 "Spark Master UI"
wait_http "http://localhost:8082/" "Spark Master UI"
wait_port 8085 "Airflow Webserver"
wait_http "http://localhost:8085/health" "Airflow health"

assert_running image_pipeline_rabbitmq
assert_running image_pipeline_minio
assert_running image_pipeline_clickhouse
assert_running image_pipeline_spark_master
assert_running image_pipeline_airflow_webserver
assert_running image_pipeline_airflow_scheduler
WORKER_COUNT=$(docker ps --filter "name=spark-worker" --filter "status=running" -q | wc -l | tr -d ' ')
[ "$WORKER_COUNT" -ge 1 ] || fail "Không có Spark worker nào đang Running"
echo -e "\033[0;32m✅ Spark workers đang Running: $WORKER_COUNT\033[0m"

echo -e "\n\033[0;34m4. Đồng bộ thư viện Spark Master...\033[0m"
docker exec -u 0 -t image_pipeline_spark_master /opt/bitnami/python/bin/python -m pip install -r /opt/spark-jobs/requirements.txt \
    || fail "Thất bại khi cài đặt thư viện vào Spark container."
echo -e "\033[0;32m✅ Thư viện Spark OK.\033[0m"

echo -e "\n\033[0;34m5. Đang chạy gRPC Server...\033[0m"
if port_open 50051; then
    echo -e "\033[0;33m⚠️ Cổng 50051 đã được dùng, bỏ qua bước này.\033[0m"
else
    "$PYTHON" ingestion/server.py > ingestion_server.log 2>&1 &
    echo "   PID: $! | Log: ingestion_server.log"
    wait_port 50051 "gRPC Server" 30
fi

echo -e "\n\033[0;34m6. Đang chạy Web Dashboard...\033[0m"
if port_open 8501; then
    echo -e "\033[0;33m⚠️ Cổng 8501 đã được dùng, bỏ qua bước này.\033[0m"
else
    "$PYTHON" -m streamlit run dashboard.py --server.headless true > dashboard.log 2>&1 &
    echo "   PID: $! | Log: dashboard.log"
    wait_port 8501 "Web Dashboard" 30
fi

echo -e "\n\033[0;36m========================================================\033[0m"
echo -e "\033[0;32m🎉 HỆ THỐNG ĐÃ SẴN SÀNG!\033[0m"
echo -e "  1. \033[0;33mpython ingestion/client.py\033[0m"
echo -e "  2. \033[0;33mdocker exec -e MINIO_ENDPOINT=minio:9000 -e RABBITMQ_HOST=rabbitmq -e CLICKHOUSE_HOST=clickhouse -t image_pipeline_spark_master spark-submit /opt/spark-jobs/spark_job.py\033[0m"
echo -e "\033[0;36m========================================================\033[0m"
