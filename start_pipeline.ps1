# Windows PowerShell script to automate starting the Traffic Monitoring Pipeline

# Set console to UTF8 to print Vietnamese characters cleanly
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🚦 BẮT ĐẦU KHỞI ĐỘNG TRAFFIC MONITORING PIPELINE..." -ForegroundColor Cyan

# 1. Kiểm tra Docker Daemon có đang chạy không
Write-Host "`n1. Kiểm tra Docker..." -ForegroundColor Blue
docker info >$null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Lỗi: Docker Desktop chưa được khởi chạy. Vui lòng mở Docker Desktop trước khi tiếp tục." -ForegroundColor Red
    Exit 1
}
Write-Host "✅ Docker đang hoạt động." -ForegroundColor Green

# 2. Khởi động Docker Compose
Write-Host "`n2. Khởi chạy các container nền (ClickHouse, MinIO, RabbitMQ, Spark)..." -ForegroundColor Blue
docker-compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Lỗi khởi động docker-compose." -ForegroundColor Red
    Exit 1
}

# 3. Đợi các dịch vụ quan trọng sẵn sàng
Write-Host "`n3. Chờ đợi ClickHouse và RabbitMQ khởi động hoàn tất (10 giây)..." -ForegroundColor Blue
Start-Sleep -Seconds 10

# 4. Cài đặt thư viện Python trong Spark Master
Write-Host "`n4. Cài đặt các thư viện cần thiết vào Spark Master container..." -ForegroundColor Blue
docker exec -u 0 -t image_pipeline_spark_master pip install minio pika clickhouse-connect Pillow
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Thất bại khi cài đặt thư viện vào Spark container." -ForegroundColor Red
} else {
    Write-Host "✅ Cài đặt thư viện Spark thành công." -ForegroundColor Green
}

# 5. Khởi động gRPC Server trong tiến trình nền
Write-Host "`n5. Đang chạy gRPC Server (ingestion/server.py) trong nền..." -ForegroundColor Blue
if (Test-Path ".\venv\Scripts\python.exe") {
    Start-Process -NoNewWindow -FilePath ".\venv\Scripts\python.exe" -ArgumentList "ingestion/server.py"
    Write-Host "✅ gRPC Server đã được khởi động." -ForegroundColor Green
} else {
    Write-Host "⚠️ Không tìm thấy venv/Scripts/python.exe. Hãy chắc chắn venv đã được tạo." -ForegroundColor Yellow
}

# 6. Khởi động Web Dashboard
Write-Host "`n6. Đang mở Web Dashboard (dashboard.py) trên trình duyệt..." -ForegroundColor Blue
if (Test-Path ".\venv\Scripts\python.exe") {
    Start-Process -FilePath ".\venv\Scripts\python.exe" -ArgumentList "-m streamlit run dashboard.py"
    Write-Host "✅ Web Dashboard đang được mở tại http://localhost:8501." -ForegroundColor Green
} else {
    Write-Host "⚠️ Không tìm thấy venv/Scripts/python.exe. Vui lòng chạy thủ công: python -m streamlit run dashboard.py" -ForegroundColor Yellow
}

Write-Host "`n========================================================" -ForegroundColor Cyan
Write-Host "🎉 HỆ THỐNG ĐÃ SẴN SÀNG!" -ForegroundColor Green
Write-Host "Bây giờ bạn có thể:" -ForegroundColor White
Write-Host "  1. Chạy mô phỏng gửi ảnh từ camera: " -ForegroundColor White
Write-Host "     python ingestion/client.py" -ForegroundColor Yellow
Write-Host "  2. Kích hoạt Spark Job để xử lý dữ liệu: " -ForegroundColor White
Write-Host "     docker exec -e MINIO_ENDPOINT=minio:9000 -e RABBITMQ_HOST=rabbitmq -e CLICKHOUSE_HOST=clickhouse -t image_pipeline_spark_master spark-submit /opt/spark-jobs/spark_job.py" -ForegroundColor Yellow
Write-Host "========================================================" -ForegroundColor Cyan
