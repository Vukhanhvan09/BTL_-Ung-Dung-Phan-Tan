# Windows PowerShell script to automate starting the Traffic Monitoring Pipeline

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🚦 BẮT ĐẦU KHỞI ĐỘNG TRAFFIC MONITORING PIPELINE..." -ForegroundColor Cyan

Set-Location $PSScriptRoot

function Fail($message) {
    Write-Host "❌ $message" -ForegroundColor Red
    Exit 1
}

function Test-Port($port) {
    try {
        $client = New-Object System.Net.Sockets.TcpClient
        $client.Connect("localhost", $port)
        $client.Close()
        return $true
    } catch {
        return $false
    }
}

function Wait-Port($port, $name, $timeoutSeconds = 120) {
    Write-Host "   Đang chờ $name (cổng $port)..." -ForegroundColor Gray
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $timeoutSeconds) {
        if (Test-Port $port) {
            Write-Host "✅ $name sẵn sàng (cổng $port)." -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 2
    }
    Fail "$name không phản hồi trên cổng $port sau $timeoutSeconds giây. Kiểm tra bằng: docker compose logs"
}

function Test-HttpOk($url) {
    try {
        $resp = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 5
        return ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500)
    } catch {
        return $false
    }
}

function Wait-Http($url, $name, $timeoutSeconds = 120) {
    Write-Host "   Đang chờ HTTP $name ($url)..." -ForegroundColor Gray
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $timeoutSeconds) {
        if (Test-HttpOk $url) {
            Write-Host "✅ $name HTTP sẵn sàng." -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 3
    }
    Fail "$name không trả lời HTTP tại $url sau $timeoutSeconds giây."
}

function Assert-ContainerRunning($name) {
    $status = docker inspect -f "{{.State.Running}}" $name 2>$null
    if ($status -ne "true") {
        Fail "Container $name chưa Running. Kiểm tra: docker logs $name"
    }
    Write-Host "✅ Container $name đang Running." -ForegroundColor Green
}

# 0. Tìm Python trong venv
Write-Host "`n0. Kiểm tra môi trường Python (venv)..." -ForegroundColor Blue
$venvPython = @("$PSScriptRoot\venv\Scripts\python.exe", "$PSScriptRoot\..\venv\Scripts\python.exe") |
    Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $venvPython) {
    Fail "Không tìm thấy venv. Hãy tạo bằng: python -m venv venv"
}
& $venvPython -c "import grpc, minio, pika, clickhouse_connect, streamlit, plotly, pandas" 2>$null
if ($LASTEXITCODE -ne 0) {
    Fail "Thiếu thư viện Python trong venv. Chạy: $venvPython -m pip install -r requirements.txt"
}
Write-Host "✅ venv hợp lệ: $venvPython" -ForegroundColor Green

# 1. Docker
Write-Host "`n1. Kiểm tra Docker..." -ForegroundColor Blue
docker info >$null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Lỗi: Docker Desktop chưa được khởi chạy." -ForegroundColor Red
    Exit 1
}
Write-Host "✅ Docker đang hoạt động." -ForegroundColor Green

# 2. Compose
Write-Host "`n2. Khởi chạy các container nền..." -ForegroundColor Blue
if (-not (Test-Path "$PSScriptRoot\.env")) {
    Fail "Thiếu file .env (cần DETECTION_MODE=mock hoặc yolo)"
}
$workerReplicas = 3
if (Test-Path "$PSScriptRoot\.env") {
    Get-Content "$PSScriptRoot\.env" | ForEach-Object {
        if ($_ -match '^\s*SPARK_WORKER_REPLICAS\s*=\s*(\d+)') { $workerReplicas = [int]$Matches[1] }
    }
}
Write-Host "   Spark workers: $workerReplicas" -ForegroundColor Gray
docker compose up -d --scale "spark-worker=$workerReplicas"
if ($LASTEXITCODE -ne 0) { Fail "Lỗi khởi động docker compose." }

# 3. Chờ đủ mọi service quan trọng
Write-Host "`n3. Chờ toàn bộ service sẵn sàng..." -ForegroundColor Blue
Wait-Port 8123 "ClickHouse"
Wait-Http "http://localhost:8123/ping" "ClickHouse ping"
Wait-Port 5672 "RabbitMQ AMQP"
Wait-Port 15672 "RabbitMQ Management"
Wait-Port 9000 "MinIO API"
Wait-Http "http://localhost:9000/minio/health/live" "MinIO health"
Wait-Port 7077 "Spark Master RPC"
Wait-Port 8082 "Spark Master UI"
Wait-Http "http://localhost:8082/" "Spark Master UI"
Wait-Port 8085 "Airflow Webserver"
Wait-Http "http://localhost:8085/health" "Airflow health"

Assert-ContainerRunning "image_pipeline_rabbitmq"
Assert-ContainerRunning "image_pipeline_minio"
Assert-ContainerRunning "image_pipeline_clickhouse"
Assert-ContainerRunning "image_pipeline_spark_master"
Assert-ContainerRunning "image_pipeline_airflow_webserver"
Assert-ContainerRunning "image_pipeline_airflow_scheduler"
$workerCount = (docker ps --filter "name=van-tuananh-spark-worker" --filter "status=running" -q | Measure-Object).Count
if ($workerCount -lt 1) {
    # compose project name may vary; also match generic spark-worker
    $workerCount = (docker ps --filter "name=spark-worker" --filter "status=running" -q | Measure-Object).Count
}
if ($workerCount -lt 1) { Fail "Không có Spark worker nào đang Running" }
Write-Host "✅ Spark workers đang Running: $workerCount" -ForegroundColor Green

# 4. Spark deps (idempotent)
Write-Host "`n4. Đồng bộ thư viện Spark Master..." -ForegroundColor Blue
docker exec -u 0 -t image_pipeline_spark_master /opt/bitnami/python/bin/python -m pip install -r /opt/spark-jobs/requirements.txt
if ($LASTEXITCODE -ne 0) { Fail "Thất bại khi cài đặt thư viện vào Spark container." }
Write-Host "✅ Thư viện Spark OK." -ForegroundColor Green

# 5. gRPC Server
Write-Host "`n5. Đang chạy gRPC Server (ingestion/server.py)..." -ForegroundColor Blue
if (Test-Port 50051) {
    Write-Host "⚠️ Cổng 50051 đã được dùng, bỏ qua bước này." -ForegroundColor Yellow
} else {
    $grpc = Start-Process -FilePath $venvPython -ArgumentList "ingestion/server.py" -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput "$PSScriptRoot\ingestion_server.log" -RedirectStandardError "$PSScriptRoot\ingestion_server.err.log"
    Wait-Port 50051 "gRPC Server" 30
    Write-Host "   PID: $($grpc.Id) | Log: ingestion_server.err.log" -ForegroundColor Gray
}

# 6. Dashboard
Write-Host "`n6. Đang chạy Web Dashboard (dashboard.py)..." -ForegroundColor Blue
if (Test-Port 8501) {
    Write-Host "⚠️ Cổng 8501 đã được dùng, bỏ qua bước này." -ForegroundColor Yellow
} else {
    $dash = Start-Process -FilePath $venvPython -ArgumentList "-m","streamlit","run","dashboard.py","--server.headless","true" -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput "$PSScriptRoot\dashboard.log" -RedirectStandardError "$PSScriptRoot\dashboard.err.log"
    Wait-Port 8501 "Web Dashboard" 30
    Write-Host "   PID: $($dash.Id) | Log: dashboard.err.log" -ForegroundColor Gray
}
Start-Process "http://localhost:8501"

Write-Host "`n========================================================" -ForegroundColor Cyan
Write-Host "🎉 HỆ THỐNG ĐÃ SẴN SÀNG!" -ForegroundColor Green
Write-Host "  1. python ingestion/client.py" -ForegroundColor Yellow
Write-Host "  2. docker exec -e MINIO_ENDPOINT=minio:9000 -e RABBITMQ_HOST=rabbitmq -e CLICKHOUSE_HOST=clickhouse -t image_pipeline_spark_master spark-submit /opt/spark-jobs/spark_job.py" -ForegroundColor Yellow
Write-Host "========================================================" -ForegroundColor Cyan
