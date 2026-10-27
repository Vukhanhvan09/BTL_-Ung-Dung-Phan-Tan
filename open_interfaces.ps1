# Open Traffic-Monitoring management interfaces

$urls = @(
    "http://localhost:8085"   # Airflow
    "http://localhost:8080"   # Spark Master
    "http://localhost:8081"   # Spark Worker
    "http://localhost:15672"  # RabbitMQ
    "http://localhost:9001"   # MinIO
    "http://localhost:8501"   # Streamlit Dashboard
)

foreach ($url in $urls) {
    Start-Process $url
}

Write-Host "All management interfaces have been opened."
