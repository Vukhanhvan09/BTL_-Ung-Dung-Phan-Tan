# Hướng dẫn cập nhật code mới

Dành cho bạn **đã chạy Docker bản cũ mượt**, giờ nhận bản code đã sửa.

Làm đúng thứ tự dưới là được.

---

## Trước khi làm

1. Docker Desktop đang mở (Engine running)
2. Bạn kia **đã push code mới lên Git** rồi
3. Vào đúng folder project (có file `docker-compose.yml`)

---

## Đã đổi gì? (siêu ngắn)

- Sửa lỗi cũ
- 8 camera, 3 worker xử lý
- Dashboard ổn hơn
- Spark web xem ở cổng **8082** (không còn 8080)

---

## Vì sao vẫn cần `pip` trên máy?

Không phải thứ gì cũng nằm trong Docker.

- **Trong Docker:** database, lưu ảnh, hàng đợi, Spark, Airflow  
- **Trên máy bạn:** trang dashboard + gửi ảnh giả lập (`client.py`)

Nên vẫn cần `pip install` cho phần trên máy.

---

## Làm lần lượt

### 1) Bỏ code local chưa commit

⚠️ Mất hết sửa chưa commit trên máy bạn.

```powershell
git reset --hard
git clean -fd
```

### 2) Lấy code mới

```powershell
git pull
```

### 3) Kiểm tra file `.env`

Mở file `.env` trong project, phải có dòng:

```env
DETECTION_MODE=mock
SPARK_WORKER_REPLICAS=3
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2G
SPARK_TOTAL_EXECUTOR_CORES=6
CAMERA_MODE=synthetic
CAMERA_COUNT=8
FRAMES_PER_CAMERA=2
```

Thiếu thì copy nguyên khối trên vào `.env` rồi lưu.

### 4) Cài lại thư viện trên máy

```powershell
.\venv\Scripts\activate
pip install -r requirements.txt
```

Chưa có `venv`:

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

### 5) Tắt Docker cũ + xóa data cũ + build lại

Lần cập nhật này nên **xóa data Docker cũ** cho sạch schema mới  
(mất dữ liệu demo cũ, không sao):

```powershell
docker compose down -v
docker compose build
.\start_pipeline.ps1
```

Đợi chạy xong (có thể vài phút).

### 6) Gửi dữ liệu + xử lý

```powershell
.\venv\Scripts\activate
python ingestion\client.py
```

```powershell
docker exec -e MINIO_ENDPOINT=minio:9000 -e RABBITMQ_HOST=rabbitmq -e CLICKHOUSE_HOST=clickhouse -e DETECTION_MODE=mock -t image_pipeline_spark_master spark-submit --master spark://spark-master:7077 --total-executor-cores 6 /opt/spark-jobs/spark_job.py
```

### 7) Xem kết quả

- Dashboard: http://localhost:8501  
- Spark (thấy 3 worker): http://localhost:8082  

---

## Checklist copy nhanh

1. `git reset --hard`  
2. `git clean -fd`  
3. `git pull`  
4. Check `.env` (có `DETECTION_MODE=mock`…)  
5. `pip install -r requirements.txt`  
6. `docker compose down -v`  
7. `docker compose build`  
8. `.\start_pipeline.ps1`  
9. `python ingestion\client.py`  
10. lệnh `docker exec ... spark-submit ...`  
11. mở http://localhost:8501  

---

## Nếu lỗi

| Lỗi | Cách xử lý |
|---|---|
| `git pull` lỗi | Làm lại bước 1–2 |
| Báo thiếu `DETECTION_MODE` | Sửa `.env` như mục 3 |
| Web không lên | Docker còn xanh? Chạy lại `.\start_pipeline.ps1`, đợi 1–2 phút, F5 |
| Cổng đang bị chiếm | Tắt dashboard/server cũ, chạy lại `start_pipeline` |

---

## Nhớ hộ

- Bản cũ chạy mượt ≠ bản mới tự lên: phải **pull + build + down -v** 1 lần  
- Dashboard/client vẫn chạy trên máy → cần **pip**  
- Xem Spark ở **8082**, dashboard ở **8501**
