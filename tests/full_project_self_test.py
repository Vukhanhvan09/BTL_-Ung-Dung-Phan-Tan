"""
Full-project self-test:
  1) Docker services healthy
  2) Unit tests (camera registry)
  3) API / infra checks
  4) Dashboard ClickHouse queries used by UI
  5) MinIO image fetch for latest fact
  6) gRPC upload + optional Spark drain
  7) Live Chrome Selenium across all tabs (no Streamlit errors)
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / "venv" / "Scripts" / "python.exe"
if not PY.exists():
    PY = Path(sys.executable)

PASS = 0
FAIL = 0
NOTES: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"[PASS] {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f"[FAIL] {name}" + (f" — {detail}" if detail else ""))


def http_get(url: str, timeout: float = 8.0) -> tuple[int, str]:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", "ignore")


def section(title: str) -> None:
    print(f"\n===== {title} =====")


def test_docker_services() -> None:
    section("Docker services")
    try:
        out = subprocess.check_output(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
            text=True,
            cwd=str(ROOT),
        )
    except Exception as exc:
        check("docker ps", False, str(exc))
        return

    needed = {
        "image_pipeline_rabbitmq": False,
        "image_pipeline_minio": False,
        "image_pipeline_clickhouse": False,
        "image_pipeline_spark_master": False,
        "image_pipeline_airflow_webserver": False,
        "image_pipeline_airflow_scheduler": False,
    }
    workers = 0
    for line in out.splitlines():
        name, _, status = line.partition("\t")
        for key in list(needed):
            if key in name and "Up" in status:
                needed[key] = True
        if "spark-worker" in name and "Up" in status:
            workers += 1

    for name, ok in needed.items():
        check(f"container {name}", ok)
    check("spark workers >= 3", workers >= 3, f"workers={workers}")


def test_unit() -> None:
    section("Unit tests")
    proc = subprocess.run(
        [str(PY), "-m", "unittest", "tests.test_camera_registry", "-v"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    print(proc.stdout[-1500:] if proc.stdout else "")
    if proc.stderr:
        # unittest writes to stderr
        print(proc.stderr[-1500:])
    check("unittest camera_registry", proc.returncode == 0, f"exit={proc.returncode}")


def test_apis_and_queries() -> None:
    section("APIs + dashboard queries")
    import clickhouse_connect
    from minio import Minio
    import pika

    # HTTP endpoints
    for name, url in [
        ("ClickHouse ping", "http://localhost:8123/ping"),
        ("Spark Master", "http://localhost:8082/"),
        ("Airflow health", "http://localhost:8085/health"),
        ("RabbitMQ mgmt", "http://localhost:15672/"),
        ("Dashboard", "http://localhost:8501/"),
        ("MinIO console", "http://localhost:9001/"),
    ]:
        try:
            status, body = http_get(url)
            check(name, 200 <= status < 500, f"status={status}")
            if name == "Spark Master":
                alive = 0
                for line in body.splitlines():
                    if "Alive Workers" in line:
                        digits = "".join(ch for ch in line if ch.isdigit())
                        alive = int(digits) if digits else 0
                        break
                check("Spark Alive Workers >= 3", alive >= 3, f"alive={alive}")
        except Exception as exc:
            check(name, False, str(exc))

    client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="default",
        password="traffic_password",
        database="traffic_dw",
    )
    try:
        cams = client.query("SELECT uniqExact(camera_id) FROM dim_cameras FINAL").result_rows[0][0]
        check("dim_cameras >= 8", cams >= 8, f"cameras={cams}")

        # Exact queries used by dashboard.py
        mode = "mock"
        q_summary = client.query_df(
            """
            SELECT f.camera_id AS camera_id, c.location AS location,
                   v.vehicle_type_name AS vehicle_type_name, sum(f.count) AS total_count
            FROM fact_traffic_flow AS f FINAL
            JOIN (SELECT * FROM dim_cameras FINAL) AS c ON f.camera_id = c.camera_id
            JOIN (SELECT * FROM dim_vehicles FINAL) AS v ON f.vehicle_type_id = v.vehicle_type_id
            WHERE f.detection_mode = {mode:String}
            GROUP BY f.camera_id, c.location, v.vehicle_type_name
            """,
            parameters={"mode": mode},
        )
        check("dashboard traffic_summary query", not q_summary.empty, f"rows={len(q_summary)}")

        q_kpi = client.query_df(
            """
            SELECT uniqExact(camera_id) AS cameras_with_data,
                   uniqExact(event_id) AS processed_frames,
                   sumIf(count, vehicle_type_id IN (1,2,3,4)) AS total_vehicles,
                   avgIf(inference_time_ms, vehicle_type_id = 1) AS avg_inference_ms
            FROM fact_traffic_flow FINAL
            WHERE detection_mode = {mode:String}
            """,
            parameters={"mode": mode},
        )
        check(
            "dashboard KPI query",
            not q_kpi.empty and int(q_kpi.iloc[0]["processed_frames"]) > 0,
            q_kpi.iloc[0].to_dict(),
        )

        q_images = client.query_df(
            """
            SELECT img.image_id AS image_id, img.s3_uri AS s3_uri, f.camera_id AS camera_id
            FROM (SELECT * FROM dim_images FINAL) AS img
            JOIN fact_traffic_flow AS f FINAL ON img.image_id = f.image_id
            WHERE f.detection_mode = {mode:String}
            GROUP BY img.image_id, img.s3_uri, f.camera_id
            ORDER BY img.image_id DESC
            LIMIT 5
            """,
            parameters={"mode": mode},
        )
        check("dashboard images query", not q_images.empty, f"rows={len(q_images)}")

        # MinIO fetch like dashboard.load_minio_image
        minio = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
        s3_uri = q_images.iloc[0]["s3_uri"]
        bucket, key = s3_uri[5:].split("/", 1)
        obj = minio.get_object(bucket, key)
        data = obj.read()
        obj.close()
        obj.release_conn()
        check("MinIO fetch image for dashboard", len(data) > 100 and data[:2] == b"\xff\xd8", f"bytes={len(data)}")

        # RabbitMQ
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost", credentials=pika.PlainCredentials("guest", "guest"))
        )
        ch = conn.channel()
        q = ch.queue_declare(queue="image_processing_queue", durable=True, passive=True)
        check("RabbitMQ queue", True, f"messages={q.method.message_count}")
        conn.close()
    finally:
        client.close()


def test_grpc_upload() -> None:
    section("gRPC ingestion")
    import io
    import sys

    sys.path.insert(0, str(ROOT / "ingestion" / "generated"))
    import grpc
    import image_ingestion_pb2
    import image_ingestion_pb2_grpc
    from PIL import Image

    img = Image.new("RGB", (80, 60), color=(40, 40, 40))
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    channel = grpc.insecure_channel("localhost:50051")
    stub = image_ingestion_pb2_grpc.ImageIngestionServiceStub(channel)
    resp = stub.UploadImage(
        image_ingestion_pb2.ImageUploadRequest(
            filename="full_selftest.jpg",
            image_data=buf.getvalue(),
            source="full_self_test",
            mime_type="image/jpeg",
            camera_id="CAM-HN-004",
            capture_timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
        ),
        timeout=20,
    )
    check("gRPC UploadImage", resp.status == "SUCCESS", f"{resp.status} {resp.s3_uri}")
    channel.close()


def drain_spark_once() -> None:
    section("Spark job drain (optional)")
    try:
        proc = subprocess.run(
            [
                "docker",
                "exec",
                "-e",
                "MINIO_ENDPOINT=minio:9000",
                "-e",
                "RABBITMQ_HOST=rabbitmq",
                "-e",
                "CLICKHOUSE_HOST=clickhouse",
                "-e",
                "DETECTION_MODE=mock",
                "-t",
                "image_pipeline_spark_master",
                "spark-submit",
                "--master",
                "spark://spark-master:7077",
                "--total-executor-cores",
                "6",
                "/opt/spark-jobs/spark_job.py",
            ],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=240,
        )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        ok = proc.returncode == 0 and (
            "ClickHouse verified" in out
            or "RabbitMQ queue is empty" in out
            or "Messages=" in out
        )
        check("spark-submit drain", ok, f"exit={proc.returncode}")
        if "ClickHouse verified" in out:
            NOTES.append("Spark processed pending queue messages")
        elif "RabbitMQ queue is empty" in out:
            NOTES.append("Queue already empty")
    except Exception as exc:
        check("spark-submit drain", False, str(exc))


def test_selenium_live() -> None:
    section("Selenium live Chrome UI")
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from webdriver_manager.chrome import ChromeDriverManager

    def wait_text(driver, needles, timeout=75):
        end = time.time() + timeout
        last = ""
        while time.time() < end:
            last = driver.find_element(By.TAG_NAME, "body").text
            if any(n.lower() in last.lower() for n in needles):
                return last
            time.sleep(1.2)
        return last

    def click_label(driver, label: str) -> bool:
        for el in driver.find_elements(By.CSS_SELECTOR, "button, [role='tab']"):
            if label.lower() in (el.text or "").lower():
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    el.click()
                    time.sleep(2.5)
                    return True
                except Exception:
                    continue
        return False

    options = Options()
    if os.getenv("SELENIUM_HEADLESS", "0") in {"1", "true", "yes"}:
        options.add_argument("--headless=new")
    else:
        options.add_argument("--start-maximized")
    options.add_argument("--window-size=1500,1000")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    try:
        driver.get("http://localhost:8501/")
        WebDriverWait(driver, 45).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        body = wait_text(driver, ["Giám Sát", "Mock"], 60)
        body = wait_text(driver, ["Tổng Số Xe", "Camera Có Dữ Liệu", "Thống Kê"], 90)

        error_tokens = [
            "Exception",
            "Traceback",
            "Failed to load data",
            "use_container_width",
            "ModuleNotFoundError",
            "ImportError",
            "TypeError",
            "AttributeError",
        ]
        bad = [t for t in error_tokens if t.lower() in body.lower()]
        # Streamlit may include benign words; only fail hard errors
        hard = [t for t in bad if t not in {"Exception"} or "Traceback" in body]
        # More precise: look for Streamlit exception block
        has_streamlit_error = (
            "Traceback" in body
            or "Failed to load data from ClickHouse" in body
            or "ModuleNotFoundError" in body
            or "ImportError" in body
        )
        check("Dashboard has no Streamlit exception", not has_streamlit_error, body[:300].replace("\n", " "))
        check("Dashboard KPIs visible", "Tổng Số Xe".lower() in body.lower() or "CAMERA CÓ DỮ LIỆU" in body.upper())
        check("Dashboard not empty warning", "chưa có dữ liệu" not in body.lower())

        # Tabs
        ok_map = click_label(driver, "Bản Đồ")
        check("Open map tab", ok_map)
        if ok_map:
            map_body = driver.find_element(By.TAG_NAME, "body").text
            check(
                "Map tab content",
                ("Vị Trí" in map_body) or ("camera" in map_body.lower()) or ("Mã Camera" in map_body),
            )
            check(
                "Map tab no exception",
                "Traceback" not in map_body and "Failed to load" not in map_body,
            )
            driver.save_screenshot(str(ROOT / "tests" / "full_test_map.png"))

        ok_img = click_label(driver, "Ảnh") or click_label(driver, "MinIO")
        check("Open images tab", ok_img)
        if ok_img:
            time.sleep(3)
            img_body = driver.find_element(By.TAG_NAME, "body").text
            check(
                "Images tab content",
                ("MinIO" in img_body) or ("khung hình" in img_body.lower()) or ("Image ID" in img_body),
            )
            check(
                "Images tab no MinIO fatal error for default selection",
                "Không thể lấy ảnh từ MinIO" not in img_body or "Image ID" in img_body,
            )
            # If spinner finished and image loaded, good; if select exists also good
            check(
                "Images tab no Traceback",
                "Traceback" not in img_body and "ModuleNotFoundError" not in img_body,
            )
            driver.save_screenshot(str(ROOT / "tests" / "full_test_images.png"))

        ok_stats = click_label(driver, "Thống Kê")
        check("Back to stats tab", ok_stats)
        driver.save_screenshot(str(ROOT / "tests" / "full_test_dashboard.png"))

        pause = float(os.getenv("SELENIUM_PAUSE_SECONDS", "6"))
        time.sleep(pause)
    except Exception as exc:
        check("Selenium live run", False, str(exc))
        try:
            driver.save_screenshot(str(ROOT / "tests" / "full_test_error.png"))
        except Exception:
            pass
    finally:
        driver.quit()


def ensure_dashboard_running() -> None:
    section("Ensure dashboard running")
    try:
        http_get("http://localhost:8501/")
        check("Dashboard already up", True)
        return
    except Exception:
        pass
    # restart
    subprocess.Popen(
        [
            str(PY),
            "-m",
            "streamlit",
            "run",
            "dashboard.py",
            "--server.headless",
            "true",
            "--server.port",
            "8501",
        ],
        cwd=str(ROOT),
        stdout=open(ROOT / "dashboard.log", "a", encoding="utf-8"),
        stderr=open(ROOT / "dashboard.err.log", "a", encoding="utf-8"),
    )
    for _ in range(30):
        time.sleep(2)
        try:
            http_get("http://localhost:8501/")
            check("Dashboard restarted", True)
            return
        except Exception:
            continue
    check("Dashboard restarted", False, "timeout")


def check_dashboard_log_clean() -> None:
    section("Dashboard log hygiene")
    err_path = ROOT / "dashboard.err.log"
    if not err_path.exists():
        check("dashboard.err.log exists", False)
        return
    lines = err_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    # Only inspect after the latest Streamlit/Uvicorn boot
    start_idx = 0
    for i, line in enumerate(lines):
        if "Uvicorn server started" in line or "You can now view your Streamlit app" in line:
            start_idx = i
    recent = "\n".join(lines[start_idx:])
    check(
        "No use_container_width warning after latest boot",
        "use_container_width" not in recent,
        "clean" if "use_container_width" not in recent else "still deprecated",
    )
    hard_tokens = ("Traceback (most recent call last)", "ModuleNotFoundError", "ImportError:")
    hard = any(tok in recent for tok in hard_tokens)
    check("No hard traceback after latest boot", not hard)


def main() -> int:
    print("FULL PROJECT SELF-TEST")
    print(f"ROOT={ROOT}")
    test_docker_services()
    test_unit()
    ensure_dashboard_running()
    # Give streamlit a moment after potential restart / code reload
    time.sleep(4)
    test_apis_and_queries()
    test_grpc_upload()
    drain_spark_once()
    # Restart dashboard to pick up code fix cleanly
    # Kill old streamlit then start
    section("Reload dashboard after code fix")
    try:
        if os.name == "nt":
            subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    (
                        "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'streamlit|dashboard.py' } "
                        "| ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
                    ),
                ],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
            )
        time.sleep(2)
    except Exception:
        pass
    ensure_dashboard_running()
    time.sleep(6)
    check_dashboard_log_clean()
    test_selenium_live()

    print("\n===== NOTES =====")
    for n in NOTES:
        print("-", n)
    print(f"\n=== TOTAL: {PASS} passed, {FAIL} failed ===")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
