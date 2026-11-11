"""Self-test APIs after camera + Spark worker expansion."""
from __future__ import annotations

import time
import urllib.request
from pathlib import Path

import clickhouse_connect
import pika
from minio import Minio

PASS = 0
FAIL = 0
ROOT = Path(__file__).resolve().parents[1]


def check(name: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"[PASS] {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f"[FAIL] {name}" + (f" — {detail}" if detail else ""))


def http_ok(url: str, timeout: float = 5.0) -> tuple[bool, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read(2000).decode("utf-8", "ignore")
            return True, f"status={resp.status}; body_len={len(body)}"
    except Exception as exc:
        return False, str(exc)


def main() -> int:
    print("=== Self-test APIs (expanded cameras + workers) ===")

    ok, detail = http_ok("http://localhost:8123/ping")
    check("ClickHouse /ping", ok, detail)

    try:
        client = clickhouse_connect.get_client(
            host="localhost",
            port=8123,
            username="default",
            password="traffic_password",
            database="traffic_dw",
        )
        tables = {r[0] for r in client.query("SHOW TABLES").result_rows}
        expected = {"dim_cameras", "dim_vehicles", "dim_images", "fact_traffic_flow"}
        check("ClickHouse tables", expected.issubset(tables), str(sorted(tables)))

        engine = client.query(
            "SELECT engine FROM system.tables WHERE database='traffic_dw' AND name='fact_traffic_flow'"
        ).result_rows
        check(
            "fact_traffic_flow ReplacingMergeTree",
            bool(engine) and "Replacing" in engine[0][0],
            engine[0][0] if engine else "missing",
        )

        cam_count = client.query(
            "SELECT uniqExact(camera_id) FROM dim_cameras FINAL"
        ).result_rows[0][0]
        check("dim_cameras expanded (>=8)", cam_count >= 8, f"cameras={cam_count}")

        facts = client.query(
            "SELECT count(), uniqExact(event_id), uniqExact(camera_id) FROM fact_traffic_flow FINAL WHERE detection_mode='mock'"
        ).result_rows[0]
        check(
            "ClickHouse mock facts present",
            facts[0] > 0 and facts[1] > 0,
            f"rows={facts[0]} events={facts[1]} cameras={facts[2]}",
        )
        check(
            "Facts cover multiple cameras (>=3)",
            facts[2] >= 3,
            f"cameras_in_facts={facts[2]}",
        )
        client.close()
    except Exception as exc:
        check("ClickHouse query", False, str(exc))

    try:
        minio = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
        buckets = [b.name for b in minio.list_buckets()]
        check("MinIO reachable", True, f"buckets={buckets}")
        check("MinIO raw-images bucket", "raw-images" in buckets, str(buckets))
        if "raw-images" in buckets:
            objs = list(minio.list_objects("raw-images", recursive=True))
            check("MinIO has uploaded objects", len(objs) > 0, f"objects={len(objs)}")
    except Exception as exc:
        check("MinIO", False, str(exc))

    try:
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(
                host="localhost",
                credentials=pika.PlainCredentials("guest", "guest"),
            )
        )
        ch = conn.channel()
        q = ch.queue_declare(queue="image_processing_queue", durable=True, passive=True)
        check("RabbitMQ queue exists", True, f"messages={q.method.message_count}")
        conn.close()
    except Exception as exc:
        check("RabbitMQ", False, str(exc))

    ok, detail = http_ok("http://localhost:8082/")
    check("Spark Master UI :8082", ok, detail)
    if ok:
        try:
            html = urllib.request.urlopen("http://localhost:8082/", timeout=5).read().decode("utf-8", "ignore")
            alive = 0
            for line in html.splitlines():
                if "Alive Workers" in line:
                    digits = "".join(ch for ch in line if ch.isdigit())
                    alive = int(digits) if digits else 0
                    break
            check("Spark Alive Workers >= 3", alive >= 3, f"alive={alive}")
        except Exception as exc:
            check("Spark Alive Workers >= 3", False, str(exc))

    ok, detail = http_ok("http://localhost:8085/health")
    check("Airflow health :8085", ok, detail)

    ok, detail = http_ok("http://localhost:15672/")
    check("RabbitMQ Management :15672", ok, detail)

    ok, detail = http_ok("http://localhost:8501/")
    check("Streamlit Dashboard :8501", ok, detail)

    # Registry file present for both host + spark
    check("config/cameras.json exists", (ROOT / "config" / "cameras.json").is_file())
    check(
        "spark/cameras_registry.json exists",
        (ROOT / "spark" / "cameras_registry.json").is_file(),
    )

    # gRPC UploadImage smoke
    try:
        import io
        import sys

        gen = ROOT / "ingestion" / "generated"
        sys.path.insert(0, str(gen))
        import grpc
        import image_ingestion_pb2
        import image_ingestion_pb2_grpc
        from PIL import Image

        img = Image.new("RGB", (64, 64), color=(80, 80, 80))
        buf = io.BytesIO()
        img.save(buf, format="JPEG")
        channel = grpc.insecure_channel("localhost:50051")
        stub = image_ingestion_pb2_grpc.ImageIngestionServiceStub(channel)
        resp = stub.UploadImage(
            image_ingestion_pb2.ImageUploadRequest(
                filename="selftest_multi.jpg",
                image_data=buf.getvalue(),
                source="self_test",
                mime_type="image/jpeg",
                camera_id="CAM-HN-008",
                capture_timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
            ),
            timeout=15,
        )
        check(
            "gRPC UploadImage CAM-HN-008",
            resp.status == "SUCCESS",
            f"status={resp.status} s3={resp.s3_uri}",
        )
        channel.close()
    except Exception as exc:
        check("gRPC UploadImage CAM-HN-008", False, str(exc))

    print(f"\n=== Result: {PASS} passed, {FAIL} failed ===")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
