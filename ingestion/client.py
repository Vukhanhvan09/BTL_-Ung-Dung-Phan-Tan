"""
Camera Simulator entrypoint.

Uses CameraSimulatorManager:
  - CAMERA_MODE=synthetic|real
  - CAMERA_COUNT=N (optional subset of registry)
  - FRAMES_PER_CAMERA (default 2)
  - REAL_IMAGE_DIR for real mode
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROTO_DIR = CURRENT_DIR / "protos"
GEN_DIR = CURRENT_DIR / "generated"
PROTO_FILE = PROTO_DIR / "image_ingestion.proto"

sys.path.insert(0, str(CURRENT_DIR))
sys.path.insert(0, str(GEN_DIR))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

try:
    import grpc
    import image_ingestion_pb2  # noqa: F401
    import image_ingestion_pb2_grpc
except ImportError:
    from grpc_tools import protoc

    logger.info("Compiling Protobuf definitions in client...")
    GEN_DIR.mkdir(parents=True, exist_ok=True)
    protoc.main(
        (
            "",
            f"-I{PROTO_DIR}",
            f"--python_out={GEN_DIR}",
            f"--grpc_python_out={GEN_DIR}",
            str(PROTO_FILE),
        )
    )
    import grpc
    import image_ingestion_pb2  # noqa: F401
    import image_ingestion_pb2_grpc

from camera_manager import CameraSimulatorManager  # noqa: E402
from camera_registry import CameraRegistry  # noqa: E402


def sync_cameras_to_clickhouse(registry: CameraRegistry) -> None:
    """Upsert registry cameras into dim_cameras so dashboard map stays complete."""
    try:
        import clickhouse_connect
    except ImportError:
        logger.warning("clickhouse_connect missing; skip dim_cameras sync")
        return

    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "traffic_password")
    database = os.getenv("CLICKHOUSE_DB", "traffic_dw")

    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
        )
        rows = registry.clickhouse_rows()
        client.insert(
            "dim_cameras",
            rows,
            column_names=["camera_id", "location", "latitude", "longitude", "created_at"],
        )
        client.close()
        logger.info("Synced %d cameras into ClickHouse dim_cameras", len(rows))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not sync cameras to ClickHouse: %s", exc)


def run():
    target = os.getenv("GRPC_SERVER_ADDRESS", "localhost:50051")
    mode = os.getenv("CAMERA_MODE", "synthetic")
    logger.info("Connecting to gRPC server at %s (CAMERA_MODE=%s)...", target, mode)

    registry = CameraRegistry.from_json()
    sync_cameras_to_clickhouse(registry)

    manager = CameraSimulatorManager(registry=registry, mode=mode)
    logger.info(
        "Active virtual cameras (%d): %s",
        len(manager.list_cameras()),
        ", ".join(c.camera_id for c in manager.list_cameras()),
    )

    with grpc.insecure_channel(target) as channel:
        stub = image_ingestion_pb2_grpc.ImageIngestionServiceStub(channel)
        manager.run_simulation(stub)


if __name__ == "__main__":
    run()
