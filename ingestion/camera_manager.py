"""Camera Simulator Manager — Synthetic Mode / Real Image Mode."""
from __future__ import annotations

import io
import logging
import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    from .camera_registry import Camera, CameraRegistry, apply_camera_traits
except ImportError:
    from camera_registry import Camera, CameraRegistry, apply_camera_traits

logger = logging.getLogger(__name__)

try:
    from PIL import Image, ImageDraw

    HAS_PILLOW = True
except ImportError:
    HAS_PILLOW = False


SUPPORTED_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}


def create_dummy_traffic_image(camera_id: str, vehicle_counts: dict) -> bytes:
    if not HAS_PILLOW:
        return bytes([random.randint(0, 255) for _ in range(5000)])

    img = Image.new("RGB", (640, 480), color=(50, 50, 50))
    d = ImageDraw.Draw(img)
    d.line([(318, 0), (318, 480)], fill=(255, 215, 0), width=3)
    d.line([(322, 0), (322, 480)], fill=(255, 215, 0), width=3)
    for y in range(0, 480, 40):
        d.line([(160, y), (160, y + 20)], fill=(255, 255, 255), width=2)
        d.line([(480, y), (480, y + 20)], fill=(255, 255, 255), width=2)

    colors = {
        "Car": (0, 102, 204),
        "Motorcycle": (0, 204, 102),
        "Bus": (204, 0, 0),
        "Truck": (204, 153, 0),
        "Pedestrian": (255, 255, 0),
    }
    y_offset = 30
    for v_type, count in vehicle_counts.items():
        if count == 0:
            continue
        color = colors.get(v_type, (128, 128, 128))
        for _ in range(min(count, 5)):
            x = random.randint(30, 580)
            y = random.randint(y_offset, min(y_offset + 80, 430))
            if v_type == "Car":
                d.rectangle([x, y, x + 45, y + 25], fill=color, outline=(255, 255, 255))
                d.rectangle([x + 10, y + 5, x + 35, y + 20], fill=(200, 220, 255))
            elif v_type == "Bus":
                d.rectangle([x, y, x + 70, y + 30], fill=color, outline=(255, 255, 255))
            elif v_type == "Motorcycle":
                d.ellipse([x, y, x + 15, y + 15], fill=color)
                d.line([(x, y + 7), (x + 20, y + 7)], fill=(200, 200, 200), width=2)
            elif v_type == "Truck":
                d.rectangle([x, y, x + 60, y + 28], fill=color, outline=(255, 255, 255))
                d.rectangle([x + 45, y + 2, x + 58, y + 26], fill=(100, 100, 100))
            elif v_type == "Pedestrian":
                d.ellipse([x, y, x + 10, y + 10], fill=color)
                d.line([(x + 5, y + 10), (x + 5, y + 25)], fill=color, width=2)
        y_offset = (y_offset + 90) % 400

    d.rectangle([10, 10, 360, 45], fill=(0, 0, 0))
    d.text((20, 15), f"Camera: {camera_id} | Synthetic", fill=(255, 255, 255))
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def get_traffic_density(camera: Camera | None, camera_id: str, current_hour: int) -> dict:
    is_peak = (7 <= current_hour <= 9) or (17 <= current_hour <= 19)
    is_night = (current_hour >= 22) or (current_hour <= 5)

    if is_peak:
        counts = {
            "Car": random.randint(15, 30),
            "Motorcycle": random.randint(40, 80),
            "Bus": random.randint(2, 6),
            "Truck": random.randint(1, 3),
            "Pedestrian": random.randint(5, 15),
        }
    elif is_night:
        counts = {
            "Car": random.randint(0, 3),
            "Motorcycle": random.randint(1, 5),
            "Bus": 0,
            "Truck": random.randint(1, 4),
            "Pedestrian": random.randint(0, 1),
        }
    else:
        counts = {
            "Car": random.randint(5, 15),
            "Motorcycle": random.randint(15, 35),
            "Bus": random.randint(0, 2),
            "Truck": random.randint(0, 2),
            "Pedestrian": random.randint(2, 6),
        }
    return apply_camera_traits(counts, camera, camera_id)


class CameraSimulatorManager:
    """
    Quản lý nguồn ảnh và chế độ mô phỏng:
      - synthetic: vẽ ảnh giả lập theo mật độ giờ trong ngày
      - real: đọc ảnh/video frame từ thư mục REAL_IMAGE_DIR
    """

    def __init__(
        self,
        registry: CameraRegistry | None = None,
        mode: str | None = None,
        camera_limit: int | None = None,
        real_image_dir: str | None = None,
    ):
        self.registry = registry or CameraRegistry.from_json()
        self.mode = (mode or os.getenv("CAMERA_MODE", "synthetic")).strip().lower()

        if self.mode not in {"synthetic", "real", "video"}:
            raise ValueError(
                "CAMERA_MODE must be synthetic, real or video"
            )

        limit_env = os.getenv("CAMERA_COUNT")
        if camera_limit is None and limit_env:
            camera_limit = int(limit_env)
        self.cameras = self.registry.take(camera_limit)

        self.real_image_dir = Path(
            real_image_dir or os.getenv("REAL_IMAGE_DIR", "ingestion/sample_images")
        )
        self._real_files = self._discover_real_images()
        self._real_cursor = 0

        # Video input configuration
        self.video_dir = Path(
            os.getenv("VIDEO_DIR", "ingestion/sample_videos")
        )

        self.video_fps = float(os.getenv("VIDEO_FPS", "1"))
        self.video_max_frames = int(
            os.getenv("VIDEO_MAX_FRAMES", "10")
        )

        if self.mode == "video":
            if self.video_fps <= 0:
                raise ValueError("VIDEO_FPS must be greater than 0")

            if self.video_max_frames <= 0:
                raise ValueError("VIDEO_MAX_FRAMES must be greater than 0")
        logger.info(
            "CameraSimulatorManager mode=%s cameras=%d real_files=%d",
            self.mode,
            len(self.cameras),
            len(self._real_files),
        )

    def _discover_real_images(self) -> list[Path]:
        if not self.real_image_dir.exists():
            return []
        files = [
            p
            for p in sorted(self.real_image_dir.rglob("*"))
            if p.is_file() and p.suffix.lower() in SUPPORTED_IMAGE_EXTS
        ]
        return files

    def list_cameras(self) -> list[Camera]:
        return list(self.cameras)

    def build_frame(self, camera: Camera, hour_offset: int = 0) -> tuple[bytes, str, str, str]:
        """
        Returns (image_bytes, filename, mime_type, capture_timestamp_iso)
        """
        capture_time = datetime.now() - timedelta(hours=hour_offset)
        timestamp_str = capture_time.isoformat()

        if self.mode == "real":
            image_bytes, src_name, mime = self._next_real_image()
            filename = f"{camera.camera_id}_{capture_time.strftime('%Y%m%d_%H%M%S')}_{src_name}"
            return image_bytes, filename, mime, timestamp_str

        counts = get_traffic_density(camera, camera.camera_id, capture_time.hour)
        image_bytes = create_dummy_traffic_image(camera.camera_id, counts)
        filename = f"{camera.camera_id}_{capture_time.strftime('%Y%m%d_%H%M%S')}.jpg"
        return image_bytes, filename, "image/jpeg", timestamp_str

    def _next_real_image(self) -> tuple[bytes, str, str]:
        if not self._real_files:
            raise FileNotFoundError(
                f"Real image mode requires images in {self.real_image_dir}. "
                "Add .jpg/.png files or set CAMERA_MODE=synthetic."
            )
        path = self._real_files[self._real_cursor % len(self._real_files)]
        self._real_cursor += 1
        data = path.read_bytes()
        mime = "image/png" if path.suffix.lower() == ".png" else "image/jpeg"
        return data, path.name, mime

    def run_video_simulation(self, stub):
        """Read and upload each camera's video in an independent worker thread."""
        import cv2
        from concurrent.futures import ThreadPoolExecutor, as_completed

        camera_video_names = {
            "CAM-HN-001": "traffic.mp4",
            "CAM-HN-002": "17022934_4096_2160_60fps.mp4",
            "CAM-HN-003": "17604987-hd_1920_1080_30fps.mp4",
        }
        assignments = []
        for camera in self.cameras:
            video_name = camera_video_names.get(camera.camera_id)
            if video_name is None:
                raise ValueError(
                    f"No video mapping for camera {camera.camera_id}. "
                    "Use CAMERA_COUNT=3 or add an explicit mapping."
                )
            video_path = self.video_dir / video_name
            if not video_path.is_file():
                raise FileNotFoundError(
                    f"Video for {camera.camera_id} not found: {video_path}"
                )
            assignments.append((camera, video_path))
            logger.info("VIDEO_MAPPING %s -> %s", camera.camera_id, video_path.name)

        if not assignments:
            logger.warning("No active cameras for video simulation")
            return 0

        def upload_camera_video(camera, video_path):
            # Each worker owns its VideoCapture, counters and sampling clock.
            cap = cv2.VideoCapture(str(video_path))
            sent = 0
            try:
                if not cap.isOpened():
                    logger.error("[%s] Cannot open video: %s", camera.camera_id, video_path)
                    return 0
                source_fps = cap.get(cv2.CAP_PROP_FPS)
                if source_fps <= 0:
                    logger.error("[%s] Invalid source FPS: %s", camera.camera_id, video_path)
                    return 0
                logger.info(
                    "[%s] Reading video %s (source FPS=%.2f)",
                    camera.camera_id, video_path.name, source_fps,
                )
                frame_index = 0
                next_sample_time = 0.0
                next_send_at = time.monotonic()

                while sent < self.video_max_frames:
                    success, frame = cap.read()
                    if not success:
                        logger.warning("[%s] End of video after %d uploads", camera.camera_id, sent)
                        break
                    video_time = frame_index / source_fps
                    frame_index += 1
                    if video_time + 1e-9 < next_sample_time:
                        continue
                    next_sample_time += 1.0 / self.video_fps

                    encoded, buffer = cv2.imencode(".jpg", frame)
                    if not encoded:
                        logger.warning("[%s] Cannot encode frame %d", camera.camera_id, frame_index)
                        continue

                    # Pace uploads per camera; the three workers do not wait for each other.
                    delay = next_send_at - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    capture_time = datetime.now()
                    filename = (
                        f"{camera.camera_id}_"
                        f"{capture_time.strftime('%Y%m%d_%H%M%S_%f')}_"
                        f"frame{frame_index:06d}.jpg"
                    )
                    response = self._upload(
                        stub, camera.camera_id, buffer.tobytes(), filename,
                        "image/jpeg", capture_time.isoformat(),
                    )
                    if response is None or response.status != "SUCCESS":
                        logger.error("[%s] Video upload failed; stopping this camera", camera.camera_id)
                        break
                    sent += 1
                    next_send_at = max(next_send_at + 1.0 / self.video_fps, time.monotonic())
            except Exception:
                logger.exception("[%s] Unexpected video worker error", camera.camera_id)
            finally:
                cap.release()
                logger.info("[%s] Video frames uploaded: %d", camera.camera_id, sent)
            return sent

        total = 0
        with ThreadPoolExecutor(max_workers=len(assignments), thread_name_prefix="camera-video") as executor:
            futures = {
                executor.submit(upload_camera_video, camera, video_path): camera.camera_id
                for camera, video_path in assignments
            }
            for future in as_completed(futures):
                total += future.result()

        logger.info(
            "Video simulation complete: %d frames from %d cameras",
            total, len(assignments),
        )
        return total

    def run_simulation(self, stub, frames_per_camera: int | None = None, sleep_s: float = 0.3):
        """Gửi frame từ N camera độc lập lên gRPC ingestion."""
        offsets_env = os.getenv("SIM_HOUR_OFFSETS", "0,1,2,3,5,8,12,14,16,18,20,22")
        default_offsets = [int(x.strip()) for x in offsets_env.split(",") if x.strip()]
        if frames_per_camera is None:
            frames_per_camera = int(os.getenv("FRAMES_PER_CAMERA", "2"))
        if self.mode == "video":
            return self.run_video_simulation(stub)
        # Round-robin across cameras for independent streams
        total = 0
        for round_idx in range(frames_per_camera):
            for camera in self.cameras:
                offset = default_offsets[(round_idx + total) % len(default_offsets)]
                image_bytes, filename, mime, ts = self.build_frame(camera, hour_offset=offset)
                self._upload(stub, camera.camera_id, image_bytes, filename, mime, ts)
                total += 1
                time.sleep(sleep_s)
        logger.info("Simulation complete: %d frames from %d cameras", total, len(self.cameras))
        return total

    def _upload(self, stub, camera_id, image_bytes, filename, mime, timestamp_str):
        import image_ingestion_pb2

        request = image_ingestion_pb2.ImageUploadRequest(
            filename=filename,
            image_data=image_bytes,
            source=f"camera_manager:{self.mode}",
            mime_type=mime,
            camera_id=camera_id,
            capture_timestamp=timestamp_str,
        )
        try:
            import grpc

            response = stub.UploadImage(request, timeout=20)
            logger.info(
                "[%s] Uploaded %s status=%s s3=%s",
                camera_id,
                filename,
                response.status,
                response.s3_uri,
            )
            return response
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed upload from %s: %s", camera_id, exc)
            return None