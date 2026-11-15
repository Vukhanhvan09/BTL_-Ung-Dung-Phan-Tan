
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import cv2
import grpc

# Đường dẫn thư mục dự án
INGESTION_DIR = Path(__file__).resolve().parent
GENERATED_DIR = INGESTION_DIR / "generated"
sys.path.insert(0, str(GENERATED_DIR))

import image_ingestion_pb2
import image_ingestion_pb2_grpc

GRPC_SERVER = os.getenv(
    "GRPC_SERVER_ADDRESS",
    "localhost:50051"
)

SAMPLE_FPS = float(os.getenv("SAMPLE_FPS", "1"))

# Mỗi video tương ứng với một camera
CAMERAS = {
    "CAM-HN-001": INGESTION_DIR / "sample_video" / "traffic1.mp4",
    "CAM-HN-002": INGESTION_DIR / "sample_video" / "traffic2.mp4",
    "CAM-HN-003": INGESTION_DIR / "sample_video" / "traffic3.mp4",
}


def stream_camera(camera_id, video_path):
    cap = cv2.VideoCapture(str(video_path))

    if not cap.isOpened():
        print(f"[{camera_id}] Không mở được: {video_path}")
        return

    video_fps = cap.get(cv2.CAP_PROP_FPS)

    if video_fps <= 0:
        print(f"[{camera_id}] Không đọc được FPS")
        cap.release()
        return

    frame_index = 0
    next_sample = 0.0
    sent_count = 0

    try:
        with grpc.insecure_channel(GRPC_SERVER) as channel:
            grpc.channel_ready_future(channel).result(timeout=10)

            stub = (
                image_ingestion_pb2_grpc
                .ImageIngestionServiceStub(channel)
            )

            while True:
                ret, frame = cap.read()

                if not ret:
                    print(f"[{camera_id}] Đã đọc hết video")
                    break

                frame_index += 1
                video_time = (frame_index - 1) / video_fps

                if video_time < next_sample:
                    continue

                next_sample += 1.0 / SAMPLE_FPS

                success, encoded = cv2.imencode(
                    ".jpg",
                    frame,
                    [cv2.IMWRITE_JPEG_QUALITY, 80]
                )

                if not success:
                    continue

                now = datetime.now(timezone.utc)

                # Tên file tương thích với Dashboard
                filename = (
                    f"{camera_id}_"
                    f"{now.strftime('%Y%m%d_%H%M%S_%f')}_"
                    f"frame{frame_index:06d}.jpg"
                )

                request = image_ingestion_pb2.ImageUploadRequest(
                    filename=filename,
                    image_data=encoded.tobytes(),
                    source="video",
                    mime_type="image/jpeg",
                    camera_id=camera_id,
                    capture_timestamp=now.isoformat()
                )

                response = stub.UploadImage(
                    request,
                    timeout=30
                )

                if str(response.status).upper() != "SUCCESS":
                    print(
                        f"[{camera_id}] Lỗi: {response.status}"
                    )
                    break

                sent_count += 1

                print(
                    f"[{camera_id}] Đã gửi frame "
                    f"{sent_count}: SUCCESS"
                )

                # Giới hạn tốc độ gửi theo thời gian thực
                time.sleep(1.0 / SAMPLE_FPS)

    except grpc.RpcError as error:
        print(f"[{camera_id}] Lỗi gRPC: {error}")
    except Exception as error:
        print(f"[{camera_id}] Lỗi: {error}")
    finally:
        cap.release()
        print(
            f"[{camera_id}] Tổng frame gửi: {sent_count}"
        )


def main():
    if SAMPLE_FPS <= 0:
        raise ValueError("SAMPLE_FPS phải lớn hơn 0")

    print("Bắt đầu chạy 3 camera video đồng thời")

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(stream_camera, camera_id, video_path)
            for camera_id, video_path in CAMERAS.items()
        ]

        for future in futures:
            future.result()

    print("Đã hoàn tất cả 3 video")


if __name__ == "__main__":
    main()

