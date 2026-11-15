

import os
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

import cv2
import grpc

# Thêm thư mục generated vào đường dẫn tìm module
GENERATED_DIR = Path(__file__).resolve().parent / "generated"
sys.path.insert(0, str(GENERATED_DIR))

import image_ingestion_pb2
import image_ingestion_pb2_grpc



def main():
    video_path = os.getenv(
        "VIDEO_PATH",
        "ingestion/sample_video/traffic.mp4"
    )

    camera_id = os.getenv(
        "CAMERA_ID",
        "CAM-HN-001"
    )

    sample_fps = float(
        os.getenv("SAMPLE_FPS", "1")
    )

    if sample_fps <= 0:
        raise ValueError("SAMPLE_FPS phải lớn hơn 0")

    cap = cv2.VideoCapture(video_path)

    if not cap.isOpened():
        print("Không mở được video:", video_path)
        return

    video_fps = cap.get(cv2.CAP_PROP_FPS)

    if video_fps <= 0:
        print("Không xác định được FPS")
        cap.release()
        return

    frame_index = 0
    next_sample = 0.0
    sent_count = 0

    try:
        with grpc.insecure_channel(
            "localhost:50051"
        ) as channel:

            stub = (
                image_ingestion_pb2_grpc
                .ImageIngestionServiceStub(channel)
            )

            while True:
                ret, frame = cap.read()

                if not ret:
                    print("Đã đọc hết video")
                    break

                frame_index += 1

                video_time = (
                    frame_index - 1
                ) / video_fps

                if video_time < next_sample:
                    continue

                next_sample += 1.0 / sample_fps

                success, encoded = cv2.imencode(
                    ".jpg",
                    frame
                )

                if not success:
                    continue

                timestamp = datetime.now(
                    timezone.utc
                ).isoformat()

                
# Tạo tên frame theo định dạng Dashboard yêu cầu
                now = datetime.now(timezone.utc)

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
                    capture_timestamp=timestamp
                )


                response = stub.UploadImage(
                    request,
                    timeout=20
                )

                if str(response.status).upper() != "SUCCESS":
                    print("Gửi thất bại:", response.status)
                    break

                sent_count += 1
                print(
                    f"Đã gửi frame {sent_count}: "
                    f"{response.status}"
                )

    except grpc.RpcError as error:
        print("Lỗi gRPC:", error)

    finally:
        cap.release()
        print("Tổng frame đã gửi:", sent_count)


if __name__ == "__main__":
    main()
