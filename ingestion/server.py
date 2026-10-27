import os
import sys
import uuid
import json
import logging
from concurrent import futures
from datetime import datetime
import io

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- Dynamic protobuf compile ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROTO_DIR = os.path.join(CURRENT_DIR, "protos")
GEN_DIR = os.path.join(CURRENT_DIR, "generated")
PROTO_FILE = os.path.join(PROTO_DIR, "image_ingestion.proto")

if not os.path.exists(GEN_DIR):
    os.makedirs(GEN_DIR)

try:
    # pyrefly: ignore [missing-import]
    from grpc_tools import protoc
    logger.info("Compiling Protobuf definitions...")
    protoc.main((
        '',
        f'-I{PROTO_DIR}',
        f'--python_out={GEN_DIR}',
        f'--grpc_python_out={GEN_DIR}',
        PROTO_FILE,
    ))
    # Correct import paths inside generated files if needed
    # (sometimes necessary in python packages, but here simple path insert is enough)
except ImportError:
    logger.warning("grpc_tools is not installed. Will attempt to import from generated directory directly.")

sys.path.insert(0, GEN_DIR)

try:
    import grpc
    # pyrefly: ignore [missing-import]
    import image_ingestion_pb2
    # pyrefly: ignore [missing-import]
    import image_ingestion_pb2_grpc
except ImportError as e:
    logger.error("Failed to import required libraries. Make sure grpcio, grpcio-tools are installed.")
    sys.exit(1)

# pyrefly: ignore [missing-import]
from minio import Minio
# pyrefly: ignore [missing-import]
from minio.error import S3Error
import pika

# Configuration from Environment Variables (or defaults)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
BUCKET_NAME = os.getenv("MINIO_BUCKET", "raw-images")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_NAME = os.getenv("RABBITMQ_QUEUE", "image_processing_queue")


class ImageIngestionService(image_ingestion_pb2_grpc.ImageIngestionServiceServicer):
    def __init__(self):
        # Initialize MinIO client
        logger.info(f"Connecting to MinIO at {MINIO_ENDPOINT}...")
        self.minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        # Ensure bucket exists
        try:
            if not self.minio_client.bucket_exists(BUCKET_NAME):
                self.minio_client.make_bucket(BUCKET_NAME)
                logger.info(f"Created MinIO bucket '{BUCKET_NAME}'")
            else:
                logger.info(f"MinIO bucket '{BUCKET_NAME}' already exists")
        except S3Error as e:
            logger.error(f"MinIO bucket validation failed: {e}")
            raise e

        # Test RabbitMQ Connection
        self.rabbitmq_params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
            heartbeat=600,
            blocked_connection_timeout=300
        )
        self._ensure_rabbitmq_queue()

    def _ensure_rabbitmq_queue(self):
        try:
            connection = pika.BlockingConnection(self.rabbitmq_params)
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            connection.close()
            logger.info(f"Declared RabbitMQ queue: {QUEUE_NAME}")
        except Exception as e:
            logger.error(f"Failed to connect/declare RabbitMQ queue: {e}")
            raise e

    def UploadImage(self, request, context):
        image_id = str(uuid.uuid4())
        event_id = str(uuid.uuid4())
        filename = request.filename or f"image_{image_id}.jpg"
        mime_type = request.mime_type or "image/jpeg"
        uploaded_at = datetime.utcnow().isoformat()
        
        # Get camera metadata
        camera_id = request.camera_id or "CAM-UNKNOWN"
        capture_timestamp = request.capture_timestamp or uploaded_at
        
        logger.info(f"Received upload request from {camera_id} for image: {filename} (Size: {len(request.image_data)} bytes)")

        # 1. Upload to MinIO
        s3_key = f"uploads/{datetime.utcnow().strftime('%Y/%m/%d')}/{image_id}_{filename}"
        try:
            data_stream = io.BytesIO(request.image_data)
            self.minio_client.put_object(
                BUCKET_NAME,
                s3_key,
                data_stream,
                length=len(request.image_data),
                content_type=mime_type
            )
            s3_uri = f"s3://{BUCKET_NAME}/{s3_key}"
            logger.info(f"Uploaded {filename} successfully to MinIO: {s3_uri}")
        except Exception as e:
            logger.error(f"Failed to upload image {filename} to MinIO: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"MinIO Upload Error: {str(e)}")
            return image_ingestion_pb2.ImageUploadResponse(
                status="FAILED",
                message=f"Failed to upload image to S3: {str(e)}"
            )

        # 2. Push event metadata to RabbitMQ
        event_payload = {
            "event_id": event_id,
            "image_id": image_id,
            "filename": filename,
            "s3_uri": s3_uri,
            "bucket": BUCKET_NAME,
            "s3_key": s3_key,
            "file_size_bytes": len(request.image_data),
            "mime_type": mime_type,
            "source": request.source or "grpc_client",
            "uploaded_at": uploaded_at,
            "camera_id": camera_id,
            "capture_timestamp": capture_timestamp
        }

        try:
            connection = pika.BlockingConnection(self.rabbitmq_params)
            channel = connection.channel()
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=json.dumps(event_payload),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    content_type='application/json'
                )
            )
            connection.close()
            logger.info(f"Published ingestion event to RabbitMQ for image: {image_id}")
        except Exception as e:
            logger.error(f"Failed to publish event to RabbitMQ for image {image_id}: {e}")
            # If RabbitMQ fails, we have an inconsistency, but for now log it and raise error
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"RabbitMQ Publish Error: {str(e)}")
            return image_ingestion_pb2.ImageUploadResponse(
                image_id=image_id,
                s3_uri=s3_uri,
                status="PARTIAL_SUCCESS",
                message=f"Uploaded to MinIO, but failed to queue processing: {str(e)}"
            )

        # 3. Return response
        return image_ingestion_pb2.ImageUploadResponse(
            image_id=image_id,
            s3_uri=s3_uri,
            status="SUCCESS",
            message="Image successfully uploaded and queued for processing."
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    image_ingestion_pb2_grpc.add_ImageIngestionServiceServicer_to_server(
        ImageIngestionService(), server
    )
    port = "[::]:50051"
    server.add_insecure_port(port)
    logger.info(f"gRPC server starting on port 50051...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
