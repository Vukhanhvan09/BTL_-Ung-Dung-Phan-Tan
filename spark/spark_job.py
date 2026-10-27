import os
import sys
import json
import time
import random
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Load configurations
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_NAME = os.getenv("RABBITMQ_QUEUE", "image_processing_queue")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "traffic_dw")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "traffic_password")

# Try to import PySpark
try:
    # pyrefly: ignore [missing-import]
    from pyspark.sql import SparkSession
except ImportError:
    logger.error("PySpark is not installed. Please install pyspark to run this script.")
    sys.exit(1)


def fetch_queue_messages():
    """
    Fetches messages from RabbitMQ queue without acknowledging them yet.
    Returns: (list of message dicts, list of delivery tags)
    """
    import pika
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    
    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        
        messages = []
        delivery_tags = []
        
        logger.info(f"Fetching messages from RabbitMQ queue '{QUEUE_NAME}'...")
        while True:
            method_frame, header_frame, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=False)
            if method_frame:
                try:
                    payload = json.loads(body.decode('utf-8'))
                    payload['_delivery_tag'] = method_frame.delivery_tag
                    messages.append(payload)
                    delivery_tags.append(method_frame.delivery_tag)
                except Exception as e:
                    logger.error(f"Error parsing message body: {e}. Rejecting message.")
                    channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
            else:
                break
                
        connection.close()
        logger.info(f"Fetched {len(messages)} messages from RabbitMQ.")
        return messages, delivery_tags
    except Exception as e:
        logger.error(f"Failed to fetch messages from RabbitMQ: {e}")
        return [], []


def ack_messages(delivery_tags):
    """
    Acknowledges the successfully processed messages in RabbitMQ.
    """
    import pika
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        for tag in delivery_tags:
            channel.basic_ack(delivery_tag=tag)
        connection.close()
        logger.info(f"Acknowledged {len(delivery_tags)} messages in RabbitMQ.")
    except Exception as e:
        logger.error(f"Failed to acknowledge messages in RabbitMQ: {e}")


def process_partition(iterator):
    """
    Processes a partition of events: downloads images from MinIO,
    runs YOLOv8 object detection (or mock detection), and yields metrics.
    """
    import io
    
    # Initialize MinIO client inside worker partition
    try:
        # pyrefly: ignore [missing-import]
        from minio import Minio
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
    except ImportError:
        logger.error("minio library missing on worker node.")
        raise

    has_ml = False
    model = None
    Image = None

    try:
        # pyrefly: ignore [missing-import]
        from PIL import Image
        # pyrefly: ignore [missing-import]
        from ultralytics import YOLO
        # Load pre-trained YOLOv8 nano model
        model = YOLO('yolov8n.pt')
        has_ml = True
        logger.info("YOLOv8 Model loaded successfully on Spark worker partition.")
    except Exception as e:
        logger.warning(f"Could not load YOLOv8 (missing ultralytics/PyTorch): {e}. "
                       "Falling back to high-fidelity mock object detection.")

    vehicle_type_mapping = {
        'Car': 1,
        'Motorcycle': 2,
        'Bus': 3,
        'Truck': 4,
        'Pedestrian': 5
    }

    for row in iterator:
        # row: (event_id, image_id, bucket, s3_key, uploaded_at, filename, camera_id, capture_timestamp)
        event_id, image_id, bucket, s3_key, uploaded_at, filename, camera_id, capture_timestamp = row
        start_time = time.time()
        
        vehicle_counts = {
            'Car': 0,
            'Motorcycle': 0,
            'Bus': 0,
            'Truck': 0,
            'Pedestrian': 0
        }

        try:
            # 1. Fetch image from MinIO
            response = minio_client.get_object(bucket, s3_key)
            img_data = response.read()
            response.close()

            if has_ml and Image is not None:
                # 2. Run real YOLOv8 object detection
                img_io = io.BytesIO(img_data)
                img = Image.open(img_io)
                results = model(img)
                
                # YOLO COCO class indices:
                # 0: person (Pedestrian)
                # 2: car (Car)
                # 3: motorcycle (Motorcycle)
                # 5: bus (Bus)
                # 7: truck (Truck)
                for r in results:
                    for box in r.boxes:
                        cls_id = int(box.cls[0].item())
                        if cls_id == 2:
                            vehicle_counts['Car'] += 1
                        elif cls_id == 3:
                            vehicle_counts['Motorcycle'] += 1
                        elif cls_id == 5:
                            vehicle_counts['Bus'] += 1
                        elif cls_id == 7:
                            vehicle_counts['Truck'] += 1
                        elif cls_id == 0:
                            vehicle_counts['Pedestrian'] += 1
            else:
                # 3. Fallback: High-fidelity Time-of-day Mock Detection
                try:
                    dt = datetime.fromisoformat(capture_timestamp)
                    hour = dt.hour
                except Exception:
                    hour = 12  # default to noon
                
                is_peak = (7 <= hour <= 9) or (17 <= hour <= 19)
                is_night = (hour >= 22) or (hour <= 5)

                if is_peak:
                    vehicle_counts = {
                        'Car': random.randint(15, 30),
                        'Motorcycle': random.randint(40, 80),
                        'Bus': random.randint(2, 6),
                        'Truck': random.randint(1, 3),
                        'Pedestrian': random.randint(5, 15)
                    }
                elif is_night:
                    vehicle_counts = {
                        'Car': random.randint(0, 3),
                        'Motorcycle': random.randint(1, 5),
                        'Bus': 0,
                        'Truck': random.randint(1, 4),
                        'Pedestrian': random.randint(0, 1)
                    }
                else:
                    vehicle_counts = {
                        'Car': random.randint(5, 15),
                        'Motorcycle': random.randint(15, 35),
                        'Bus': random.randint(0, 2),
                        'Truck': random.randint(0, 2),
                        'Pedestrian': random.randint(2, 6)
                    }

                # Camera-specific modifers
                if camera_id == 'CAM-HN-003':  # Nhat Tan Bridge
                    vehicle_counts['Pedestrian'] = 0
                    vehicle_counts['Car'] = int(vehicle_counts['Car'] * 1.5)
                    vehicle_counts['Truck'] = int(vehicle_counts['Truck'] * 1.5)
                elif camera_id == 'CAM-HN-001': # Kim Ma
                    vehicle_counts['Motorcycle'] = int(vehicle_counts['Motorcycle'] * 1.3)
                    vehicle_counts['Truck'] = max(0, vehicle_counts['Truck'] - 1)

        except Exception as ex:
            logger.error(f"Error processing image {image_id}: {ex}")

        inference_time_ms = int((time.time() - start_time) * 1000)
        
        # Determine congestion level
        total_vehicles = sum(vehicle_counts.values())
        if total_vehicles >= 45:
            congestion_level = "High"
        elif total_vehicles >= 15:
            congestion_level = "Medium"
        else:
            congestion_level = "Low"

        # Yield a database record for each vehicle type
        for v_type, count in vehicle_counts.items():
            v_type_id = vehicle_type_mapping.get(v_type, 0)
            yield (
                event_id,
                capture_timestamp,
                camera_id,
                v_type_id,
                count,
                congestion_level,
                image_id,
                inference_time_ms
            )


def save_to_clickhouse(events, traffic_results):
    """
    Saves processed results and image metadata to ClickHouse traffic_dw.
    """
    try:
        # pyrefly: ignore [missing-import]
        import clickhouse_connect
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB
        )
    except ImportError:
        logger.error("clickhouse-connect library missing on driver node.")
        raise

    # 1. Prepare dim_images records (using set to de-duplicate)
    dim_images_data = []
    seen_images = set()
    for evt in events:
        if evt["image_id"] not in seen_images:
            dim_images_data.append((
                evt["image_id"],
                evt["filename"],
                evt["s3_uri"],
                int(evt["file_size_bytes"]),
                evt["mime_type"],
                datetime.fromisoformat(evt["uploaded_at"])
            ))
            seen_images.add(evt["image_id"])

    # 2. Prepare fact_traffic_flow records
    fact_traffic_data = []
    for res in traffic_results:
        # res: (event_id, capture_timestamp, camera_id, v_type_id, count, congestion_level, image_id, inference_time_ms)
        try:
            timestamp_dt = datetime.fromisoformat(res[1])
        except ValueError:
            timestamp_dt = datetime.now()

        fact_traffic_data.append((
            res[0],
            timestamp_dt,
            res[2],
            int(res[3]),
            int(res[4]),
            res[5],
            res[6],
            int(res[7])
        ))

    # Insert into ClickHouse
    if dim_images_data:
        client.insert(
            "dim_images",
            dim_images_data,
            column_names=["image_id", "filename", "s3_uri", "file_size_bytes", "mime_type", "uploaded_at"]
        )
        logger.info(f"Successfully inserted {len(dim_images_data)} rows into 'dim_images'.")

    if fact_traffic_data:
        client.insert(
            "fact_traffic_flow",
            fact_traffic_data,
            column_names=["event_id", "timestamp", "camera_id", "vehicle_type_id", "count", "congestion_level", "image_id", "inference_time_ms"]
        )
        logger.info(f"Successfully inserted {len(fact_traffic_data)} rows into 'fact_traffic_flow'.")

    client.close()


def main():
    # 1. Fetch events from RabbitMQ
    events, delivery_tags = fetch_queue_messages()
    if not events:
        logger.info("No events found in queue. Exiting Spark job.")
        return

    # 2. Start Spark Session
    logger.info("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("Distributed-Traffic-YOLO-Detection") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # 3. Create parallel input list
    # Structure: (event_id, image_id, bucket, s3_key, uploaded_at, filename, camera_id, capture_timestamp)
    spark_input_data = [
        (
            e["event_id"],
            e["image_id"],
            e["bucket"],
            e["s3_key"],
            e["uploaded_at"],
            e["filename"],
            e.get("camera_id", "CAM-UNKNOWN"),
            e.get("capture_timestamp", e["uploaded_at"])
        )
        for e in events
    ]

    rdd = spark.sparkContext.parallelize(spark_input_data)
    
    # 4. Process partitions on workers (run YOLO / mock detection)
    logger.info("Running traffic vehicle detection distributed Spark job...")
    results_rdd = rdd.mapPartitions(process_partition)
    
    # 5. Collect results back to driver
    traffic_results = results_rdd.collect()
    logger.info(f"Spark job completed. Collected {len(traffic_results)} traffic flow records.")
    
    # 6. Save results to ClickHouse
    try:
        save_to_clickhouse(events, traffic_results)
        # 7. Acknowledge messages in RabbitMQ only after successful database write
        ack_messages(delivery_tags)
        logger.info("Pipeline run succeeded. All messages acknowledged.")
    except Exception as e:
        logger.error(f"Failed to save results to ClickHouse: {e}. Messages will not be acknowledged.")
    
    spark.stop()


if __name__ == "__main__":
    main()
