import os
import sys
import time
import random
import logging
import io
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- Dynamic protobuf compile ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROTO_DIR = os.path.join(CURRENT_DIR, "protos")
GEN_DIR = os.path.join(CURRENT_DIR, "generated")
PROTO_FILE = os.path.join(PROTO_DIR, "image_ingestion.proto")

sys.path.insert(0, GEN_DIR)

try:
    import grpc
    # pyrefly: ignore [missing-import]
    import image_ingestion_pb2
    # pyrefly: ignore [missing-import]
    import image_ingestion_pb2_grpc
except ImportError:
    # Try compiling protobuf definitions first
    # pyrefly: ignore [missing-import]
    from grpc_tools import protoc
    logger.info("Compiling Protobuf definitions in client...")
    if not os.path.exists(GEN_DIR):
        os.makedirs(GEN_DIR)
    protoc.main((
        '',
        f'-I{PROTO_DIR}',
        f'--python_out={GEN_DIR}',
        f'--grpc_python_out={GEN_DIR}',
        PROTO_FILE,
    ))
    import grpc
    # pyrefly: ignore [missing-import]
    import image_ingestion_pb2
    # pyrefly: ignore [missing-import]
    import image_ingestion_pb2_grpc

# Pillow is optional, fallback to random bytes if not installed
try:
    # pyrefly: ignore [missing-import]
    from PIL import Image, ImageDraw, ImageFont
    HAS_PILLOW = True
except ImportError:
    HAS_PILLOW = False
    logger.warning("Pillow is not installed. Dummy traffic images will contain random bytes.")


def create_dummy_traffic_image(camera_id, vehicle_counts):
    """
    Creates a simulated traffic image.
    Draws a gray road with lanes and colored rectangles representing detected vehicles.
    """
    if not HAS_PILLOW:
        return bytes([random.randint(0, 255) for _ in range(5000)])

    # 1. Create dark asphalt road background
    img = Image.new('RGB', (640, 480), color=(50, 50, 50))
    d = ImageDraw.Draw(img)

    # 2. Draw lane markings
    # Draw double yellow line in the center
    d.line([(318, 0), (318, 480)], fill=(255, 215, 0), width=3)
    d.line([(322, 0), (322, 480)], fill=(255, 215, 0), width=3)
    # Draw white dashed lane lines
    for y in range(0, 480, 40):
        d.line([(160, y), (160, y + 20)], fill=(255, 255, 255), width=2)
        d.line([(480, y), (480, y + 20)], fill=(255, 255, 255), width=2)

    # 3. Draw simulated vehicles
    colors = {
        'Car': (0, 102, 204),        # Blue
        'Motorcycle': (0, 204, 102), # Green
        'Bus': (204, 0, 0),          # Red
        'Truck': (204, 153, 0),      # Orange/Brown
        'Pedestrian': (255, 255, 0)  # Yellow
    }

    # Generate visual boxes based on the count of vehicles
    y_offset = 30
    for v_type, count in vehicle_counts.items():
        if count == 0:
            continue
        color = colors.get(v_type, (128, 128, 128))
        for _ in range(min(count, 5)):  # Limit visual boxes to avoid overcrowded drawing
            # Random position within lanes
            x = random.randint(30, 580)
            y = random.randint(y_offset, min(y_offset + 80, 430))
            
            # Vehicle shapes depending on type
            if v_type == 'Car':
                d.rectangle([x, y, x + 45, y + 25], fill=color, outline=(255, 255, 255))
                d.rectangle([x + 10, y + 5, x + 35, y + 20], fill=(200, 220, 255)) # windshields
            elif v_type == 'Bus':
                d.rectangle([x, y, x + 70, y + 30], fill=color, outline=(255, 255, 255))
            elif v_type == 'Motorcycle':
                d.ellipse([x, y, x + 15, y + 15], fill=color)
                d.line([(x, y + 7), (x + 20, y + 7)], fill=(200, 200, 200), width=2)
            elif v_type == 'Truck':
                d.rectangle([x, y, x + 60, y + 28], fill=color, outline=(255, 255, 255))
                d.rectangle([x + 45, y + 2, x + 58, y + 26], fill=(100, 100, 100)) # cab
            elif v_type == 'Pedestrian':
                d.ellipse([x, y, x + 10, y + 10], fill=color) # head
                d.line([(x + 5, y + 10), (x + 5, y + 25)], fill=color, width=2) # body

        y_offset = (y_offset + 90) % 400

    # 4. Draw Camera Info Overlay
    d.rectangle([10, 10, 300, 45], fill=(0, 0, 0, 150))
    d.text((20, 15), f"Camera: {camera_id} | Traffic Sim", fill=(255, 255, 255))

    # Save to memory buffer
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='JPEG')
    return img_byte_arr.getvalue()


def get_traffic_density(camera_id, current_hour):
    """
    Returns realistic traffic counts based on the camera location and hour of day.
    """
    # Peak hours: 7 AM - 9 AM and 5 PM - 7 PM (17 - 19)
    is_peak = (7 <= current_hour <= 9) or (17 <= current_hour <= 19)
    # Night hours: 10 PM - 5 AM
    is_night = (current_hour >= 22) or (current_hour <= 5)

    if is_peak:
        # High traffic
        counts = {
            'Car': random.randint(15, 30),
            'Motorcycle': random.randint(40, 80),
            'Bus': random.randint(2, 6),
            'Truck': random.randint(1, 3),
            'Pedestrian': random.randint(5, 15)
        }
    elif is_night:
        # Low traffic
        counts = {
            'Car': random.randint(0, 3),
            'Motorcycle': random.randint(1, 5),
            'Bus': 0,
            'Truck': random.randint(1, 4), # Trucks are common at night
            'Pedestrian': random.randint(0, 1)
        }
    else:
        # Medium traffic
        counts = {
            'Car': random.randint(5, 15),
            'Motorcycle': random.randint(15, 35),
            'Bus': random.randint(0, 2),
            'Truck': random.randint(0, 2),
            'Pedestrian': random.randint(2, 6)
        }

    # Camera specific modifiers
    if camera_id == 'CAM-HN-003':  # Nhat Tan Bridge: no pedestrians, more trucks/cars
        counts['Pedestrian'] = 0
        counts['Car'] = int(counts['Car'] * 1.5)
        counts['Truck'] = int(counts['Truck'] * 1.5)
    elif camera_id == 'CAM-HN-001': # Kim Ma (narrow/city center): tons of motorcycles
        counts['Motorcycle'] = int(counts['Motorcycle'] * 1.3)
        counts['Truck'] = max(0, counts['Truck'] - 1)

    return counts


def send_traffic_image(stub, camera_id, hour_offset=0):
    # Simulate historical or current time
    capture_time = datetime.now() - timedelta(hours=hour_offset)
    timestamp_str = capture_time.isoformat()
    
    # Generate realistic vehicle counts based on hour
    vehicle_counts = get_traffic_density(camera_id, capture_time.hour)
    
    filename = f"{camera_id}_{capture_time.strftime('%Y%m%d_%H%M%S')}.jpg"
    image_bytes = create_dummy_traffic_image(camera_id, vehicle_counts)
    
    request = image_ingestion_pb2.ImageUploadRequest(
        filename=filename,
        image_data=image_bytes,
        source="traffic_simulator",
        mime_type="image/jpeg",
        camera_id=camera_id,
        capture_timestamp=timestamp_str
    )

    try:
        response = stub.UploadImage(request, timeout=10)
        logger.info(f"[{camera_id}] Uploaded {filename} at {capture_time.strftime('%H:%M:%S')}. "
                    f"ID: {response.image_id}. S3: {response.s3_uri}")
    except grpc.RpcError as e:
        logger.error(f"Failed to upload image from {camera_id}: {e.code()} - {e.details()}")


def run():
    target = os.getenv("GRPC_SERVER_ADDRESS", "localhost:50051")
    logger.info(f"Connecting to gRPC server at {target}...")
    
    with grpc.insecure_channel(target) as channel:
        stub = image_ingestion_pb2_grpc.ImageIngestionServiceStub(channel)
        
        cameras = ['CAM-HN-001', 'CAM-HN-002', 'CAM-HN-003']
        
        logger.info("Starting simulation: Sending traffic camera frames...")
        
        # We will send 12 mock frames in total (simulating different times of day to show varying density)
        # We simulate a mix of peak hours, off-peak hours, and night hours
        time_offsets = [0, 1, 2, 3, 5, 8, 12, 14, 16, 18, 20, 22]  # simulated hours ago
        
        for i, offset in enumerate(time_offsets):
            camera_id = random.choice(cameras)
            logger.info(f"--- Sending frame #{i+1}/{len(time_offsets)} from {camera_id} (Simulated offset: -{offset}h) ---")
            send_traffic_image(stub, camera_id, hour_offset=offset)
            time.sleep(1.0) # sleep 1 second between requests
            
        logger.info("Simulation complete.")


if __name__ == "__main__":
    run()
