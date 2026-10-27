CREATE DATABASE IF NOT EXISTS traffic_dw;

USE traffic_dw;

-- Dimension: Cameras (Stores metadata about traffic cameras)
CREATE TABLE IF NOT EXISTS dim_cameras (
    camera_id String,
    location String,
    latitude Float32,
    longitude Float32,
    created_at DateTime
) ENGINE = ReplacingMergeTree()
ORDER BY camera_id;

-- Dimension: Vehicle Types (Car, Motorcycle, Bus, Truck, Pedestrian)
CREATE TABLE IF NOT EXISTS dim_vehicles (
    vehicle_type_id UInt8,
    vehicle_type_name String
) ENGINE = ReplacingMergeTree()
ORDER BY vehicle_type_id;

-- Dimension: Images (Tracks original capture metadata)
CREATE TABLE IF NOT EXISTS dim_images (
    image_id String,
    filename String,
    s3_uri String,
    file_size_bytes UInt64,
    mime_type String,
    uploaded_at DateTime
) ENGINE = ReplacingMergeTree()
ORDER BY image_id;

-- Fact Table: Traffic Flow (Measurement table for vehicle counts and congestion)
CREATE TABLE IF NOT EXISTS fact_traffic_flow (
    event_id String,
    timestamp DateTime,
    camera_id String,
    vehicle_type_id UInt8,
    count UInt16,
    congestion_level String, -- Low, Medium, High
    image_id String,
    inference_time_ms UInt32
) ENGINE = MergeTree()
ORDER BY (camera_id, timestamp, vehicle_type_id);

-- Populate static dimension data for vehicles
INSERT INTO dim_vehicles (vehicle_type_id, vehicle_type_name) VALUES
(1, 'Car'),
(2, 'Motorcycle'),
(3, 'Bus'),
(4, 'Truck'),
(5, 'Pedestrian'),
(0, 'Unknown');

-- Populate initial cameras for testing
INSERT INTO dim_cameras (camera_id, location, latitude, longitude, created_at) VALUES
('CAM-HN-001', 'Kim Ma - Nguyen Chi Thanh Intersection', 21.0294, 105.8112, now()),
('CAM-HN-002', 'Cau Giay Street (Near University of Transport and Communications)', 21.0264, 105.8023, now()),
('CAM-HN-003', 'Tay Ho (Nhat Tan Bridge)', 21.0964, 105.8234, now());
