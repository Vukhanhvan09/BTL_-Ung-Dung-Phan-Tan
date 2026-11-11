-- Migrate existing fact_traffic_flow to ReplacingMergeTree for retry-safe dedup.
-- Safe to re-run: skips if already on ReplacingMergeTree.

USE traffic_dw;

CREATE TABLE IF NOT EXISTS fact_traffic_flow_new (
    event_id String,
    timestamp DateTime,
    camera_id String,
    vehicle_type_id UInt8,
    count UInt16,
    congestion_level String,
    image_id String,
    inference_time_ms UInt32,
    detection_mode LowCardinality(String) DEFAULT 'unknown',
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (event_id, vehicle_type_id, detection_mode);

INSERT INTO fact_traffic_flow_new
    (event_id, timestamp, camera_id, vehicle_type_id, count, congestion_level,
     image_id, inference_time_ms, detection_mode, inserted_at)
SELECT
    event_id,
    timestamp,
    camera_id,
    vehicle_type_id,
    count,
    congestion_level,
    image_id,
    inference_time_ms,
    detection_mode,
    now()
FROM fact_traffic_flow;

RENAME TABLE
    fact_traffic_flow TO fact_traffic_flow_old_backup,
    fact_traffic_flow_new TO fact_traffic_flow;

DROP TABLE IF EXISTS fact_traffic_flow_old_backup;
