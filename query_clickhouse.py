# pyrefly: ignore [missing-import]
import clickhouse_connect

def print_table(rows, headers):
    if not rows:
        print("No records found.")
        return
    try:
        from tabulate import tabulate
        print(tabulate(rows, headers=headers, tablefmt='grid'))
    except ImportError:
        # Fallback simple formatter if tabulate is not installed
        widths = [max(len(str(val)) for val in col) for col in zip(*rows, headers)]
        format_str = " | ".join(f"{{:<{w}}}" for w in widths)
        border = "-+-".join("-" * w for w in widths)
        print(border)
        print(format_str.format(*headers))
        print(border)
        for row in rows:
            print(format_str.format(*(str(x) for x in row)))
        print(border)

def main():
    print("Connecting to ClickHouse database 'traffic_dw' at localhost:8123...")
    try:
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='default',
            password='traffic_password',
            database='traffic_dw'
        )
        
        # 1. Query Cameras
        print("\n--- DIM_CAMERAS ---")
        cam_res = client.query('SELECT camera_id, location, latitude, longitude FROM dim_cameras')
        print_table(cam_res.result_rows, cam_res.column_names)

        # 2. Query Recent Frames
        print("\n--- DIM_IMAGES (Latest 5 uploaded frames) ---")
        img_res = client.query('SELECT image_id, filename, file_size_bytes, uploaded_at FROM dim_images ORDER BY uploaded_at DESC LIMIT 5')
        print_table(img_res.result_rows, img_res.column_names)

        # 3. Query Total Vehicle Count by Type per Camera
        print("\n--- VEHICLE COUNT SUMMARY BY CAMERA ---")
        query_summary = """
            SELECT 
                f.camera_id,
                c.location,
                v.vehicle_type_name,
                SUM(f.count) as total_count,
                ROUND(AVG(f.count), 2) as avg_count_per_frame
            FROM fact_traffic_flow f
            JOIN dim_cameras c ON f.camera_id = c.camera_id
            JOIN dim_vehicles v ON f.vehicle_type_id = v.vehicle_type_id
            GROUP BY f.camera_id, c.location, v.vehicle_type_name
            ORDER BY f.camera_id, total_count DESC
        """
        summary_res = client.query(query_summary)
        print_table(summary_res.result_rows, summary_res.column_names)

        # 4. Query Congestion Level Frequency
        print("\n--- CONGESTION LEVEL DISTRIBUTION ---")
        query_congestion = """
            SELECT 
                camera_id,
                congestion_level,
                COUNT(DISTINCT event_id) as total_frames,
                toString(ROUND(100.0 * COUNT(DISTINCT event_id) / SUM(COUNT(DISTINCT event_id)) OVER (PARTITION BY camera_id), 2)) || '%' as percentage
            FROM fact_traffic_flow
            GROUP BY camera_id, congestion_level
            ORDER BY camera_id, total_frames DESC
        """
        congestion_res = client.query(query_congestion)
        print_table(congestion_res.result_rows, congestion_res.column_names)

        # 5. Query Raw Traffic Flow Records
        print("\n--- FACT_TRAFFIC_FLOW (Latest 10 metrics) ---")
        flow_res = client.query("""
            SELECT 
                f.timestamp,
                f.camera_id,
                v.vehicle_type_name,
                f.count,
                f.congestion_level,
                f.inference_time_ms
            FROM fact_traffic_flow f
            JOIN dim_vehicles v ON f.vehicle_type_id = v.vehicle_type_id
            ORDER BY f.timestamp DESC, f.camera_id
            LIMIT 10
        """)
        print_table(flow_res.result_rows, flow_res.column_names)

    except Exception as e:
        print(f"\n[ERROR] Connection failed: {e}")
        print("Please make sure ClickHouse is running in Docker (docker-compose up -d).")

if __name__ == '__main__':
    main()
