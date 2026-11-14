import os
import io
import pandas as pd
# pyrefly: ignore [missing-import]
import streamlit as st
# pyrefly: ignore [missing-import]
import plotly.express as px
# pyrefly: ignore [missing-import]
import plotly.graph_objects as go
from datetime import datetime
# pyrefly: ignore [missing-import]
import clickhouse_connect
# pyrefly: ignore [missing-import]
from minio import Minio
# pyrefly: ignore [missing-import]
from PIL import Image

# Streamlit Page Configuration
st.set_page_config(
    page_title="Smart Traffic Control Center",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Connect configuration
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "traffic_dw")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "traffic_password")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
VEHICLE_TYPE_NAMES = ["Car", "Motorcycle", "Bus", "Truck"]

# Custom premium styling overrides (Neon Theme)
st.markdown("""
    <style>
        .stApp {
            background-color: #0b0f17;
            color: #f1f5f9;
        }
        .main-header {
            font-size: 2.8rem;
            font-weight: 800;
            background: linear-gradient(90deg, #06b6d4 0%, #3b82f6 50%, #6366f1 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 5px;
            display: flex;
            align-items: center;
            gap: 15px;
        }
        .subheader {
            font-size: 1.1rem;
            color: #94a3b8;
            margin-bottom: 25px;
        }
        .metric-container {
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            padding: 20px;
            border-radius: 12px;
            border: 1px solid #334155;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
            text-align: center;
            transition: transform 0.3s ease;
        }
        .metric-container:hover {
            transform: translateY(-5px);
            border-color: #06b6d4;
        }
        .metric-val {
            font-size: 2.5rem;
            font-weight: 800;
            color: #06b6d4;
            margin-top: 5px;
        }
        .metric-lbl {
            font-size: 0.85rem;
            color: #94a3b8;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .camera-badge {
            background-color: #1e293b;
            padding: 8px 15px;
            border-radius: 20px;
            border: 1px solid #334155;
            font-weight: 600;
            color: #38bdf8;
            display: inline-block;
            margin-bottom: 10px;
        }
        /* Style adjustments for native widgets */
        .stSelectbox div[data-baseweb="select"] {
            background-color: #1e293b !important;
            color: #f1f5f9 !important;
            border: 1px solid #334155 !important;
        }
    </style>
""", unsafe_allow_html=True)

# Helper function to get ClickHouse Client
def get_ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

# Helper function to load image from MinIO
def load_minio_image(s3_uri):
    if not s3_uri or not s3_uri.startswith("s3://"):
        return None
    try:
        parts = s3_uri[5:].split("/", 1)
        bucket = parts[0]
        object_name = parts[1]
        
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        response = client.get_object(bucket, object_name)
        return Image.open(io.BytesIO(response.read()))
    except Exception as e:
        st.sidebar.error(f"Failed to fetch image from MinIO: {e}")
        return None


# Cached frame retrieval for the video replay tab.
@st.cache_data(ttl=120, max_entries=240, show_spinner=False)
def load_video_frame_bytes(s3_uri):
    if not isinstance(s3_uri, str) or not s3_uri.startswith("s3://"):
        return None
    try:
        bucket, object_name = s3_uri[5:].split("/", 1)
        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                       secret_key=MINIO_SECRET_KEY, secure=False)
        response = client.get_object(bucket, object_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()
    except Exception:
        return None


def annotated_s3_uri(raw_s3_uri):
    """Match spark_job_step5.py object naming without changing ClickHouse schema."""
    if not isinstance(raw_s3_uri, str) or not raw_s3_uri.startswith("s3://"):
        return None
    try:
        bucket, key = raw_s3_uri[5:].split("/", 1)
        stem = key.rsplit(".", 1)[0]
        return f"s3://{bucket}/annotated/{stem}_yolo.jpg"
    except ValueError:
        return None


# Load Data from ClickHouse
@st.cache_data(ttl=10) # Refresh data cache every 10 seconds
def fetch_dashboard_data(detection_mode):
    try:
        client = get_ch_client()
        
        # 2. Cameras
        cameras_df = client.query_df("SELECT camera_id, location, latitude, longitude FROM dim_cameras FINAL")
        cameras_df["latitude"] = cameras_df["latitude"].astype(float)
        cameras_df["longitude"] = cameras_df["longitude"].astype(float)
        
        # 3. Traffic Summary — FINAL collapses retry duplicates
        traffic_summary = client.query_df("""
            SELECT
                f.camera_id AS camera_id,
                c.location AS location,
                v.vehicle_type_name AS vehicle_type_name,
                sum(f.count) AS total_count,
                avg(f.count) AS avg_count
            FROM fact_traffic_flow AS f FINAL
            JOIN (SELECT * FROM dim_cameras FINAL) AS c ON f.camera_id = c.camera_id
            JOIN (SELECT * FROM dim_vehicles FINAL) AS v ON f.vehicle_type_id = v.vehicle_type_id
            WHERE f.detection_mode = {mode:String}
            GROUP BY f.camera_id, c.location, v.vehicle_type_name
        """, parameters={"mode": detection_mode})

        # 4. Congestion — one row per event (not 5 vehicle rows)
        congestion_levels = client.query_df("""
            SELECT
                camera_id,
                congestion_level,
                count() AS frame_count
            FROM (
                SELECT
                    event_id,
                    any(camera_id) AS camera_id,
                    any(congestion_level) AS congestion_level
                FROM fact_traffic_flow FINAL
                WHERE detection_mode = {mode:String}
                GROUP BY event_id
            )
            GROUP BY camera_id, congestion_level
        """, parameters={"mode": detection_mode})

        # 5. Time series — vehicle counts exclude pedestrians; inference per event
        time_series = client.query_df("""
            SELECT
                timestamp,
                camera_id,
                location,
                sum(vehicle_count) AS total_vehicles,
                avg(inference_time_ms) AS avg_inference
            FROM (
                SELECT
                    f.event_id AS event_id,
                    any(f.timestamp) AS timestamp,
                    any(f.camera_id) AS camera_id,
                    any(c.location) AS location,
                    sumIf(f.count, f.vehicle_type_id IN (1, 2, 3, 4)) AS vehicle_count,
                    any(f.inference_time_ms) AS inference_time_ms
                FROM fact_traffic_flow AS f FINAL
                JOIN (SELECT * FROM dim_cameras FINAL) AS c
                    ON f.camera_id = c.camera_id
                WHERE f.detection_mode = {mode:String}
                GROUP BY f.event_id
            )
            GROUP BY timestamp, camera_id, location
            ORDER BY timestamp ASC
        """, parameters={"mode": detection_mode})

        # 6. KPI aggregates (accurate frame / vehicle / inference totals)
        kpi_df = client.query_df("""
            SELECT
                uniqExact(camera_id) AS cameras_with_data,
                uniqExact(event_id) AS processed_frames,
                sumIf(count, vehicle_type_id IN (1, 2, 3, 4)) AS total_vehicles,
                avgIf(inference_time_ms, vehicle_type_id = 1) AS avg_inference_ms
            FROM fact_traffic_flow FINAL
            WHERE detection_mode = {mode:String}
        """, parameters={"mode": detection_mode})

        # 7. Latest Images list
        images_df = client.query_df("""
            SELECT
                img.image_id AS image_id,
                img.filename AS filename,
                img.s3_uri AS s3_uri,
                img.file_size_bytes AS file_size_bytes,
                min(f.timestamp) AS captured_at,
                f.camera_id AS camera_id,
                c.location AS location,
                any(f.congestion_level) AS congestion_level,
                sumIf(f.count, f.vehicle_type_id IN (1, 2, 3, 4)) AS total_vehicles,
                sumIf(f.count, f.vehicle_type_id = 1) AS cars,
                sumIf(f.count, f.vehicle_type_id = 2) AS motorcycles,
                sumIf(f.count, f.vehicle_type_id = 3) AS buses,
                sumIf(f.count, f.vehicle_type_id = 4) AS trucks,
                sumIf(f.count, f.vehicle_type_id = 5) AS pedestrians,
                max(f.inference_time_ms) AS inference_time_ms
            FROM (SELECT * FROM dim_images FINAL) AS img
            JOIN fact_traffic_flow AS f FINAL
                ON img.image_id = f.image_id
            JOIN (SELECT * FROM dim_cameras FINAL) AS c
                ON f.camera_id = c.camera_id
            WHERE f.detection_mode = {mode:String}
            GROUP BY
                img.image_id,
                img.filename,
                img.s3_uri,
                img.file_size_bytes,
                f.camera_id,
                c.location
            ORDER BY captured_at DESC
        """, parameters={"mode": detection_mode})

        client.close()
        return cameras_df, traffic_summary, congestion_levels, time_series, images_df, kpi_df
    except Exception as e:
        st.error(f"Failed to load data from ClickHouse: {e}")
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        )

# Main Layout
st.markdown('<div class="main-header">🚦 Trung Tâm Giám Sát Giao Thông Thông Minh</div>', unsafe_allow_html=True)
st.markdown('<div class="subheader">Hệ thống phân tích lưu lượng xe thời gian thực tích hợp AI (PySpark & ClickHouse Star Schema)</div>', unsafe_allow_html=True)

# Fetch Data
mode_labels = {
    "mock": "Mock — dữ liệu mô phỏng",
    "yolo": "YOLOv8 — nhận diện thật",
    "unknown": "Dữ liệu cũ — chưa rõ chế độ",
}

detection_mode = st.sidebar.selectbox(
    "Nguồn kết quả nhận diện",
    list(mode_labels),
    index=0,
    format_func=mode_labels.get,
)

st.caption(f"Đang hiển thị: {mode_labels[detection_mode]}")
cameras_df, traffic_summary, congestion_levels, time_series, images_df, kpi_df = fetch_dashboard_data(detection_mode)

if cameras_df.empty or traffic_summary.empty:
    st.warning("⚠️ Cơ sở dữ liệu ClickHouse chưa có dữ liệu giao thông hoặc bảng trống!")
    st.info("Hãy khởi động gRPC server (`python ingestion/server.py`), gửi ảnh qua client (`python ingestion/client.py`) và kích hoạt Spark Job để xử lý dữ liệu.")
    if st.button("🔄 Tải lại dữ liệu"):
        st.rerun()
else:
    # --- Sidebar Filters ---
    st.sidebar.markdown("### ⚙️ Cấu Hiện Tại")
    st.sidebar.markdown(f"**Kho Dữ Liệu:** `ClickHouse` ({CLICKHOUSE_HOST}:{CLICKHOUSE_PORT})")
    st.sidebar.markdown(f"**Kho Lưu Trữ:** `MinIO S3` ({MINIO_ENDPOINT})")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔍 Bộ lọc hiển thị")
    selected_camera = st.sidebar.selectbox("Chọn Camera giám sát:", ["Tất cả camera"] + list(cameras_df["camera_id"].unique()))

    # Filtered DataFrames based on sidebar selection
    if selected_camera != "Tất cả camera":
        filtered_ts = time_series[time_series["camera_id"] == selected_camera]
        filtered_summary = traffic_summary[traffic_summary["camera_id"] == selected_camera]
        filtered_congestion = congestion_levels[congestion_levels["camera_id"] == selected_camera]
        filtered_images = images_df[images_df["camera_id"] == selected_camera]
    else:
        filtered_ts = time_series
        filtered_summary = traffic_summary
        filtered_congestion = congestion_levels
        filtered_images = images_df

    # --- KPI: camera filter uses filtered frames; all-cameras uses SQL aggregates ---
    vehicle_rows = filtered_summary[filtered_summary["vehicle_type_name"].isin(VEHICLE_TYPE_NAMES)]
    if selected_camera == "Tất cả camera" and not kpi_df.empty:
        kpi_cameras = int(kpi_df.iloc[0]["cameras_with_data"] or 0)
        kpi_frames = int(kpi_df.iloc[0]["processed_frames"] or 0)
        kpi_vehicles = int(kpi_df.iloc[0]["total_vehicles"] or 0)
        kpi_infer = round(float(kpi_df.iloc[0]["avg_inference_ms"] or 0), 1)
    else:
        kpi_cameras = int(filtered_summary["camera_id"].nunique())
        kpi_frames = int(filtered_images["image_id"].nunique())
        kpi_vehicles = int(vehicle_rows["total_count"].sum()) if not vehicle_rows.empty else 0
        # Weighted-ish: average of per-bucket averages only when one camera selected
        kpi_infer = round(float(filtered_ts["avg_inference"].mean()), 1) if not filtered_ts.empty else 0.0

    # --- Top KPI Metrics ---
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""
            <div class="metric-container">
                <div class="metric-lbl">Camera Có Dữ Liệu</div>
                <div class="metric-val">{kpi_cameras}</div>
            </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
            <div class="metric-container">
                <div class="metric-lbl">Tổng Khung Hình Đã Xử Lý</div>
                <div class="metric-val">{kpi_frames}</div>
            </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
            <div class="metric-container">
                <div class="metric-lbl">Tổng Số Xe (Không Gồm Người Đi Bộ)</div>
                <div class="metric-val">{kpi_vehicles}</div>
            </div>
        """, unsafe_allow_html=True)
    with col4:
        st.markdown(f"""
            <div class="metric-container">
                <div class="metric-lbl">Thời Gian Chạy AI Trung Bình</div>
                <div class="metric-val">{kpi_infer} ms</div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Tabbed Main Content ---
    tab_dashboard, tab_map, tab_images, tab_video = st.tabs(["📊 Thống Kê & Đồ Thị", "🗺️ Bản Đồ Giao Thông", "🖼️ Xem Ảnh Chi Tiết (MinIO API)", "🎬 Giám Sát Video"])

    with tab_dashboard:
        # First row of graphs
        row1_col1, row1_col2 = st.columns([2, 1])
        
        with row1_col1:
            st.markdown("### 📈 Xu Hướng Lưu Lượng Giao Thông (Theo Thời Gian)")
            if not filtered_ts.empty:
                # Group by timestamp and camera if all cameras, else direct
                fig_ts = px.line(
                    filtered_ts,
                    x="timestamp",
                    y="total_vehicles",
                    color="location" if selected_camera == "Tất cả camera" else None,
                    title="Số lượng phương tiện lưu thông qua các khung giờ",
                    labels={"timestamp": "Thời gian", "total_vehicles": "Tổng số xe"},
                    markers=True,
                    color_discrete_sequence=px.colors.qualitative.Plotly
                )
                fig_ts.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font_color='#cbd5e1',
                    title_font_color='#38bdf8'
                )
                fig_ts.update_xaxes(showgrid=True, gridcolor='#334155')
                fig_ts.update_yaxes(showgrid=True, gridcolor='#334155')
                st.plotly_chart(fig_ts, width="stretch")
            else:
                st.info("Không có dữ liệu thời gian.")

        with row1_col2:
            st.markdown("### 🚦 Mức Độ Tắc Nghẽn")
            if not filtered_congestion.empty:
                # Aggregate congestion across filtered cameras
                cong_agg = filtered_congestion.groupby("congestion_level")["frame_count"].sum().reset_index()
                
                # Color map for congestion levels
                color_map = {"High": "#ef4444", "Medium": "#f59e0b", "Low": "#10b981"}
                
                fig_pie = px.pie(
                    cong_agg,
                    names="congestion_level",
                    values="frame_count",
                    title="Phân phối mức độ ùn tắc giao thông",
                    color="congestion_level",
                    color_discrete_map=color_map,
                    hole=0.4
                )
                fig_pie.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font_color='#cbd5e1',
                    title_font_color='#38bdf8',
                    legend_title_text="Trạng thái"
                )
                st.plotly_chart(fig_pie, width="stretch")
            else:
                st.info("Không có dữ liệu kẹt xe.")

        st.markdown("---")

        # Second row of graphs
        row2_col1, row2_col2 = st.columns([1, 1])
        
        with row2_col1:
            st.markdown("### 🚗 Tỷ Lệ Phương Tiện Theo Loại")
            if not filtered_summary.empty:
                # Align chart with KPI: vehicles only (exclude Pedestrian)
                vehicle_agg = (
                    filtered_summary[filtered_summary["vehicle_type_name"].isin(VEHICLE_TYPE_NAMES)]
                    .groupby("vehicle_type_name")["total_count"]
                    .sum()
                    .reset_index()
                )
                fig_bar = px.bar(
                    vehicle_agg,
                    x="vehicle_type_name",
                    y="total_count",
                    title="Tổng số lượng phương tiện theo chủng loại (không gồm người đi bộ)",
                    labels={"vehicle_type_name": "Loại xe", "total_count": "Số lượng xe đếm được"},
                    color="vehicle_type_name",
                    color_discrete_sequence=px.colors.sequential.Blues
                )
                fig_bar.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font_color='#cbd5e1',
                    title_font_color='#38bdf8',
                    showlegend=False
                )
                fig_bar.update_xaxes(showgrid=False)
                fig_bar.update_yaxes(showgrid=True, gridcolor='#334155')
                st.plotly_chart(fig_bar, width="stretch")
            else:
                st.info("Không có dữ liệu loại xe.")
                
        with row2_col2:
            st.markdown("### ⚡ Hiệu Năng Xử Lý AI")
            if not filtered_ts.empty:
                fig_inf = px.area(
                    filtered_ts,
                    x="timestamp",
                    y="avg_inference",
                    color="location" if selected_camera == "Tất cả camera" else None,
                    title="Thời gian AI phân tích (Inference time) qua các khung hình",
                    labels={"timestamp": "Thời gian", "avg_inference": "Thời gian xử lý (ms)"},
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig_inf.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font_color='#cbd5e1',
                    title_font_color='#38bdf8'
                )
                fig_inf.update_xaxes(showgrid=True, gridcolor='#334155')
                fig_inf.update_yaxes(showgrid=True, gridcolor='#334155')
                st.plotly_chart(fig_inf, width="stretch")
            else:
                st.info("Không có dữ liệu hiệu năng.")

    with tab_map:
        st.markdown("### 🗺️ Vị Trí Các Camera Giám Sát Trên Bản Đồ")
        # Ensure we have coordinates
        map_df = cameras_df.dropna(subset=["latitude", "longitude"])
        if not map_df.empty:
            # Display interactive map
            st.map(map_df, latitude="latitude", longitude="longitude", size=20, zoom=12)
            
            # Show cameras list table
            st.markdown("**Danh sách chi tiết camera:**")
            st.dataframe(
                cameras_df.rename(columns={
                    "camera_id": "Mã Camera",
                    "location": "Vị Trí Lắp Đặt",
                    "latitude": "Vĩ Độ (Lat)",
                    "longitude": "Kinh Độ (Lng)"
                }),
                width="stretch",
                hide_index=True
            )
        else:
            st.warning("Không có dữ liệu tọa độ vĩ độ/kinh độ của các camera.")

    with tab_images:
        st.markdown("### 📸 Truy Xuất Hình Ảnh Camera & Số Liệu AI")
        st.markdown("Chọn một khung hình trong danh sách để xem ảnh chụp camera thực tế lưu trên MinIO S3 cùng với kết quả đếm xe chi tiết của Spark:")
        
        if not filtered_images.empty:
            # Streamlit options must be scalar IDs, never pandas Series.
            # Use a unique image ID to retrieve the complete frame record.
            image_rows = filtered_images.drop_duplicates(subset=["image_id"]).copy()
            image_rows["image_id"] = image_rows["image_id"].astype(str)
            image_by_id = image_rows.set_index("image_id", drop=False)
            image_options = image_rows["image_id"].tolist()

            def format_image_option(image_id):
                r = image_by_id.loc[image_id]
                timestamp = pd.to_datetime(r["captured_at"])
                return (
                    f"{r['camera_id']} | {r['location']} | "
                    f"{timestamp.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"(Xe đếm được: {r['total_vehicles']}, "
                    f"Ùn tắc: {r['congestion_level']})"
                )

            selected_image_id = st.selectbox(
                "Chọn khung hình muốn kiểm tra:",
                options=image_options,
                format_func=format_image_option,
                key="selected_detail_image_id",
            )

            if selected_image_id is not None:
                frame_details = image_by_id.loc[selected_image_id]
                image_id = selected_image_id
                s3_uri = frame_details["s3_uri"]
                cam_id = frame_details["camera_id"]
                loc = frame_details["location"]
                captured_at = pd.to_datetime(frame_details["captured_at"])
                cong = frame_details["congestion_level"]

                # Retrieve from MinIO
                with st.spinner("🔄 Đang tải hình ảnh từ kho lưu trữ MinIO S3..."):
                    prefer_overlay = detection_mode == "yolo" and st.checkbox(
                        "Hiển thị bounding box YOLOv8", value=True, key="image_detail_overlay"
                    )
                    display_uri = annotated_s3_uri(s3_uri) if prefer_overlay else s3_uri
                    img_file = load_minio_image(display_uri) if display_uri else None
                    if img_file is None and prefer_overlay:
                        img_file = load_minio_image(s3_uri)
                        st.caption("Frame này chưa có ảnh bounding box trong MinIO; đang hiển thị ảnh gốc.")
                    
                img_col, details_col = st.columns([5, 3])
                
                with img_col:
                    if img_file:
                        st.image(img_file, caption=f"Ảnh chụp camera thực tế từ {cam_id} ({loc}) vào {captured_at}", width="stretch")
                    else:
                        st.error("❌ Không thể lấy ảnh từ MinIO S3. Vui lòng kiểm tra kết nối tới container MinIO (localhost:9000).")
                        
                with details_col:
                    st.markdown(f'<div class="camera-badge">{cam_id}</div>', unsafe_allow_html=True)
                    st.markdown(f"**Vị trí:** `{loc}`")
                    st.markdown(f"**Thời gian chụp:** `{captured_at.strftime('%Y-%m-%d %H:%M:%S')}`")
                    
                    # Congestion color box
                    cong_colors = {"High": "red", "Medium": "orange", "Low": "green"}
                    cong_color = cong_colors.get(cong, "gray")
                    st.markdown(f"**Mức độ kẹt xe:** <span style='color:{cong_color}; font-weight:bold;'>{cong}</span>", unsafe_allow_html=True)
                    st.markdown(f"**Image ID:** `{image_id}`")
                    st.markdown(f"**Đường dẫn S3:** `{s3_uri}`")
                    
                    st.markdown("---")
                    st.markdown("### 📋 Kết Quả Phân Tích Phương Tiện")
                    st.metric("Tổng số phương tiện", int(frame_details["total_vehicles"]))
                    st.metric("Thời gian xử lý frame", f'{int(frame_details["inference_time_ms"])} ms')
                    st.caption("Thời gian này bao gồm tải/giải mã ảnh và suy luận trong Spark; không chỉ riêng model.predict().")
                    
                    # Fetch specific counts for this image
                    try:
                        client = get_ch_client()
                        counts_df = client.query_df("""
                            SELECT v.vehicle_type_name as `Loại Xe`, f.count as `Số Lượng`
                            FROM fact_traffic_flow AS f FINAL
                            JOIN (SELECT * FROM dim_vehicles FINAL) AS v ON f.vehicle_type_id = v.vehicle_type_id
                            WHERE f.image_id = {image_id:String}
                              AND f.detection_mode = {mode:String}
                            ORDER BY `Số Lượng` DESC
                        """, parameters={
                                "image_id": image_id,
                                "mode": detection_mode
                                })
                        client.close()
                        
                        st.table(counts_df)
                    except Exception as e:
                        st.error(f"Lỗi truy vấn chi tiết phương tiện từ ClickHouse: {e}")
        else:
            st.info("Không có khung hình nào phù hợp với bộ lọc hiện tại.")


    with tab_video:
        st.markdown("### 🎬 Phát lại frame video từ MinIO")
        st.caption("Đây là chuỗi frame đã được YOLOv8 xử lý và lưu trữ, không phải video trực tiếp. Mỗi camera có thể dùng một video nguồn riêng.")
        video_frames = filtered_images[
            filtered_images["filename"].astype(str).str.contains(r"_frame\d+\.jpg$", regex=True, case=False, na=False)
        ].copy()
        if detection_mode != "yolo":
            st.info("Chọn 'YOLOv8 — nhận diện thật' trong thanh bên để xem frame video đã nhận diện.")
        elif video_frames.empty:
            st.info("Chưa có frame video phù hợp với camera đang chọn. Hãy gửi video từ client và đợi Spark xử lý.")
        else:
            # Filenames from video client follow:
            # CAM-HN-001_YYYYMMDD_HHMMSS_microseconds_frame000001.jpg
            # Group by recording/upload minute across all cameras, not by frame position
            # across the entire history. This is a time-window filter, not a true run ID.
            video_frames["session_minute"] = video_frames["filename"].astype(str).str.extract(
                r"_((?:20\d{6})_\d{4})\d{2}_\d+_frame\d+\.jpg$",
                expand=False,
            )
            video_frames["frame_number"] = pd.to_numeric(
                video_frames["filename"].astype(str).str.extract(
                    r"_frame(\d+)\.jpg$", expand=False
                ), errors="coerce"
            )
            session_minutes = sorted(video_frames["session_minute"].dropna().unique().tolist(), reverse=True)
            if not session_minutes:
                st.warning("Không tìm thấy mã thời gian phiên trong tên frame. Hãy kiểm tra quy tắc đặt tên của ingestion/client.py.")
                st.stop()
            selected_session = st.selectbox(
                "Chọn phiên video (theo phút gửi frame)",
                options=session_minutes,
                format_func=lambda x: f"{x[:4]}-{x[4:6]}-{x[6:8]} {x[9:11]}:{x[11:13]} · {x}",
                key="video_replay_session",
            )
            session_frames = video_frames[video_frames["session_minute"] == selected_session].copy()
            session_frames = session_frames.sort_values(["camera_id", "frame_number", "captured_at", "filename"])
            video_camera_ids = sorted(session_frames["camera_id"].astype(str).unique().tolist())
            chosen_cameras = st.multiselect(
                "Camera phát lại (tối đa 3)", video_camera_ids,
                default=video_camera_ids[:3], max_selections=3,
                key="video_replay_cameras",
            )
            fps = st.slider("Tốc độ phát lại (frame/giây)", 1, 3, 1, key="video_replay_fps")
            show_overlay = st.checkbox(
                "Hiển thị bounding box YOLOv8 trên video", value=True,
                key="video_replay_overlay"
            )
            st.caption("Phiên được nhóm theo phút trong tên file. Nếu một lần gửi kéo dài qua hai phút, hãy chọn từng phút để xem; đây chưa phải mã phiên duy nhất.")
            streams = {}
            for camera_id in chosen_cameras:
                camera_rows = session_frames[session_frames["camera_id"] == camera_id].tail(100)
                streams[camera_id] = camera_rows.to_dict("records")
            if streams:
                st.caption("Số frame trong phiên: " + " · ".join(f"{cam}: {len(rows)}" for cam, rows in streams.items()))
            max_frames = max((len(rows) for rows in streams.values()), default=0)
            if max_frames:
                # Reset the cursor when camera selection or dataset changes.
                signature = (selected_session, tuple(chosen_cameras), tuple(
                    (cam, len(rows), str(rows[-1]["image_id"])) for cam, rows in streams.items()
                ))
                if st.session_state.get("video_replay_signature") != signature:
                    st.session_state.video_replay_signature = signature
                    st.session_state.video_replay_index = 0
                    st.session_state.video_replay_playing = False
                st.session_state.video_replay_index = min(
                    st.session_state.get("video_replay_index", 0), max_frames - 1
                )
                controls = st.columns([1, 1, 1, 3])
                if controls[0].button("▶ Phát", key="video_play"):
                    if st.session_state.video_replay_index >= max_frames - 1:
                        st.session_state.video_replay_index = 0
                    st.session_state.video_replay_playing = True
                if controls[1].button("⏸ Dừng", key="video_pause"):
                    st.session_state.video_replay_playing = False
                if controls[2].button("↺ Đầu", key="video_reset"):
                    st.session_state.video_replay_index = 0
                    st.session_state.video_replay_playing = False
                # Fragment refreshes only the replay, not the entire dashboard.
                @st.fragment(run_every="1s")
                def render_video_replay():
                    idx = min(st.session_state.video_replay_index, max_frames - 1)
                    st.progress((idx + 1) / max_frames,
                                text=f"Frame {idx + 1}/{max_frames} · {'Đang phát' if st.session_state.video_replay_playing else 'Đang dừng'}")
                    columns = st.columns(len(streams))
                    for col, (camera_id, frames) in zip(columns, streams.items()):
                        with col:
                            st.markdown(f"**📹 {camera_id}**")
                            if idx >= len(frames):
                                st.info("Camera này đã hết frame.")
                                continue
                            frame = frames[idx]
                            overlay_uri = annotated_s3_uri(frame["s3_uri"]) if show_overlay else None
                            data = load_video_frame_bytes(overlay_uri) if overlay_uri else None
                            overlay_found = data is not None
                            if data is None:
                                data = load_video_frame_bytes(frame["s3_uri"])
                            if data:
                                st.image(data, width="stretch")
                                if show_overlay and not overlay_found:
                                    st.caption("Ảnh gốc · chưa có bounding box cho frame này")
                            else:
                                st.warning("Không tải được frame từ MinIO.")
                            st.caption(f"{frame['captured_at']} · {frame['filename']}")
                            st.markdown(
                                f"**Xe: {int(frame['total_vehicles'])}** · "
                                f"**Ùn tắc: {frame['congestion_level']}** · "
                                f"**Xử lý: {int(frame['inference_time_ms'])} ms**"
                            )
                            st.caption(
                                f"🚗 Ô tô: {int(frame['cars'])}  |  "
                                f"🏍️ Xe máy: {int(frame['motorcycles'])}  |  "
                                f"🚌 Xe buýt: {int(frame['buses'])}  |  "
                                f"🚚 Xe tải: {int(frame['trucks'])}  |  "
                                f"🚶 Người đi bộ: {int(frame['pedestrians'])}"
                            )
                    if st.session_state.video_replay_playing:
                        # At 1 FPS advance each tick. At higher FPS this advances
                        # several frames per tick; the UI still refreshes once/second.
                        next_idx = idx + fps
                        if next_idx >= max_frames:
                            st.session_state.video_replay_index = max_frames - 1
                            st.session_state.video_replay_playing = False
                        else:
                            st.session_state.video_replay_index = next_idx
                render_video_replay()
            else:
                st.info("Hãy chọn ít nhất một camera để phát lại.")

# Automatic Refresh Button
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Làm mới dữ liệu bảng (Force Refresh)"):
    st.cache_data.clear()
    st.rerun()