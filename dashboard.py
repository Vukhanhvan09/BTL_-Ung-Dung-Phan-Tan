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

# Load Data from ClickHouse
@st.cache_data(ttl=10) # Refresh data cache every 10 seconds
def fetch_dashboard_data():
    try:
        client = get_ch_client()
        
        # 1. KPI Aggregations
        kpis = client.query("""
            SELECT 
                (SELECT count(distinct image_id) FROM dim_images) as total_images,
                (SELECT sum(count) FROM fact_traffic_flow) as total_vehicles,
                (SELECT round(avg(inference_time_ms), 1) FROM fact_traffic_flow) as avg_inference
        """).result_rows[0]
        
        # 2. Cameras
        cameras_df = client.query_df("SELECT camera_id, location, latitude, longitude FROM dim_cameras")
        cameras_df["latitude"] = cameras_df["latitude"].astype(float)
        cameras_df["longitude"] = cameras_df["longitude"].astype(float)
        
        # 3. Traffic Summary Grouped
        traffic_summary = client.query_df("""
            SELECT 
                f.camera_id AS camera_id, 
                c.location AS location, 
                v.vehicle_type_name AS vehicle_type_name, 
                sum(f.count) as total_count, 
                avg(f.count) as avg_count
            FROM fact_traffic_flow f
            JOIN dim_cameras c ON f.camera_id = c.camera_id
            JOIN dim_vehicles v ON f.vehicle_type_id = v.vehicle_type_id
            GROUP BY f.camera_id, c.location, v.vehicle_type_name
        """)
        
        # 4. Congestion Levels
        congestion_levels = client.query_df("""
            SELECT 
                camera_id, congestion_level, count(distinct event_id) as frame_count
            FROM fact_traffic_flow
            GROUP BY camera_id, congestion_level
        """)
        
        # 5. Time series traffic flow
        time_series = client.query_df("""
            SELECT 
                f.timestamp AS timestamp, 
                f.camera_id AS camera_id, 
                c.location AS location,
                sum(f.count) as total_vehicles,
                avg(f.inference_time_ms) as avg_inference
            FROM fact_traffic_flow f
            JOIN dim_cameras c ON f.camera_id = c.camera_id
            GROUP BY f.timestamp, f.camera_id, c.location
            ORDER BY f.timestamp ASC
        """)
        
        # 6. Latest Images list (for details pane)
        images_df = client.query_df("""
            SELECT 
                img.image_id AS image_id, 
                img.filename AS filename, 
                img.s3_uri AS s3_uri, 
                img.file_size_bytes AS file_size_bytes, 
                img.uploaded_at AS uploaded_at,
                f.camera_id AS camera_id, 
                c.location AS location, 
                f.congestion_level AS congestion_level,
                sum(f.count) as total_vehicles
            FROM dim_images img
            JOIN fact_traffic_flow f ON img.image_id = f.image_id
            JOIN dim_cameras c ON f.camera_id = c.camera_id
            GROUP BY img.image_id, img.filename, img.s3_uri, img.file_size_bytes, img.uploaded_at, f.camera_id, c.location, f.congestion_level
            ORDER BY img.uploaded_at DESC
        """)
        
        client.close()
        return kpis, cameras_df, traffic_summary, congestion_levels, time_series, images_df
    except Exception as e:
        st.error(f"Failed to load data from ClickHouse: {e}")
        return None, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# Main Layout
st.markdown('<div class="main-header">🚦 Trung Tâm Giám Sát Giao Thông Thông Minh</div>', unsafe_allow_html=True)
st.markdown('<div class="subheader">Hệ thống phân tích lưu lượng xe thời gian thực tích hợp AI (PySpark & ClickHouse Star Schema)</div>', unsafe_allow_html=True)

# Fetch Data
kpis, cameras_df, traffic_summary, congestion_levels, time_series, images_df = fetch_dashboard_data()

if cameras_df.empty or traffic_summary.empty:
    st.warning("⚠️ Cơ sở dữ liệu ClickHouse chưa có dữ liệu giao thông hoặc bảng trống!")
    st.info("Hãy khởi động gRPC server (`python ingestion/server.py`), gửi ảnh qua client (`python ingestion/client.py`) và kích hoạt Spark Job để xử lý dữ liệu.")
    if st.button("🔄 Tải lại dữ liệu"):
        st.rerun()
else:
    # --- Top KPI Metrics ---
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""
            <div class="metric-container">
                <div class="metric-lbl">Tổng Số Camera Hoạt Động</div>
                <div class="metric-val">{len(cameras_df)}</div>
            </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
            <div class="metric-container">
                <div class="metric-lbl">Tổng Khung Hình Đã Xử Lý</div>
                <div class="metric-val">{kpis[0] if kpis else 0}</div>
            </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
            <div class="metric-container">
                <div class="metric-lbl">Tổng Số Xe Đã Nhận Diện</div>
                <div class="metric-val">{kpis[1] if kpis else 0}</div>
            </div>
        """, unsafe_allow_html=True)
    with col4:
        st.markdown(f"""
            <div class="metric-container">
                <div class="metric-lbl">Thời Gian Chạy AI Trình Trung Bình</div>
                <div class="metric-val">{kpis[2] if kpis else 0} ms</div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

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

    # --- Tabbed Main Content ---
    tab_dashboard, tab_map, tab_images = st.tabs(["📊 Thống Kê & Đồ Thị", "🗺️ Bản Đồ Giao Thông", "🖼️ Xem Ảnh Chi Tiết (MinIO API)"])

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
                st.plotly_chart(fig_ts, use_container_width=True)
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
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("Không có dữ liệu kẹt xe.")

        st.markdown("---")

        # Second row of graphs
        row2_col1, row2_col2 = st.columns([1, 1])
        
        with row2_col1:
            st.markdown("### 🚗 Tỷ Lệ Phương Tiện Theo Loại")
            if not filtered_summary.empty:
                vehicle_agg = filtered_summary.groupby("vehicle_type_name")["total_count"].sum().reset_index()
                fig_bar = px.bar(
                    vehicle_agg,
                    x="vehicle_type_name",
                    y="total_count",
                    title="Tổng số lượng phương tiện theo chủng loại",
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
                st.plotly_chart(fig_bar, use_container_width=True)
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
                st.plotly_chart(fig_inf, use_container_width=True)
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
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning("Không có dữ liệu tọa độ vĩ độ/kinh độ của các camera.")

    with tab_images:
        st.markdown("### 📸 Truy Xuất Hình Ảnh Camera & Số Liệu AI")
        st.markdown("Chọn một khung hình trong danh sách để xem ảnh chụp camera thực tế lưu trên MinIO S3 cùng với kết quả đếm xe chi tiết của Spark:")
        
        if not filtered_images.empty:
            # Create a selection list
            image_options = []
            for _, r in filtered_images.iterrows():
                # Formatter: Camera_ID - Location - Time (Vehicles: X, Congestion: Y)
                label = f"{r['camera_id']} | {r['location']} | {r['uploaded_at'].strftime('%Y-%m-%d %H:%M:%S')} (Xe đếm được: {r['total_vehicles']}, Ùn tắc: {r['congestion_level']})"
                image_options.append((label, r['s3_uri'], r['image_id'], r['camera_id'], r['location'], r['uploaded_at'], r['congestion_level']))
                
            selected_option = st.selectbox(
                "Chọn khung hình muốn kiểm tra:",
                options=image_options,
                format_func=lambda x: x[0]
            )
            
            if selected_option:
                _, s3_uri, image_id, cam_id, loc, uploaded_at, cong = selected_option
                
                # Retrieve from MinIO
                with st.spinner("🔄 Đang tải hình ảnh từ kho lưu trữ MinIO S3..."):
                    img_file = load_minio_image(s3_uri)
                    
                img_col, details_col = st.columns([5, 3])
                
                with img_col:
                    if img_file:
                        st.image(img_file, caption=f"Ảnh chụp camera thực tế từ {cam_id} ({loc}) vào {uploaded_at}", use_container_width=True)
                    else:
                        st.error("❌ Không thể lấy ảnh từ MinIO S3. Vui lòng kiểm tra kết nối tới container MinIO (localhost:9000).")
                        
                with details_col:
                    st.markdown(f'<div class="camera-badge">{cam_id}</div>', unsafe_allow_html=True)
                    st.markdown(f"**Vị trí:** `{loc}`")
                    st.markdown(f"**Thời gian tải lên:** `{uploaded_at.strftime('%Y-%m-%d %H:%M:%S')}`")
                    
                    # Congestion color box
                    cong_colors = {"High": "red", "Medium": "orange", "Low": "green"}
                    cong_color = cong_colors.get(cong, "gray")
                    st.markdown(f"**Mức độ kẹt xe:** <span style='color:{cong_color}; font-weight:bold;'>{cong}</span>", unsafe_allow_html=True)
                    st.markdown(f"**Image ID:** `{image_id}`")
                    st.markdown(f"**Đường dẫn S3:** `{s3_uri}`")
                    
                    st.markdown("---")
                    st.markdown("### 📋 Kết Quả Phân Tích Phương Tiện")
                    
                    # Fetch specific counts for this image
                    try:
                        client = get_ch_client()
                        counts_df = client.query_df(f"""
                            SELECT v.vehicle_type_name as `Loại Xe`, f.count as `Số Lượng`
                            FROM fact_traffic_flow f
                            JOIN dim_vehicles v ON f.vehicle_type_id = v.vehicle_type_id
                            WHERE f.image_id = '{image_id}'
                            ORDER BY `Số Lượng` DESC
                        """)
                        client.close()
                        
                        st.table(counts_df)
                    except Exception as e:
                        st.error(f"Lỗi truy vấn chi tiết phương tiện từ ClickHouse: {e}")
        else:
            st.info("Không có khung hình nào phù hợp với bộ lọc hiện tại.")

# Automatic Refresh Button
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Làm mới dữ liệu bảng (Force Refresh)"):
    st.cache_data.clear()
    st.rerun()
