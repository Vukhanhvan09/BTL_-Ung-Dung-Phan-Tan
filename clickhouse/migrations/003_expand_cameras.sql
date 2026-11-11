USE traffic_dw;

INSERT INTO dim_cameras (camera_id, location, latitude, longitude, created_at) VALUES
('CAM-HN-004', 'Hoan Kiem Lake - Trang Tien', 21.0245, 105.8520, now()),
('CAM-HN-005', 'Long Bien Bridge Approach', 21.0430, 105.8600, now()),
('CAM-HN-006', 'Pham Van Dong - Co Nhue Intersection', 21.0605, 105.7800, now()),
('CAM-HN-007', 'Nguyen Trai - Thanh Xuan', 20.9955, 105.8095, now()),
('CAM-HN-008', 'Giai Phong - Dai La Intersection', 20.9990, 105.8410, now());
