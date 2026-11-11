# Real Image Mode

Đặt ảnh `.jpg` / `.png` giao thông vào thư mục này, rồi chạy:

```powershell
$env:CAMERA_MODE="real"
python ingestion/client.py
```

Nếu thư mục trống, hãy dùng `CAMERA_MODE=synthetic` (mặc định).
