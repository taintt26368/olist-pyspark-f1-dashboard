# Dashboard Streamlit cho project Olist PySpark

## Link đã triển khai

- Ứng dụng public: https://olist-f1-pyspark.streamlit.app/
- Source code: https://github.com/taintt26368/olist-pyspark-f1-dashboard

Ứng dụng trình bày kết quả chính thức của quy trình 01–05 mà không huấn luyện lại model mỗi lần người xem mở trang. Mọi KPI, bảng và biểu đồ đều được đọc từ `outputs/tables` và `outputs/charts`.

## Chạy local

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python -m streamlit run streamlit_app.py
```

Mở `http://localhost:8501`.

## Cấu trúc giao diện

- Tổng quan nghiên cứu và lần chạy chính thức.
- Dữ liệu, làm sạch, join, data split và 716 assertions.
- EDA theo tháng, bang, phạm vi vận chuyển và số sản phẩm.
- Quy trình chọn common threshold bằng F1 trên validation.
- So sánh Baseline, Logistic Regression và Random Forest.
- Bốn order thật A, B, C, D và kiểm chứng probability score.
- Trang tải báo cáo Markdown, Word, PowerPoint, source code, CSV và PNG.

## Deploy Streamlit Community Cloud

1. Đưa project lên một GitHub repository. `.gitignore` đã loại dữ liệu gốc, môi trường ảo, backup và Spark warehouse khỏi repository.
2. Truy cập `https://share.streamlit.io` và kết nối tài khoản GitHub.
3. Chọn repository, branch và entrypoint `streamlit_app.py`.
4. Chọn Python tương thích với các dependency trong `requirements.txt` rồi deploy.
5. Kiểm tra đủ bảy trang và các nút tải file trước khi gửi link cho giảng viên.

Không commit `.streamlit/secrets.toml`. Ứng dụng hiện tại không cần secret.
