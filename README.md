# Olist PySpark F1 dashboard

## Link public

- Streamlit: https://olist-f1-pyspark.streamlit.app/
- GitHub: https://github.com/taintt26368/olist-pyspark-f1-dashboard

Dashboard Streamlit trình bày toàn bộ quy trình phân tích dữ liệu và đánh giá model của project Olist bằng PySpark.

## Nội dung

- Kiểm tra dữ liệu gốc, làm sạch và kiểm soát fan-out khi join.
- Khám phá tỷ lệ giao trễ theo thời gian, khu vực và đặc điểm order.
- Chia `train_fit`, `validation`, `train_full` và `test` với seed 42.
- So sánh Baseline majority class, Logistic Regression và Random Forest.
- Chọn một common threshold bằng F1 trên validation, không dùng test để lựa chọn.
- Kiểm chứng confusion matrix, AUC, probability score và reproducibility.
- Tải báo cáo Markdown, tiểu luận Word, PowerPoint, source code, CSV và PNG.

## Chạy local

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python -m streamlit run streamlit_app.py
```

Xem thêm [README_STREAMLIT.md](README_STREAMLIT.md) để biết cấu trúc và hướng dẫn deploy.

## Dữ liệu công khai

Repository không chứa CSV raw, dữ liệu processed, `orders_enriched.csv`, môi trường ảo, backup hoặc Spark warehouse. Dashboard chỉ phân phối các bảng/biểu đồ tổng hợp và tài liệu bằng chứng của lần chạy chính thức.
