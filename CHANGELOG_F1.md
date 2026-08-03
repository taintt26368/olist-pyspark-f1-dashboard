# CHANGELOG F1

Tệp này được tạo tự động từ kết quả chạy chính thức trong `f1_results.json` và các output bước 05.

## File đã sửa, xác nhận hoặc tạo

- `01_doc_va_kiem_tra_du_lieu.py`: đã xác nhận `RAW_DIR = PROJECT_DIR / data / raw`; file đã đúng trước thời điểm backup nên không cần sửa trong đợt F1.
- `02_lam_sach_du_lieu.py`: đã xác nhận `RAW_DIR = PROJECT_DIR / data / raw`; file đã đúng trước thời điểm backup nên không cần sửa trong đợt F1.
- `05_huan_luyen_va_danh_gia_mo_hinh.py`: chuyển toàn bộ quyết định chính thức từ F2 sang F1.
- `bao_cao_kiem_chung_05_F1.md`: báo cáo kiểm chứng mới từ lần chạy hiện tại.
- `162_tieu_luan_olist_pyspark_F1.docx`: bản tiểu luận F1, không ghi đè bản gốc.
- `slide_thuyet_trinh_olist_F1.pptx`: deck F1 gồm 14 slide và Speaker Notes mới.

## Tiêu chí lựa chọn

- Tiêu chí cũ: F2/average_f2/minimum_f2.
- Tiêu chí mới: average_f1 cao nhất, minimum_f1 cao nhất, average_recall cao nhất, average_alert_rate thấp nhất, rồi common threshold cao hơn.
- Điều kiện hợp lệ: alert rate validation của Logistic Regression và Random Forest đều không vượt 20%.
- Số candidate thực tế: **110**.
- Common threshold mới: **0,094**.
- Model minh họa mới: **Logistic Regression**, được chọn chỉ từ validation.

## Kết quả validation mới

| Model | TP | TN | FP | FN | Accuracy | Precision | Recall | F1 | AUC | alert rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Logistic Regression | 458 | 11.692 | 2.586 | 589 | 79,28% | 15,05% | 43,74% | 22,39% | 0,695851 | 19,86% |
| Random Forest | 192 | 13.772 | 506 | 855 | 91,12% | 27,51% | 18,34% | 22,01% | 0,684502 | 4,55% |

## Kết quả test mới

| Phương pháp | TP | TN | FP | FN | Accuracy | Precision | Recall | F1 | AUC | alert rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Baseline majority class | 0 | 17.830 | 0 | 1.261 | 93,39% | 0,00% | 0,00% | 0,00% | 0,500000 | 0,00% |
| Logistic Regression | 553 | 14.473 | 3.357 | 708 | 78,71% | 14,14% | 43,85% | 21,39% | 0,701896 | 20,48% |
| Random Forest | 241 | 17.057 | 773 | 1.020 | 90,61% | 23,77% | 19,11% | 21,19% | 0,687284 | 5,31% |

## Vị trí đã cập nhật trong Word

- Tóm tắt, mục tiêu/câu hỏi nghiên cứu, cơ sở lý thuyết metric và baseline.
- Chương 6: data split, Pipeline, common threshold, validation, test, confusion matrix, ROC/AUC, probability distribution, so sánh model, assertion, hạn chế và kết luận chương.
- Chương 7: kết quả, giá trị học máy, hạn chế, kiến nghị và kết luận chung.
- Phụ lục threshold và ví dụ tính score; toàn bộ hình bước 05 được thay bằng output F1 hiện tại.

## Slide và Notes đã cập nhật

- Slide 9: làm rõ F1 chỉ dùng đánh giá/lựa chọn, không dùng để fit model.
- Slide 10: phân biệt AUC, F1 và common threshold.
- Slide 11: số candidate thực tế, quy tắc F1 và khóa quyết định trước test.
- Slide 12: biểu đồ F1, confusion matrix và kết quả test mới; baseline 0 hiển thị nhãn 0,00%.
- Slide 13: model minh họa, kết luận và hạn chế theo F1 validation.
- Speaker Notes của cả 14 slide: viết lại từ output hiện tại, loại F2 và số liệu threshold 0,5 cũ.

## Kiểm tra cuối

| Kiểm tra | Trạng thái |
|---|---|
| Dataset gốc không đổi hash | PASS |
| Bước 01–04 giữ nguyên số liệu chính thức | PASS |
| Syntax bước 05 | PASS |
| Chạy 01 → 05 | PASS |
| Common threshold/model chỉ chọn trên validation | PASS |
| Confusion matrix và công thức F1 | PASS |
| AUC Spark khớp AUC hình thang | PASS |
| Logistic Regression/Random Forest probability manual | PASS |
| Assertion bước 05 | PASS (716 kiểm tra) |
| Tái lập hai lần với seed 42 | MATCH |
| Word render và kiểm tra nội dung | PASS |
| PowerPoint overflow và template fidelity | PASS |

Reproducibility signature: `ec543f47703be58e1068a1bebb9c44c632f3e9766da82d87ddb134b460c25dbd`.
