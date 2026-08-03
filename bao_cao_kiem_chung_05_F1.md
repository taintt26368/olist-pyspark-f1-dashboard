# BÁO CÁO KIỂM CHỨNG BƯỚC 05 — LẦN CHẠY CHÍNH THỨC DÙNG F1

## PHẦN 1. THÔNG TIN LẦN CHẠY

- Thời gian bắt đầu: `2026-07-31T21:51:11.950204+07:00`
- Thời gian kết thúc: `2026-07-31T21:52:11.313806+07:00`
- Thời lượng: `59.385` giây
- Đường dẫn project: `C:\data_python\olist_tieu_luan`
- Đường dẫn dataset: `C:\data_python\olist_tieu_luan\data\output\orders_enriched.csv`
- Python: `3.11.15`
- PySpark: `4.1.2`
- Java: `17.0.19`
- Run tag: `official`
- Seed: `42`
- Threshold mode: `AUTO_VALIDATION`
- MANUAL_COMMON_THRESHOLD: `None`
- Threshold thử thủ công: `[0.09, 0.094, 0.1]`
- Số dòng dataset: `96,470`
- SHA-256 dataset trước và sau chạy: `7301d0c09c808be88ef35203991797fcdf23c3b414f33de978c995b9371601e1`
- Dataset gốc không bị sửa: **PASS** (SHA-256, kích thước và thời điểm sửa file không đổi).
- Danh sách file output:

  - `outputs\tables\05_thong_tin_chia_du_lieu.csv`
  - `outputs\tables\05_preprocessing_details.csv`
  - `outputs\tables\05_string_indexer_mapping.csv`
  - `outputs\tables\05_feature_vector_metadata.csv`
  - `outputs\tables\05_validation_common_thresholds.csv`
  - `outputs\tables\05_xep_hang_common_threshold_F1.csv`
  - `outputs\tables\05_so_sanh_threshold_thu_cong.csv`
  - `outputs\tables\05_common_threshold_duoc_chon.csv`
  - `outputs\tables\05_ket_qua_validation_common_threshold.csv`
  - `outputs\tables\05_confusion_matrix_validation.csv`
  - `outputs\tables\05_ket_qua_test_common_threshold.csv`
  - `outputs\tables\05_confusion_matrix_test.csv`
  - `outputs\tables\05_ket_qua_baseline.csv`
  - `outputs\tables\05_logistic_coefficients.csv`
  - `outputs\tables\05_logistic_score_breakdown_orders.csv`
  - `outputs\tables\05_random_forest_feature_importances.csv`
  - `outputs\tables\05_random_forest_score_breakdown_orders.csv`
  - `outputs\tables\05_random_forest_tree_details.csv`
  - `outputs\tables\05_roc_points_logistic_regression.csv`
  - `outputs\tables\05_roc_points_random_forest.csv`
  - `outputs\tables\05_roc_points_baseline.csv`
  - `outputs\tables\05_auc_trapezoids_logistic_regression.csv`
  - `outputs\tables\05_auc_trapezoids_random_forest.csv`
  - `outputs\tables\05_auc_trapezoids_baseline.csv`
  - `outputs\tables\05_demo_orders_A_B_C_D.csv`
  - `outputs\tables\05_danh_sach_feature.csv`
  - `outputs\tables\05_so_sanh_mo_hinh.csv`
  - `outputs\tables\05_ma_tran_nham_lan.csv`
  - `outputs\tables\05_chan_doan_xac_suat.csv`
  - `outputs\tables\05_code_line_reference.csv`
  - `outputs\tables\05_assertion_checks.csv`
  - `outputs\tables\05_run_metadata.csv`
  - `outputs\tables\05_reproducibility_signature.csv`
  - `outputs\tables\05_reproducibility_check.csv`
  - `outputs\charts\05_common_threshold_validation.png`
  - `outputs\charts\05_confusion_matrix_logistic_regression.png`
  - `outputs\charts\05_confusion_matrix_random_forest.png`
  - `outputs\charts\05_confusion_matrix_baseline.png`
  - `outputs\charts\05_roc_curve_test.png`
  - `outputs\charts\05_probability_distribution_test.png`
  - `outputs\charts\05_so_sanh_mo_hinh.png`
  - `bao_cao_kiem_chung_05_F1.md`

## PHẦN 2. SƠ ĐỒ QUY TRÌNH

```text
dataset
  → train_full / test
  → train_fit / validation (chỉ chia tiếp train_full)
  → preprocessing fit trên train_fit
  → baseline majority class xác định chỉ từ train
  → Logistic Regression và Random Forest
  → common threshold selection trên validation
  → khóa common threshold và model demo
  → retrain preprocessing và hai model trên train_full
  → đánh giá baseline và hai model trên cùng test split
  → kiểm chứng score, confusion matrix, AUC và công thức
```

## PHẦN 3. BẢNG “DÒNG CODE – TÍNH NĂNG – CÔNG THỨC – SỐ LIỆU THẬT”

| Dòng code thực tế | Code thực tế | Tính năng | Công thức | Số liệu thật | Cách kiểm tra |
| --- | --- | --- | --- | --- | --- |
| 2427-2432 | `data = (<br>            spark.read<br>            .option("header", True)<br>            .option("inferSchema", True)<br>            .csv(DATA_FILE.as_uri())<br>        ).cache()` | Đọc dataset | spark.read.csv | 96470 dòng | Assertion/CSV/report từ lần chạy hiện tại |
| 270-277 | `feature_leakage = sorted(<br>        set(danh_sach_feature).intersection(COT_LEAKAGE_CAM)<br>    )<br>    feature_ngoai_danh_sach = sorted(<br>        set(danh_sach_feature) - set(cot_duoc_phep)<br>    )<br>    xac_nhan(<br>        "Không có feature leakage",` | Kiểm tra feature leakage | selected features ∩ leakage columns = ∅ | PASS | Assertion/CSV/report từ lần chạy hiện tại |
| 291-298 | `ket_qua = df.agg(*[<br>        F.sum(<br>            F.when(<br>                F.isnan(F.col(ten_cot))<br>                \| (F.col(ten_cot) == float("inf"))<br>                \| (F.col(ten_cot) == float("-inf")),<br>                1,<br>            ).otherwise(0)` | Kiểm tra NaN/Infinity | isnan(x) hoặc x=±Infinity | PASS | Assertion/CSV/report từ lần chạy hiện tại |
| 2466-2473 | `data_model = data.select(<br>            "order_id",<br>            *COT_PHAN_LOAI,<br>            *COT_SO,<br>            F.col("is_late").cast("double").alias("is_late"),<br>        )<br>        for ten_cot in COT_PHAN_LOAI:<br>            data_model = data_model.withColumn(` | Tạo label input | label = cast(is_late as double) | late=6534 | Assertion/CSV/report từ lần chạy hiện tại |
| 2491-2495 | `train_full, test = data_model.randomSplit([0.8, 0.2], seed=SEED)<br>        train_full = train_full.orderBy("order_id").cache()<br>        test = test.orderBy("order_id").cache()<br>        so_train_full = train_full.count()<br>        so_test = test.count()` | Chia train_full/test | 80%/20%, seed=42 | 77379/19091 | Assertion/CSV/report từ lần chạy hiện tại |
| 2497-2504 | `train_fit, validation = train_full.randomSplit(<br>            [0.8, 0.2],<br>            seed=SEED,<br>        )<br>        train_fit = train_fit.orderBy("order_id").cache()<br>        validation = validation.orderBy("order_id").cache()<br>        so_train_fit = train_fit.count()<br>        so_validation = validation.count()` | Chia train_fit/validation | 80%/20% của train_full, seed=42 | 62054/15325 | Assertion/CSV/report từ lần chạy hiện tại |
| 354-361 | `indexers = [<br>        StringIndexer(<br>            inputCol=cot,<br>            outputCol=f"{cot}_index",<br>            handleInvalid="keep",<br>        )<br>        for cot in COT_PHAN_LOAI<br>    ]` | StringIndexer | category → index | 3 categorical feature | Assertion/CSV/report từ lần chạy hiện tại |
| 363-367 | `encoder = OneHotEncoder(<br>        inputCols=cot_chi_so,<br>        outputCols=cot_ma_hoa,<br>        handleInvalid="keep",<br>    )` | OneHotEncoder | index → sparse vector | vector=141 | Assertion/CSV/report từ lần chạy hiện tại |
| 369-373 | `imputer = Imputer(<br>        inputCols=COT_SO,<br>        outputCols=cot_so_da_dien,<br>        strategy="median",<br>    )` | Imputer | median theo train scope | 15 numeric feature | Assertion/CSV/report từ lần chạy hiện tại |
| 375-380 | `assembler = VectorAssembler(<br>        inputCols=cot_so_da_dien + cot_ma_hoa,<br>        outputCol="features",<br>        handleInvalid="keep",<br>    )<br>    return Pipeline(stages=indexers + [encoder, imputer, assembler])` | VectorAssembler | filled numeric + encoded categorical | dimension=141 | Assertion/CSV/report từ lần chạy hiện tại |
| 674-681 | `counts = (<br>        df_train<br>        .groupBy("is_late")<br>        .count()<br>        .orderBy(F.col("count").desc(), F.col("is_late").asc())<br>        .collect()<br>    )<br>    xac_nhan(` | Tạo baseline | majority class chỉ xác định từ train_full; áp dụng cố định lên test | prediction=0, AUC=0.500000 | Assertion/CSV/report từ lần chạy hiện tại |
| 575 | `model_logistic = logistic.fit(df_ready)` | Fit Logistic Regression | maxIter=50, regParam=0.01 | intercept=-366.682799587486 | Assertion/CSV/report từ lần chạy hiện tại |
| 577-578 | `model_random_forest = random_forest.fit(df_ready)<br>    return model_logistic, model_random_forest` | Fit Random Forest | numTrees=30, maxDepth=6, seed=42 | trees=30 | Assertion/CSV/report từ lần chạy hiện tại |
| 584-590 | `return (<br>        model.transform(df_ready)<br>        .withColumn(<br>            "probability_late",<br>            vector_to_array("probability")[1],<br>        )<br>    )` | Tạo probability_late | vector_to_array(probability)[1] | hai model | Assertion/CSV/report từ lần chạy hiện tại |
| 954-961 | `coarse_rows = [<br>        danh_gia_common_threshold(<br>            danh_sach_validation,<br>            threshold,<br>            "coarse",<br>            auc_logistic,<br>            auc_random_forest,<br>        )` | Tìm common threshold | coarse 0.01; refine 0.001 | 110 candidate | Assertion/CSV/report từ lần chạy hiện tại |
| 933-939 | `hop_le = [dong for dong in candidates if dong["hop_le_alert_rate"]]<br>    xac_nhan(<br>        "Có common threshold thỏa alert rate của cả hai model",<br>        bool(hop_le),<br>        f"số candidate hợp lệ={len(hop_le)}",<br>    )<br>    return max(hop_le, key=khoa_xep_hang_threshold)` | Chọn common threshold | max average_f1 → minimum_f1 → average_recall → min average_alert_rate → max threshold | 0.094 | Assertion/CSV/report từ lần chạy hiện tại |
| 884-891 | `thresholds = sorted({float(x) for x in danh_sach_threshold})<br>    xac_nhan(<br>        f"Threshold thử thủ công của {ten_tap} nằm trong [0, 1]",<br>        bool(thresholds) and all(0.0 <= x <= 1.0 for x in thresholds),<br>        f"thresholds={thresholds}",<br>    )<br>    rows = []<br>    for threshold in thresholds:` | So sánh threshold thủ công | tính lại confusion matrix và metrics tại danh sách threshold cấu hình | 6 dòng | Assertion/CSV/report từ lần chạy hiện tại |
| 712-718 | `return df_probability.withColumn(<br>        "prediction_common",<br>        F.when(<br>            F.col("probability_late") >= F.lit(common_threshold),<br>            F.lit(1.0),<br>        ).otherwise(F.lit(0.0)),<br>    )` | Tạo prediction_common | 1 nếu probability_late ≥ common threshold | 0.094 | Assertion/CSV/report từ lần chạy hiện tại |
| 728-735 | `dong = df_prediction.agg(<br>        F.sum(<br>            F.when(<br>                (F.col("is_late") == 1)<br>                & (F.col("prediction_common") == 1),<br>                1,<br>            ).otherwise(0)<br>        ).alias("tp"),` | Tính confusion matrix | đếm TP, TN, FP, FN | TP=553, TN=14473, FP=3357, FN=708 | Assertion/CSV/report từ lần chạy hiện tại |
| 601 | `accuracy = chia_an_toan(tp + tn, n)` | Tính Accuracy | (TP+TN)/N | 0.787072442512 | Assertion/CSV/report từ lần chạy hiện tại |
| 603 | `precision = chia_an_toan(tp, tp + fp)` | Tính Precision | TP/(TP+FP) | 0.141432225064 | Assertion/CSV/report từ lần chạy hiện tại |
| 605 | `recall = chia_an_toan(tp, tp + fn)` | Tính Recall | TP/(TP+FN) | 0.438540840603 | Assertion/CSV/report từ lần chạy hiện tại |
| 607 | `specificity = chia_an_toan(tn, tn + fp)` | Tính Specificity | TN/(TN+FP) | 0.811721817162 | Assertion/CSV/report từ lần chạy hiện tại |
| 609 | `fpr = chia_an_toan(fp, fp + tn)` | Tính FPR | FP/(FP+TN) | 0.188278182838 | Assertion/CSV/report từ lần chạy hiện tại |
| 611-612 | `f1 = chia_an_toan(2 * precision * recall, precision + recall)<br>    f1_truc_tiep = chia_an_toan(2 * tp, 2 * tp + fp + fn)` | Tính F1 | 2×Precision×Recall/(Precision+Recall) | 0.213885128602 | Assertion/CSV/report từ lần chạy hiện tại |
| 614-621 | `alert_rate = chia_an_toan(tp + fp, n)<br>    prevalence = chia_an_toan(tp + fn, n)<br>    xac_nhan(<br>        "F1 theo hai công thức khớp nhau",<br>        abs(f1 - f1_truc_tiep) <= 1e-12,<br>        f"F1={f1:.17g}, F1 trực tiếp={f1_truc_tiep:.17g}",<br>    )<br>    cac_chi_so = [` | Tính alert rate | (TP+FP)/N | 0.204808548531 | Assertion/CSV/report từ lần chạy hiện tại |
| 991-998 | `scores = [<br>        (<br>            float(dong["probability_late"]),<br>            int(dong["is_late"]),<br>            dong["order_id"],<br>        )<br>        for dong in df_probability.select(<br>            "order_id", "is_late", "probability_late"` | Tính ROC points | sắp probability giảm dần; TPR=TP/P, FPR=FP/N | LR=19081 points | Assertion/CSV/report từ lần chạy hiện tại |
| 1051-1058 | `dien_tich = (<br>            (tiep_theo["fpr"] - hien_tai["fpr"])<br>            * (tiep_theo["tpr"] + hien_tai["tpr"])<br>            / 2<br>        )<br>        trapezoids.append({<br>            "model": ten_model,<br>            "trapezoid_index": index,` | Tính AUC bằng trapezoid | ΔFPR×(TPR_i+TPR_i+1)/2 | 0.701896135099 | Assertion/CSV/report từ lần chạy hiện tại |
| 1235-1242 | `probability_manual = sigmoid_on_dinh(z_manual)<br>        probability_spark = float(dong["probability_logistic"])<br>        absolute_difference = abs(probability_manual - probability_spark)<br>        xac_nhan(<br>            f"Logistic Regression probability order {dong['alias']} khớp Spark",<br>            absolute_difference <= 1e-10,<br>            "manual="<br>            f"{probability_manual:.17g}, Spark={probability_spark:.17g}, "` | Kiểm chứng Logistic Regression probability | sigmoid(intercept+Σ coefficient×value) | PASS cho A, B, C, D | Assertion/CSV/report từ lần chạy hiện tại |
| 1319-1326 | `probability_manual = chia_an_toan(raw_model[1], raw_total)<br>        probability_spark = float(dong["probability_random_forest"])<br>        probability_difference = abs(probability_manual - probability_spark)<br>        xac_nhan(<br>            f"Random Forest probability order {dong['alias']} khớp Spark",<br>            probability_difference <= 1e-10,<br>            "manual="<br>            f"{probability_manual:.17g}, Spark={probability_spark:.17g}, "` | Kiểm chứng Random Forest probability | rawPrediction[1]/ΣrawPrediction | PASS cho A, B, C, D | Assertion/CSV/report từ lần chạy hiện tại |
| 1117-1124 | `quy_tac = {<br>        "A": (<br>            (F.col("is_late") == 1) & (F.col("prediction_common") == 1),<br>            False,<br>            "True Positive có probability_late cao nhất",<br>        ),<br>        "B": (<br>            (F.col("is_late") == 0) & (F.col("prediction_common") == 1),` | Chọn order A, B, C, D | quy tắc xác định trên test của model demo | A=f46b842d9b4dfd29acf5eec998837ede, B=686c0ba20be3837a5041edbc39d3f9ae, C=9b1d71b20edcf15ab15e0bb4a932f23f, D=c2bb89b5c1dd978d507284be78a04cb2 | Assertion/CSV/report từ lần chạy hiện tại |

## PHẦN 4. DATASET VÀ DATA SPLIT

- `toan_bo` late rate = 6,534 / 96,470 = 0.067730900798 = 6.773090%.
- `train_full` late rate = 5,273 / 77,379 = 0.068145103969 = 6.814510%.
- `train_fit` late rate = 4,226 / 62,054 = 0.068101975699 = 6.810198%.
- `validation` late rate = 1,047 / 15,325 = 0.068319738989 = 6.831974%.
- `test` late rate = 1,261 / 19,091 = 0.066052066419 = 6.605207%.

Kiểm tra tổng: 77,379 + 19,091 = 96,470; 62,054 + 15,325 = 77,379.

| Tập | Dòng | order_id khác nhau | late | not late | late rate | Giao nhau |
| --- | --- | --- | --- | --- | --- | --- |
| toan_bo | 96470 | 96470 | 6534 | 89936 | 6.773090% | không áp dụng |
| train_full | 77379 | 77379 | 5273 | 72106 | 6.814510% | với test: 0 |
| train_fit | 62054 | 62054 | 4226 | 57828 | 6.810198% | với validation: 0 |
| validation | 15325 | 15325 | 1047 | 14278 | 6.831974% | với train_fit: 0 |
| test | 19091 | 19091 | 1261 | 17830 | 6.605207% | với train_full: 0 |

## PHẦN 5. PREPROCESSING

Pipeline lựa chọn common threshold chỉ fit trên `train_fit`; Pipeline final chỉ fit lại trên `train_full`. Không fit trên validation hoặc test.

### Median và kích thước

| Phạm vi fit | Component | Feature gốc | Chi tiết | Giá trị |
| --- | --- | --- | --- | --- |
| train_fit | Pipeline | tất cả | số dòng trước preprocessing | 62054 |
| train_fit | Pipeline | tất cả | số dòng sau preprocessing | 62054 |
| train_fit | VectorAssembler | features | tổng chiều dài features vector | 141 |
| train_fit | Imputer | item_count | median | 1.0 |
| train_fit | Imputer | product_count | median | 1.0 |
| train_fit | Imputer | seller_count | median | 1.0 |
| train_fit | Imputer | total_price | median | 85.99 |
| train_fit | Imputer | total_freight | median | 17.14 |
| train_fit | Imputer | average_item_price | median | 79.0 |
| train_fit | Imputer | total_weight_g | median | 750.0 |
| train_fit | Imputer | total_volume_cm3 | median | 7200.0 |
| train_fit | Imputer | freight_ratio | median | 0.2244 |
| train_fit | Imputer | purchase_year | median | 2018.0 |
| train_fit | Imputer | purchase_month | median | 6.0 |
| train_fit | Imputer | purchase_day_of_week | median | 4.0 |
| train_fit | Imputer | purchase_hour | median | 15.0 |
| train_fit | Imputer | estimated_delivery_days | median | 24.0 |
| train_fit | Imputer | customer_seller_same_state | median | 0.0 |
| train_fit | StringIndexer | customer_state | số category thực tế | 27 |
| train_fit | StringIndexer | customer_state | số label trong mapping | 27 |
| train_fit | OneHotEncoder | customer_state | categorySizes | 28 |
| train_fit | OneHotEncoder | customer_state | kích thước output vector | 28 |
| train_fit | StringIndexer | main_seller_state | số category thực tế | 22 |
| train_fit | StringIndexer | main_seller_state | số label trong mapping | 22 |
| train_fit | OneHotEncoder | main_seller_state | categorySizes | 23 |
| train_fit | OneHotEncoder | main_seller_state | kích thước output vector | 23 |
| train_fit | StringIndexer | main_category | số category thực tế | 74 |
| train_fit | StringIndexer | main_category | số label trong mapping | 74 |
| train_fit | OneHotEncoder | main_category | categorySizes | 75 |
| train_fit | OneHotEncoder | main_category | kích thước output vector | 75 |
| train_full | Pipeline | tất cả | số dòng trước preprocessing | 77379 |
| train_full | Pipeline | tất cả | số dòng sau preprocessing | 77379 |
| train_full | VectorAssembler | features | tổng chiều dài features vector | 141 |
| train_full | Imputer | item_count | median | 1.0 |
| train_full | Imputer | product_count | median | 1.0 |
| train_full | Imputer | seller_count | median | 1.0 |
| train_full | Imputer | total_price | median | 85.8 |
| train_full | Imputer | total_freight | median | 17.16 |
| train_full | Imputer | average_item_price | median | 79.0 |
| train_full | Imputer | total_weight_g | median | 750.0 |
| train_full | Imputer | total_volume_cm3 | median | 7250.0 |
| train_full | Imputer | freight_ratio | median | 0.2244 |
| train_full | Imputer | purchase_year | median | 2018.0 |
| train_full | Imputer | purchase_month | median | 6.0 |
| train_full | Imputer | purchase_day_of_week | median | 4.0 |
| train_full | Imputer | purchase_hour | median | 15.0 |
| train_full | Imputer | estimated_delivery_days | median | 24.0 |
| train_full | Imputer | customer_seller_same_state | median | 0.0 |
| train_full | StringIndexer | customer_state | số category thực tế | 27 |
| train_full | StringIndexer | customer_state | số label trong mapping | 27 |
| train_full | OneHotEncoder | customer_state | categorySizes | 28 |
| train_full | OneHotEncoder | customer_state | kích thước output vector | 28 |
| train_full | StringIndexer | main_seller_state | số category thực tế | 22 |
| train_full | StringIndexer | main_seller_state | số label trong mapping | 22 |
| train_full | OneHotEncoder | main_seller_state | categorySizes | 23 |
| train_full | OneHotEncoder | main_seller_state | kích thước output vector | 23 |
| train_full | StringIndexer | main_category | số category thực tế | 74 |
| train_full | StringIndexer | main_category | số label trong mapping | 74 |
| train_full | OneHotEncoder | main_category | categorySizes | 75 |
| train_full | OneHotEncoder | main_category | kích thước output vector | 75 |

### StringIndexer mapping

| Phạm vi fit | Feature gốc | String index | Category | Invalid |
| --- | --- | --- | --- | --- |
| train_fit | customer_state | 0 | SP | 0 |
| train_fit | customer_state | 1 | RJ | 0 |
| train_fit | customer_state | 2 | MG | 0 |
| train_fit | customer_state | 3 | RS | 0 |
| train_fit | customer_state | 4 | PR | 0 |
| train_fit | customer_state | 5 | SC | 0 |
| train_fit | customer_state | 6 | BA | 0 |
| train_fit | customer_state | 7 | DF | 0 |
| train_fit | customer_state | 8 | ES | 0 |
| train_fit | customer_state | 9 | GO | 0 |
| train_fit | customer_state | 10 | PE | 0 |
| train_fit | customer_state | 11 | CE | 0 |
| train_fit | customer_state | 12 | PA | 0 |
| train_fit | customer_state | 13 | MT | 0 |
| train_fit | customer_state | 14 | MA | 0 |
| train_fit | customer_state | 15 | MS | 0 |
| train_fit | customer_state | 16 | PB | 0 |
| train_fit | customer_state | 17 | PI | 0 |
| train_fit | customer_state | 18 | RN | 0 |
| train_fit | customer_state | 19 | AL | 0 |
| train_fit | customer_state | 20 | SE | 0 |
| train_fit | customer_state | 21 | TO | 0 |
| train_fit | customer_state | 22 | RO | 0 |
| train_fit | customer_state | 23 | AM | 0 |
| train_fit | customer_state | 24 | AC | 0 |
| train_fit | customer_state | 25 | AP | 0 |
| train_fit | customer_state | 26 | RR | 0 |
| train_fit | customer_state | 27 | __unknown_or_unseen__ | 1 |
| train_fit | main_seller_state | 0 | SP | 0 |
| train_fit | main_seller_state | 1 | MG | 0 |
| train_fit | main_seller_state | 2 | PR | 0 |
| train_fit | main_seller_state | 3 | RJ | 0 |
| train_fit | main_seller_state | 4 | SC | 0 |
| train_fit | main_seller_state | 5 | RS | 0 |
| train_fit | main_seller_state | 6 | DF | 0 |
| train_fit | main_seller_state | 7 | BA | 0 |
| train_fit | main_seller_state | 8 | GO | 0 |
| train_fit | main_seller_state | 9 | MA | 0 |
| train_fit | main_seller_state | 10 | PE | 0 |
| train_fit | main_seller_state | 11 | ES | 0 |
| train_fit | main_seller_state | 12 | MT | 0 |
| train_fit | main_seller_state | 13 | CE | 0 |
| train_fit | main_seller_state | 14 | RN | 0 |
| train_fit | main_seller_state | 15 | MS | 0 |
| train_fit | main_seller_state | 16 | PB | 0 |
| train_fit | main_seller_state | 17 | RO | 0 |
| train_fit | main_seller_state | 18 | PI | 0 |
| train_fit | main_seller_state | 19 | PA | 0 |
| train_fit | main_seller_state | 20 | SE | 0 |
| train_fit | main_seller_state | 21 | AM | 0 |
| train_fit | main_seller_state | 22 | __unknown_or_unseen__ | 1 |
| train_fit | main_category | 0 | bed_bath_table | 0 |
| train_fit | main_category | 1 | health_beauty | 0 |
| train_fit | main_category | 2 | sports_leisure | 0 |
| train_fit | main_category | 3 | computers_accessories | 0 |
| train_fit | main_category | 4 | furniture_decor | 0 |
| train_fit | main_category | 5 | housewares | 0 |
| train_fit | main_category | 6 | watches_gifts | 0 |
| train_fit | main_category | 7 | telephony | 0 |
| train_fit | main_category | 8 | auto | 0 |
| train_fit | main_category | 9 | toys | 0 |
| train_fit | main_category | 10 | cool_stuff | 0 |
| train_fit | main_category | 11 | garden_tools | 0 |
| train_fit | main_category | 12 | perfumery | 0 |
| train_fit | main_category | 13 | baby | 0 |
| train_fit | main_category | 14 | electronics | 0 |
| train_fit | main_category | 15 | stationery | 0 |
| train_fit | main_category | 16 | fashion_bags_accessories | 0 |
| train_fit | main_category | 17 | pet_shop | 0 |
| train_fit | main_category | 18 | khong_xac_dinh | 0 |
| train_fit | main_category | 19 | office_furniture | 0 |
| train_fit | main_category | 20 | luggage_accessories | 0 |
| train_fit | main_category | 21 | consoles_games | 0 |
| train_fit | main_category | 22 | construction_tools_construction | 0 |
| train_fit | main_category | 23 | home_appliances | 0 |
| train_fit | main_category | 24 | musical_instruments | 0 |
| train_fit | main_category | 25 | small_appliances | 0 |
| train_fit | main_category | 26 | books_general_interest | 0 |
| train_fit | main_category | 27 | home_construction | 0 |
| train_fit | main_category | 28 | food | 0 |
| train_fit | main_category | 29 | furniture_living_room | 0 |
| train_fit | main_category | 30 | audio | 0 |
| train_fit | main_category | 31 | home_confort | 0 |
| train_fit | main_category | 32 | market_place | 0 |
| train_fit | main_category | 33 | drinks | 0 |
| train_fit | main_category | 34 | books_technical | 0 |
| train_fit | main_category | 35 | construction_tools_lights | 0 |
| train_fit | main_category | 36 | air_conditioning | 0 |
| train_fit | main_category | 37 | food_drink | 0 |
| train_fit | main_category | 38 | home_appliances_2 | 0 |
| train_fit | main_category | 39 | fashion_shoes | 0 |
| train_fit | main_category | 40 | kitchen_dining_laundry_garden_furniture | 0 |
| train_fit | main_category | 41 | industry_commerce_and_business | 0 |
| train_fit | main_category | 42 | fixed_telephony | 0 |
| train_fit | main_category | 43 | costruction_tools_garden | 0 |
| train_fit | main_category | 44 | art | 0 |
| train_fit | main_category | 45 | computers | 0 |
| train_fit | main_category | 46 | agro_industry_and_commerce | 0 |
| train_fit | main_category | 47 | construction_tools_safety | 0 |
| train_fit | main_category | 48 | fashion_underwear_beach | 0 |
| train_fit | main_category | 49 | signaling_and_security | 0 |
| train_fit | main_category | 50 | christmas_supplies | 0 |
| train_fit | main_category | 51 | fashion_male_clothing | 0 |
| train_fit | main_category | 52 | costruction_tools_tools | 0 |
| train_fit | main_category | 53 | furniture_bedroom | 0 |
| train_fit | main_category | 54 | cine_photo | 0 |
| train_fit | main_category | 55 | small_appliances_home_oven_and_coffee | 0 |
| train_fit | main_category | 56 | tablets_printing_image | 0 |
| train_fit | main_category | 57 | books_imported | 0 |
| train_fit | main_category | 58 | dvds_blu_ray | 0 |
| train_fit | main_category | 59 | music | 0 |
| train_fit | main_category | 60 | party_supplies | 0 |
| train_fit | main_category | 61 | furniture_mattress_and_upholstery | 0 |
| train_fit | main_category | 62 | fashio_female_clothing | 0 |
| train_fit | main_category | 63 | flowers | 0 |
| train_fit | main_category | 64 | home_comfort_2 | 0 |
| train_fit | main_category | 65 | fashion_sport | 0 |
| train_fit | main_category | 66 | arts_and_craftmanship | 0 |
| train_fit | main_category | 67 | diapers_and_hygiene | 0 |
| train_fit | main_category | 68 | la_cuisine | 0 |
| train_fit | main_category | 69 | portateis_cozinha_e_preparadores_de_alimentos | 0 |
| train_fit | main_category | 70 | cds_dvds_musicals | 0 |
| train_fit | main_category | 71 | fashion_childrens_clothes | 0 |
| train_fit | main_category | 72 | pc_gamer | 0 |
| train_fit | main_category | 73 | security_and_services | 0 |
| train_fit | main_category | 74 | __unknown_or_unseen__ | 1 |
| train_full | customer_state | 0 | SP | 0 |
| train_full | customer_state | 1 | RJ | 0 |
| train_full | customer_state | 2 | MG | 0 |
| train_full | customer_state | 3 | RS | 0 |
| train_full | customer_state | 4 | PR | 0 |
| train_full | customer_state | 5 | SC | 0 |
| train_full | customer_state | 6 | BA | 0 |
| train_full | customer_state | 7 | DF | 0 |
| train_full | customer_state | 8 | ES | 0 |
| train_full | customer_state | 9 | GO | 0 |
| train_full | customer_state | 10 | PE | 0 |
| train_full | customer_state | 11 | CE | 0 |
| train_full | customer_state | 12 | PA | 0 |
| train_full | customer_state | 13 | MT | 0 |
| train_full | customer_state | 14 | MA | 0 |
| train_full | customer_state | 15 | MS | 0 |
| train_full | customer_state | 16 | PB | 0 |
| train_full | customer_state | 17 | PI | 0 |
| train_full | customer_state | 18 | RN | 0 |
| train_full | customer_state | 19 | AL | 0 |
| train_full | customer_state | 20 | SE | 0 |
| train_full | customer_state | 21 | TO | 0 |
| train_full | customer_state | 22 | RO | 0 |
| train_full | customer_state | 23 | AM | 0 |
| train_full | customer_state | 24 | AC | 0 |
| train_full | customer_state | 25 | AP | 0 |
| train_full | customer_state | 26 | RR | 0 |
| train_full | customer_state | 27 | __unknown_or_unseen__ | 1 |
| train_full | main_seller_state | 0 | SP | 0 |
| train_full | main_seller_state | 1 | MG | 0 |
| train_full | main_seller_state | 2 | PR | 0 |
| train_full | main_seller_state | 3 | RJ | 0 |
| train_full | main_seller_state | 4 | SC | 0 |
| train_full | main_seller_state | 5 | RS | 0 |
| train_full | main_seller_state | 6 | DF | 0 |
| train_full | main_seller_state | 7 | BA | 0 |
| train_full | main_seller_state | 8 | GO | 0 |
| train_full | main_seller_state | 9 | PE | 0 |
| train_full | main_seller_state | 10 | MA | 0 |
| train_full | main_seller_state | 11 | ES | 0 |
| train_full | main_seller_state | 12 | MT | 0 |
| train_full | main_seller_state | 13 | CE | 0 |
| train_full | main_seller_state | 14 | MS | 0 |
| train_full | main_seller_state | 15 | RN | 0 |
| train_full | main_seller_state | 16 | PB | 0 |
| train_full | main_seller_state | 17 | RO | 0 |
| train_full | main_seller_state | 18 | PI | 0 |
| train_full | main_seller_state | 19 | PA | 0 |
| train_full | main_seller_state | 20 | SE | 0 |
| train_full | main_seller_state | 21 | AM | 0 |
| train_full | main_seller_state | 22 | __unknown_or_unseen__ | 1 |
| train_full | main_category | 0 | bed_bath_table | 0 |
| train_full | main_category | 1 | health_beauty | 0 |
| train_full | main_category | 2 | sports_leisure | 0 |
| train_full | main_category | 3 | computers_accessories | 0 |
| train_full | main_category | 4 | furniture_decor | 0 |
| train_full | main_category | 5 | housewares | 0 |
| train_full | main_category | 6 | watches_gifts | 0 |
| train_full | main_category | 7 | telephony | 0 |
| train_full | main_category | 8 | auto | 0 |
| train_full | main_category | 9 | toys | 0 |
| train_full | main_category | 10 | cool_stuff | 0 |
| train_full | main_category | 11 | garden_tools | 0 |
| train_full | main_category | 12 | perfumery | 0 |
| train_full | main_category | 13 | baby | 0 |
| train_full | main_category | 14 | electronics | 0 |
| train_full | main_category | 15 | stationery | 0 |
| train_full | main_category | 16 | fashion_bags_accessories | 0 |
| train_full | main_category | 17 | pet_shop | 0 |
| train_full | main_category | 18 | khong_xac_dinh | 0 |
| train_full | main_category | 19 | office_furniture | 0 |
| train_full | main_category | 20 | luggage_accessories | 0 |
| train_full | main_category | 21 | consoles_games | 0 |
| train_full | main_category | 22 | home_appliances | 0 |
| train_full | main_category | 23 | construction_tools_construction | 0 |
| train_full | main_category | 24 | musical_instruments | 0 |
| train_full | main_category | 25 | small_appliances | 0 |
| train_full | main_category | 26 | books_general_interest | 0 |
| train_full | main_category | 27 | home_construction | 0 |
| train_full | main_category | 28 | food | 0 |
| train_full | main_category | 29 | furniture_living_room | 0 |
| train_full | main_category | 30 | home_confort | 0 |
| train_full | main_category | 31 | audio | 0 |
| train_full | main_category | 32 | drinks | 0 |
| train_full | main_category | 33 | market_place | 0 |
| train_full | main_category | 34 | books_technical | 0 |
| train_full | main_category | 35 | air_conditioning | 0 |
| train_full | main_category | 36 | kitchen_dining_laundry_garden_furniture | 0 |
| train_full | main_category | 37 | construction_tools_lights | 0 |
| train_full | main_category | 38 | fashion_shoes | 0 |
| train_full | main_category | 39 | industry_commerce_and_business | 0 |
| train_full | main_category | 40 | food_drink | 0 |
| train_full | main_category | 41 | home_appliances_2 | 0 |
| train_full | main_category | 42 | fixed_telephony | 0 |
| train_full | main_category | 43 | costruction_tools_garden | 0 |
| train_full | main_category | 44 | art | 0 |
| train_full | main_category | 45 | computers | 0 |
| train_full | main_category | 46 | agro_industry_and_commerce | 0 |
| train_full | main_category | 47 | construction_tools_safety | 0 |
| train_full | main_category | 48 | signaling_and_security | 0 |
| train_full | main_category | 49 | fashion_underwear_beach | 0 |
| train_full | main_category | 50 | christmas_supplies | 0 |
| train_full | main_category | 51 | fashion_male_clothing | 0 |
| train_full | main_category | 52 | costruction_tools_tools | 0 |
| train_full | main_category | 53 | furniture_bedroom | 0 |
| train_full | main_category | 54 | tablets_printing_image | 0 |
| train_full | main_category | 55 | cine_photo | 0 |
| train_full | main_category | 56 | small_appliances_home_oven_and_coffee | 0 |
| train_full | main_category | 57 | dvds_blu_ray | 0 |
| train_full | main_category | 58 | books_imported | 0 |
| train_full | main_category | 59 | party_supplies | 0 |
| train_full | main_category | 60 | furniture_mattress_and_upholstery | 0 |
| train_full | main_category | 61 | music | 0 |
| train_full | main_category | 62 | fashio_female_clothing | 0 |
| train_full | main_category | 63 | flowers | 0 |
| train_full | main_category | 64 | arts_and_craftmanship | 0 |
| train_full | main_category | 65 | fashion_sport | 0 |
| train_full | main_category | 66 | home_comfort_2 | 0 |
| train_full | main_category | 67 | diapers_and_hygiene | 0 |
| train_full | main_category | 68 | la_cuisine | 0 |
| train_full | main_category | 69 | portateis_cozinha_e_preparadores_de_alimentos | 0 |
| train_full | main_category | 70 | cds_dvds_musicals | 0 |
| train_full | main_category | 71 | fashion_childrens_clothes | 0 |
| train_full | main_category | 72 | pc_gamer | 0 |
| train_full | main_category | 73 | security_and_services | 0 |
| train_full | main_category | 74 | __unknown_or_unseen__ | 1 |

### Vị trí transformed feature trong features vector

| Phạm vi fit | Vị trí | Transformed feature name | Feature gốc | Metadata type |
| --- | --- | --- | --- | --- |
| train_fit | 0 | item_count_filled | item_count | numeric |
| train_fit | 1 | product_count_filled | product_count | numeric |
| train_fit | 2 | seller_count_filled | seller_count | numeric |
| train_fit | 3 | total_price_filled | total_price | numeric |
| train_fit | 4 | total_freight_filled | total_freight | numeric |
| train_fit | 5 | average_item_price_filled | average_item_price | numeric |
| train_fit | 6 | total_weight_g_filled | total_weight_g | numeric |
| train_fit | 7 | total_volume_cm3_filled | total_volume_cm3 | numeric |
| train_fit | 8 | freight_ratio_filled | freight_ratio | numeric |
| train_fit | 9 | purchase_year_filled | purchase_year | numeric |
| train_fit | 10 | purchase_month_filled | purchase_month | numeric |
| train_fit | 11 | purchase_day_of_week_filled | purchase_day_of_week | numeric |
| train_fit | 12 | purchase_hour_filled | purchase_hour | numeric |
| train_fit | 13 | estimated_delivery_days_filled | estimated_delivery_days | numeric |
| train_fit | 14 | customer_seller_same_state_filled | customer_seller_same_state | numeric |
| train_fit | 15 | customer_state_ohe_SP | customer_state | binary |
| train_fit | 16 | customer_state_ohe_RJ | customer_state | binary |
| train_fit | 17 | customer_state_ohe_MG | customer_state | binary |
| train_fit | 18 | customer_state_ohe_RS | customer_state | binary |
| train_fit | 19 | customer_state_ohe_PR | customer_state | binary |
| train_fit | 20 | customer_state_ohe_SC | customer_state | binary |
| train_fit | 21 | customer_state_ohe_BA | customer_state | binary |
| train_fit | 22 | customer_state_ohe_DF | customer_state | binary |
| train_fit | 23 | customer_state_ohe_ES | customer_state | binary |
| train_fit | 24 | customer_state_ohe_GO | customer_state | binary |
| train_fit | 25 | customer_state_ohe_PE | customer_state | binary |
| train_fit | 26 | customer_state_ohe_CE | customer_state | binary |
| train_fit | 27 | customer_state_ohe_PA | customer_state | binary |
| train_fit | 28 | customer_state_ohe_MT | customer_state | binary |
| train_fit | 29 | customer_state_ohe_MA | customer_state | binary |
| train_fit | 30 | customer_state_ohe_MS | customer_state | binary |
| train_fit | 31 | customer_state_ohe_PB | customer_state | binary |
| train_fit | 32 | customer_state_ohe_PI | customer_state | binary |
| train_fit | 33 | customer_state_ohe_RN | customer_state | binary |
| train_fit | 34 | customer_state_ohe_AL | customer_state | binary |
| train_fit | 35 | customer_state_ohe_SE | customer_state | binary |
| train_fit | 36 | customer_state_ohe_TO | customer_state | binary |
| train_fit | 37 | customer_state_ohe_RO | customer_state | binary |
| train_fit | 38 | customer_state_ohe_AM | customer_state | binary |
| train_fit | 39 | customer_state_ohe_AC | customer_state | binary |
| train_fit | 40 | customer_state_ohe_AP | customer_state | binary |
| train_fit | 41 | customer_state_ohe_RR | customer_state | binary |
| train_fit | 42 | customer_state_ohe___unknown | customer_state | binary |
| train_fit | 43 | main_seller_state_ohe_SP | main_seller_state | binary |
| train_fit | 44 | main_seller_state_ohe_MG | main_seller_state | binary |
| train_fit | 45 | main_seller_state_ohe_PR | main_seller_state | binary |
| train_fit | 46 | main_seller_state_ohe_RJ | main_seller_state | binary |
| train_fit | 47 | main_seller_state_ohe_SC | main_seller_state | binary |
| train_fit | 48 | main_seller_state_ohe_RS | main_seller_state | binary |
| train_fit | 49 | main_seller_state_ohe_DF | main_seller_state | binary |
| train_fit | 50 | main_seller_state_ohe_BA | main_seller_state | binary |
| train_fit | 51 | main_seller_state_ohe_GO | main_seller_state | binary |
| train_fit | 52 | main_seller_state_ohe_MA | main_seller_state | binary |
| train_fit | 53 | main_seller_state_ohe_PE | main_seller_state | binary |
| train_fit | 54 | main_seller_state_ohe_ES | main_seller_state | binary |
| train_fit | 55 | main_seller_state_ohe_MT | main_seller_state | binary |
| train_fit | 56 | main_seller_state_ohe_CE | main_seller_state | binary |
| train_fit | 57 | main_seller_state_ohe_RN | main_seller_state | binary |
| train_fit | 58 | main_seller_state_ohe_MS | main_seller_state | binary |
| train_fit | 59 | main_seller_state_ohe_PB | main_seller_state | binary |
| train_fit | 60 | main_seller_state_ohe_RO | main_seller_state | binary |
| train_fit | 61 | main_seller_state_ohe_PI | main_seller_state | binary |
| train_fit | 62 | main_seller_state_ohe_PA | main_seller_state | binary |
| train_fit | 63 | main_seller_state_ohe_SE | main_seller_state | binary |
| train_fit | 64 | main_seller_state_ohe_AM | main_seller_state | binary |
| train_fit | 65 | main_seller_state_ohe___unknown | main_seller_state | binary |
| train_fit | 66 | main_category_ohe_bed_bath_table | main_category | binary |
| train_fit | 67 | main_category_ohe_health_beauty | main_category | binary |
| train_fit | 68 | main_category_ohe_sports_leisure | main_category | binary |
| train_fit | 69 | main_category_ohe_computers_accessories | main_category | binary |
| train_fit | 70 | main_category_ohe_furniture_decor | main_category | binary |
| train_fit | 71 | main_category_ohe_housewares | main_category | binary |
| train_fit | 72 | main_category_ohe_watches_gifts | main_category | binary |
| train_fit | 73 | main_category_ohe_telephony | main_category | binary |
| train_fit | 74 | main_category_ohe_auto | main_category | binary |
| train_fit | 75 | main_category_ohe_toys | main_category | binary |
| train_fit | 76 | main_category_ohe_cool_stuff | main_category | binary |
| train_fit | 77 | main_category_ohe_garden_tools | main_category | binary |
| train_fit | 78 | main_category_ohe_perfumery | main_category | binary |
| train_fit | 79 | main_category_ohe_baby | main_category | binary |
| train_fit | 80 | main_category_ohe_electronics | main_category | binary |
| train_fit | 81 | main_category_ohe_stationery | main_category | binary |
| train_fit | 82 | main_category_ohe_fashion_bags_accessories | main_category | binary |
| train_fit | 83 | main_category_ohe_pet_shop | main_category | binary |
| train_fit | 84 | main_category_ohe_khong_xac_dinh | main_category | binary |
| train_fit | 85 | main_category_ohe_office_furniture | main_category | binary |
| train_fit | 86 | main_category_ohe_luggage_accessories | main_category | binary |
| train_fit | 87 | main_category_ohe_consoles_games | main_category | binary |
| train_fit | 88 | main_category_ohe_construction_tools_construction | main_category | binary |
| train_fit | 89 | main_category_ohe_home_appliances | main_category | binary |
| train_fit | 90 | main_category_ohe_musical_instruments | main_category | binary |
| train_fit | 91 | main_category_ohe_small_appliances | main_category | binary |
| train_fit | 92 | main_category_ohe_books_general_interest | main_category | binary |
| train_fit | 93 | main_category_ohe_home_construction | main_category | binary |
| train_fit | 94 | main_category_ohe_food | main_category | binary |
| train_fit | 95 | main_category_ohe_furniture_living_room | main_category | binary |
| train_fit | 96 | main_category_ohe_audio | main_category | binary |
| train_fit | 97 | main_category_ohe_home_confort | main_category | binary |
| train_fit | 98 | main_category_ohe_market_place | main_category | binary |
| train_fit | 99 | main_category_ohe_drinks | main_category | binary |
| train_fit | 100 | main_category_ohe_books_technical | main_category | binary |
| train_fit | 101 | main_category_ohe_construction_tools_lights | main_category | binary |
| train_fit | 102 | main_category_ohe_air_conditioning | main_category | binary |
| train_fit | 103 | main_category_ohe_food_drink | main_category | binary |
| train_fit | 104 | main_category_ohe_home_appliances_2 | main_category | binary |
| train_fit | 105 | main_category_ohe_fashion_shoes | main_category | binary |
| train_fit | 106 | main_category_ohe_kitchen_dining_laundry_garden_furniture | main_category | binary |
| train_fit | 107 | main_category_ohe_industry_commerce_and_business | main_category | binary |
| train_fit | 108 | main_category_ohe_fixed_telephony | main_category | binary |
| train_fit | 109 | main_category_ohe_costruction_tools_garden | main_category | binary |
| train_fit | 110 | main_category_ohe_art | main_category | binary |
| train_fit | 111 | main_category_ohe_computers | main_category | binary |
| train_fit | 112 | main_category_ohe_agro_industry_and_commerce | main_category | binary |
| train_fit | 113 | main_category_ohe_construction_tools_safety | main_category | binary |
| train_fit | 114 | main_category_ohe_fashion_underwear_beach | main_category | binary |
| train_fit | 115 | main_category_ohe_signaling_and_security | main_category | binary |
| train_fit | 116 | main_category_ohe_christmas_supplies | main_category | binary |
| train_fit | 117 | main_category_ohe_fashion_male_clothing | main_category | binary |
| train_fit | 118 | main_category_ohe_costruction_tools_tools | main_category | binary |
| train_fit | 119 | main_category_ohe_furniture_bedroom | main_category | binary |
| train_fit | 120 | main_category_ohe_cine_photo | main_category | binary |
| train_fit | 121 | main_category_ohe_small_appliances_home_oven_and_coffee | main_category | binary |
| train_fit | 122 | main_category_ohe_tablets_printing_image | main_category | binary |
| train_fit | 123 | main_category_ohe_books_imported | main_category | binary |
| train_fit | 124 | main_category_ohe_dvds_blu_ray | main_category | binary |
| train_fit | 125 | main_category_ohe_music | main_category | binary |
| train_fit | 126 | main_category_ohe_party_supplies | main_category | binary |
| train_fit | 127 | main_category_ohe_furniture_mattress_and_upholstery | main_category | binary |
| train_fit | 128 | main_category_ohe_fashio_female_clothing | main_category | binary |
| train_fit | 129 | main_category_ohe_flowers | main_category | binary |
| train_fit | 130 | main_category_ohe_home_comfort_2 | main_category | binary |
| train_fit | 131 | main_category_ohe_fashion_sport | main_category | binary |
| train_fit | 132 | main_category_ohe_arts_and_craftmanship | main_category | binary |
| train_fit | 133 | main_category_ohe_diapers_and_hygiene | main_category | binary |
| train_fit | 134 | main_category_ohe_la_cuisine | main_category | binary |
| train_fit | 135 | main_category_ohe_portateis_cozinha_e_preparadores_de_alimentos | main_category | binary |
| train_fit | 136 | main_category_ohe_cds_dvds_musicals | main_category | binary |
| train_fit | 137 | main_category_ohe_fashion_childrens_clothes | main_category | binary |
| train_fit | 138 | main_category_ohe_pc_gamer | main_category | binary |
| train_fit | 139 | main_category_ohe_security_and_services | main_category | binary |
| train_fit | 140 | main_category_ohe___unknown | main_category | binary |
| train_full | 0 | item_count_filled | item_count | numeric |
| train_full | 1 | product_count_filled | product_count | numeric |
| train_full | 2 | seller_count_filled | seller_count | numeric |
| train_full | 3 | total_price_filled | total_price | numeric |
| train_full | 4 | total_freight_filled | total_freight | numeric |
| train_full | 5 | average_item_price_filled | average_item_price | numeric |
| train_full | 6 | total_weight_g_filled | total_weight_g | numeric |
| train_full | 7 | total_volume_cm3_filled | total_volume_cm3 | numeric |
| train_full | 8 | freight_ratio_filled | freight_ratio | numeric |
| train_full | 9 | purchase_year_filled | purchase_year | numeric |
| train_full | 10 | purchase_month_filled | purchase_month | numeric |
| train_full | 11 | purchase_day_of_week_filled | purchase_day_of_week | numeric |
| train_full | 12 | purchase_hour_filled | purchase_hour | numeric |
| train_full | 13 | estimated_delivery_days_filled | estimated_delivery_days | numeric |
| train_full | 14 | customer_seller_same_state_filled | customer_seller_same_state | numeric |
| train_full | 15 | customer_state_ohe_SP | customer_state | binary |
| train_full | 16 | customer_state_ohe_RJ | customer_state | binary |
| train_full | 17 | customer_state_ohe_MG | customer_state | binary |
| train_full | 18 | customer_state_ohe_RS | customer_state | binary |
| train_full | 19 | customer_state_ohe_PR | customer_state | binary |
| train_full | 20 | customer_state_ohe_SC | customer_state | binary |
| train_full | 21 | customer_state_ohe_BA | customer_state | binary |
| train_full | 22 | customer_state_ohe_DF | customer_state | binary |
| train_full | 23 | customer_state_ohe_ES | customer_state | binary |
| train_full | 24 | customer_state_ohe_GO | customer_state | binary |
| train_full | 25 | customer_state_ohe_PE | customer_state | binary |
| train_full | 26 | customer_state_ohe_CE | customer_state | binary |
| train_full | 27 | customer_state_ohe_PA | customer_state | binary |
| train_full | 28 | customer_state_ohe_MT | customer_state | binary |
| train_full | 29 | customer_state_ohe_MA | customer_state | binary |
| train_full | 30 | customer_state_ohe_MS | customer_state | binary |
| train_full | 31 | customer_state_ohe_PB | customer_state | binary |
| train_full | 32 | customer_state_ohe_PI | customer_state | binary |
| train_full | 33 | customer_state_ohe_RN | customer_state | binary |
| train_full | 34 | customer_state_ohe_AL | customer_state | binary |
| train_full | 35 | customer_state_ohe_SE | customer_state | binary |
| train_full | 36 | customer_state_ohe_TO | customer_state | binary |
| train_full | 37 | customer_state_ohe_RO | customer_state | binary |
| train_full | 38 | customer_state_ohe_AM | customer_state | binary |
| train_full | 39 | customer_state_ohe_AC | customer_state | binary |
| train_full | 40 | customer_state_ohe_AP | customer_state | binary |
| train_full | 41 | customer_state_ohe_RR | customer_state | binary |
| train_full | 42 | customer_state_ohe___unknown | customer_state | binary |
| train_full | 43 | main_seller_state_ohe_SP | main_seller_state | binary |
| train_full | 44 | main_seller_state_ohe_MG | main_seller_state | binary |
| train_full | 45 | main_seller_state_ohe_PR | main_seller_state | binary |
| train_full | 46 | main_seller_state_ohe_RJ | main_seller_state | binary |
| train_full | 47 | main_seller_state_ohe_SC | main_seller_state | binary |
| train_full | 48 | main_seller_state_ohe_RS | main_seller_state | binary |
| train_full | 49 | main_seller_state_ohe_DF | main_seller_state | binary |
| train_full | 50 | main_seller_state_ohe_BA | main_seller_state | binary |
| train_full | 51 | main_seller_state_ohe_GO | main_seller_state | binary |
| train_full | 52 | main_seller_state_ohe_PE | main_seller_state | binary |
| train_full | 53 | main_seller_state_ohe_MA | main_seller_state | binary |
| train_full | 54 | main_seller_state_ohe_ES | main_seller_state | binary |
| train_full | 55 | main_seller_state_ohe_MT | main_seller_state | binary |
| train_full | 56 | main_seller_state_ohe_CE | main_seller_state | binary |
| train_full | 57 | main_seller_state_ohe_MS | main_seller_state | binary |
| train_full | 58 | main_seller_state_ohe_RN | main_seller_state | binary |
| train_full | 59 | main_seller_state_ohe_PB | main_seller_state | binary |
| train_full | 60 | main_seller_state_ohe_RO | main_seller_state | binary |
| train_full | 61 | main_seller_state_ohe_PI | main_seller_state | binary |
| train_full | 62 | main_seller_state_ohe_PA | main_seller_state | binary |
| train_full | 63 | main_seller_state_ohe_SE | main_seller_state | binary |
| train_full | 64 | main_seller_state_ohe_AM | main_seller_state | binary |
| train_full | 65 | main_seller_state_ohe___unknown | main_seller_state | binary |
| train_full | 66 | main_category_ohe_bed_bath_table | main_category | binary |
| train_full | 67 | main_category_ohe_health_beauty | main_category | binary |
| train_full | 68 | main_category_ohe_sports_leisure | main_category | binary |
| train_full | 69 | main_category_ohe_computers_accessories | main_category | binary |
| train_full | 70 | main_category_ohe_furniture_decor | main_category | binary |
| train_full | 71 | main_category_ohe_housewares | main_category | binary |
| train_full | 72 | main_category_ohe_watches_gifts | main_category | binary |
| train_full | 73 | main_category_ohe_telephony | main_category | binary |
| train_full | 74 | main_category_ohe_auto | main_category | binary |
| train_full | 75 | main_category_ohe_toys | main_category | binary |
| train_full | 76 | main_category_ohe_cool_stuff | main_category | binary |
| train_full | 77 | main_category_ohe_garden_tools | main_category | binary |
| train_full | 78 | main_category_ohe_perfumery | main_category | binary |
| train_full | 79 | main_category_ohe_baby | main_category | binary |
| train_full | 80 | main_category_ohe_electronics | main_category | binary |
| train_full | 81 | main_category_ohe_stationery | main_category | binary |
| train_full | 82 | main_category_ohe_fashion_bags_accessories | main_category | binary |
| train_full | 83 | main_category_ohe_pet_shop | main_category | binary |
| train_full | 84 | main_category_ohe_khong_xac_dinh | main_category | binary |
| train_full | 85 | main_category_ohe_office_furniture | main_category | binary |
| train_full | 86 | main_category_ohe_luggage_accessories | main_category | binary |
| train_full | 87 | main_category_ohe_consoles_games | main_category | binary |
| train_full | 88 | main_category_ohe_home_appliances | main_category | binary |
| train_full | 89 | main_category_ohe_construction_tools_construction | main_category | binary |
| train_full | 90 | main_category_ohe_musical_instruments | main_category | binary |
| train_full | 91 | main_category_ohe_small_appliances | main_category | binary |
| train_full | 92 | main_category_ohe_books_general_interest | main_category | binary |
| train_full | 93 | main_category_ohe_home_construction | main_category | binary |
| train_full | 94 | main_category_ohe_food | main_category | binary |
| train_full | 95 | main_category_ohe_furniture_living_room | main_category | binary |
| train_full | 96 | main_category_ohe_home_confort | main_category | binary |
| train_full | 97 | main_category_ohe_audio | main_category | binary |
| train_full | 98 | main_category_ohe_drinks | main_category | binary |
| train_full | 99 | main_category_ohe_market_place | main_category | binary |
| train_full | 100 | main_category_ohe_books_technical | main_category | binary |
| train_full | 101 | main_category_ohe_air_conditioning | main_category | binary |
| train_full | 102 | main_category_ohe_kitchen_dining_laundry_garden_furniture | main_category | binary |
| train_full | 103 | main_category_ohe_construction_tools_lights | main_category | binary |
| train_full | 104 | main_category_ohe_fashion_shoes | main_category | binary |
| train_full | 105 | main_category_ohe_industry_commerce_and_business | main_category | binary |
| train_full | 106 | main_category_ohe_food_drink | main_category | binary |
| train_full | 107 | main_category_ohe_home_appliances_2 | main_category | binary |
| train_full | 108 | main_category_ohe_fixed_telephony | main_category | binary |
| train_full | 109 | main_category_ohe_costruction_tools_garden | main_category | binary |
| train_full | 110 | main_category_ohe_art | main_category | binary |
| train_full | 111 | main_category_ohe_computers | main_category | binary |
| train_full | 112 | main_category_ohe_agro_industry_and_commerce | main_category | binary |
| train_full | 113 | main_category_ohe_construction_tools_safety | main_category | binary |
| train_full | 114 | main_category_ohe_signaling_and_security | main_category | binary |
| train_full | 115 | main_category_ohe_fashion_underwear_beach | main_category | binary |
| train_full | 116 | main_category_ohe_christmas_supplies | main_category | binary |
| train_full | 117 | main_category_ohe_fashion_male_clothing | main_category | binary |
| train_full | 118 | main_category_ohe_costruction_tools_tools | main_category | binary |
| train_full | 119 | main_category_ohe_furniture_bedroom | main_category | binary |
| train_full | 120 | main_category_ohe_tablets_printing_image | main_category | binary |
| train_full | 121 | main_category_ohe_cine_photo | main_category | binary |
| train_full | 122 | main_category_ohe_small_appliances_home_oven_and_coffee | main_category | binary |
| train_full | 123 | main_category_ohe_dvds_blu_ray | main_category | binary |
| train_full | 124 | main_category_ohe_books_imported | main_category | binary |
| train_full | 125 | main_category_ohe_party_supplies | main_category | binary |
| train_full | 126 | main_category_ohe_furniture_mattress_and_upholstery | main_category | binary |
| train_full | 127 | main_category_ohe_music | main_category | binary |
| train_full | 128 | main_category_ohe_fashio_female_clothing | main_category | binary |
| train_full | 129 | main_category_ohe_flowers | main_category | binary |
| train_full | 130 | main_category_ohe_arts_and_craftmanship | main_category | binary |
| train_full | 131 | main_category_ohe_fashion_sport | main_category | binary |
| train_full | 132 | main_category_ohe_home_comfort_2 | main_category | binary |
| train_full | 133 | main_category_ohe_diapers_and_hygiene | main_category | binary |
| train_full | 134 | main_category_ohe_la_cuisine | main_category | binary |
| train_full | 135 | main_category_ohe_portateis_cozinha_e_preparadores_de_alimentos | main_category | binary |
| train_full | 136 | main_category_ohe_cds_dvds_musicals | main_category | binary |
| train_full | 137 | main_category_ohe_fashion_childrens_clothes | main_category | binary |
| train_full | 138 | main_category_ohe_pc_gamer | main_category | binary |
| train_full | 139 | main_category_ohe_security_and_services | main_category | binary |
| train_full | 140 | main_category_ohe___unknown | main_category | binary |

### Baseline majority class

Baseline không fit model và không dùng common threshold. Majority class được xác định từ `train_fit` khi quan sát validation và từ `train_full` khi đánh giá test; validation/test không tham gia quyết định label baseline.

| Phạm vi | Nguồn xác định majority class | Prediction cố định | Class counts nguồn | AUC |
| --- | --- | --- | --- | --- |
| validation | train_fit | 0 | {0: 57828, 1: 4226} | 0.500000 |
| test | train_full | 0 | {0: 72106, 1: 5273} | 0.500000 |

Với dataset hiện tại, majority class dùng cho test là `0` (`not late`). Vì baseline cho cùng một probability score cho mọi order nên AUC bằng 0.5; đây là mức tham chiếu xếp hạng ngẫu nhiên, không phải common threshold.

## PHẦN 6. LOGISTIC REGRESSION

Cấu hình: `maxIter=50`, `regParam=0.01`. Intercept final: `-366.6827995874858175`.

Công thức: `z = intercept + Σ(coefficient_j × transformed_feature_j)` và `probability_manual = 1 / (1 + exp(-z))`.

| Index | Transformed feature | Coefficient | Intercept |
| --- | --- | --- | --- |
| 0 | item_count_filled | -0.0728159701318056 | -366.6827995874858175 |
| 1 | product_count_filled | -0.2436242860374153 | -366.6827995874858175 |
| 2 | seller_count_filled | -0.8792444875416756 | -366.6827995874858175 |
| 3 | total_price_filled | -0.0000509402828405 | -366.6827995874858175 |
| 4 | total_freight_filled | 0.0002663690733723 | -366.6827995874858175 |
| 5 | average_item_price_filled | 0.0003734099167293 | -366.6827995874858175 |
| 6 | total_weight_g_filled | 0.0000119625389294 | -366.6827995874858175 |
| 7 | total_volume_cm3_filled | 0.0000012205708204 | -366.6827995874858175 |
| 8 | freight_ratio_filled | -0.0290712534134062 | -366.6827995874858175 |
| 9 | purchase_year_filled | 0.1816513880658075 | -366.6827995874858175 |
| 10 | purchase_month_filled | -0.0370578137239926 | -366.6827995874858175 |
| 11 | purchase_day_of_week_filled | 0.0127842712277946 | -366.6827995874858175 |
| 12 | purchase_hour_filled | 0.0057187616629516 | -366.6827995874858175 |
| 13 | estimated_delivery_days_filled | -0.0492311382182447 | -366.6827995874858175 |
| 14 | customer_seller_same_state_filled | -0.5807172667005529 | -366.6827995874858175 |
| 15 | customer_state_ohe_SP | -0.3735134168689600 | -366.6827995874858175 |
| 16 | customer_state_ohe_RJ | 0.5710734116339560 | -366.6827995874858175 |
| 17 | customer_state_ohe_MG | -0.3995234276708031 | -366.6827995874858175 |
| 18 | customer_state_ohe_RS | -0.0046755299331931 | -366.6827995874858175 |
| 19 | customer_state_ohe_PR | -0.5005240076942024 | -366.6827995874858175 |
| 20 | customer_state_ohe_SC | 0.1728781728044807 | -366.6827995874858175 |
| 21 | customer_state_ohe_BA | 0.7336239201067468 | -366.6827995874858175 |
| 22 | customer_state_ohe_DF | -0.2765665461519764 | -366.6827995874858175 |
| 23 | customer_state_ohe_ES | 0.3688767539160139 | -366.6827995874858175 |
| 24 | customer_state_ohe_GO | 0.0005842233254661 | -366.6827995874858175 |
| 25 | customer_state_ohe_PE | 0.5254016663786383 | -366.6827995874858175 |
| 26 | customer_state_ohe_CE | 0.9579962028314398 | -366.6827995874858175 |
| 27 | customer_state_ohe_PA | 0.8909918841477619 | -366.6827995874858175 |
| 28 | customer_state_ohe_MT | 0.0746552877791018 | -366.6827995874858175 |
| 29 | customer_state_ohe_MA | 1.1577666353520508 | -366.6827995874858175 |
| 30 | customer_state_ohe_MS | 0.2733674811605025 | -366.6827995874858175 |
| 31 | customer_state_ohe_PB | 0.6496602429997137 | -366.6827995874858175 |
| 32 | customer_state_ohe_PI | 0.7964059696512064 | -366.6827995874858175 |
| 33 | customer_state_ohe_RN | 0.5624528942309469 | -366.6827995874858175 |
| 34 | customer_state_ohe_AL | 1.4430432722591877 | -366.6827995874858175 |
| 35 | customer_state_ohe_SE | 0.9767258484063803 | -366.6827995874858175 |
| 36 | customer_state_ohe_TO | 0.4879499318421651 | -366.6827995874858175 |
| 37 | customer_state_ohe_RO | -0.1110653306292368 | -366.6827995874858175 |
| 38 | customer_state_ohe_AM | -0.1601611942098955 | -366.6827995874858175 |
| 39 | customer_state_ohe_AC | -0.1183202561430085 | -366.6827995874858175 |
| 40 | customer_state_ohe_AP | 0.1998021884194558 | -366.6827995874858175 |
| 41 | customer_state_ohe_RR | 1.1989196754423321 | -366.6827995874858175 |
| 42 | customer_state_ohe___unknown | 0.0000000000000000 | -366.6827995874858175 |
| 43 | main_seller_state_ohe_SP | 0.2038061232285540 | -366.6827995874858175 |
| 44 | main_seller_state_ohe_MG | -0.2283490362824515 | -366.6827995874858175 |
| 45 | main_seller_state_ohe_PR | -0.0898308659238170 | -366.6827995874858175 |
| 46 | main_seller_state_ohe_RJ | 0.0166892772679467 | -366.6827995874858175 |
| 47 | main_seller_state_ohe_SC | -0.1321573984085392 | -366.6827995874858175 |
| 48 | main_seller_state_ohe_RS | -0.4007651159531262 | -366.6827995874858175 |
| 49 | main_seller_state_ohe_DF | -0.1671148457326668 | -366.6827995874858175 |
| 50 | main_seller_state_ohe_BA | -0.3842772526409082 | -366.6827995874858175 |
| 51 | main_seller_state_ohe_GO | -0.6006652221394472 | -366.6827995874858175 |
| 52 | main_seller_state_ohe_PE | -0.7052045730053843 | -366.6827995874858175 |
| 53 | main_seller_state_ohe_MA | 1.1476019106146522 | -366.6827995874858175 |
| 54 | main_seller_state_ohe_ES | -0.0653681872095911 | -366.6827995874858175 |
| 55 | main_seller_state_ohe_MT | -0.2850513138536655 | -366.6827995874858175 |
| 56 | main_seller_state_ohe_CE | -0.9298383887509989 | -366.6827995874858175 |
| 57 | main_seller_state_ohe_MS | -0.1893730001138626 | -366.6827995874858175 |
| 58 | main_seller_state_ohe_RN | 0.3275715222775277 | -366.6827995874858175 |
| 59 | main_seller_state_ohe_PB | -0.7638438459307757 | -366.6827995874858175 |
| 60 | main_seller_state_ohe_RO | -1.2318708383223536 | -366.6827995874858175 |
| 61 | main_seller_state_ohe_PI | -1.8995231792136000 | -366.6827995874858175 |
| 62 | main_seller_state_ohe_PA | -1.2617977005650489 | -366.6827995874858175 |
| 63 | main_seller_state_ohe_SE | -1.4288165054268738 | -366.6827995874858175 |
| 64 | main_seller_state_ohe_AM | 2.7811139752492888 | -366.6827995874858175 |
| 65 | main_seller_state_ohe___unknown | 0.0000000000000000 | -366.6827995874858175 |
| 66 | main_category_ohe_bed_bath_table | 0.1919777635412221 | -366.6827995874858175 |
| 67 | main_category_ohe_health_beauty | -0.0038540879090342 | -366.6827995874858175 |
| 68 | main_category_ohe_sports_leisure | 0.0053152608607145 | -366.6827995874858175 |
| 69 | main_category_ohe_computers_accessories | 0.0754051437736786 | -366.6827995874858175 |
| 70 | main_category_ohe_furniture_decor | 0.1143279574911856 | -366.6827995874858175 |
| 71 | main_category_ohe_housewares | -0.1795997184326425 | -366.6827995874858175 |
| 72 | main_category_ohe_watches_gifts | -0.0171809173659974 | -366.6827995874858175 |
| 73 | main_category_ohe_telephony | 0.0091219442711533 | -366.6827995874858175 |
| 74 | main_category_ohe_auto | 0.0152381941384413 | -366.6827995874858175 |
| 75 | main_category_ohe_toys | 0.0061277203308880 | -366.6827995874858175 |
| 76 | main_category_ohe_cool_stuff | -0.1857455917756038 | -366.6827995874858175 |
| 77 | main_category_ohe_garden_tools | 0.0464693086719049 | -366.6827995874858175 |
| 78 | main_category_ohe_perfumery | 0.0054305158902073 | -366.6827995874858175 |
| 79 | main_category_ohe_baby | 0.1917838903228999 | -366.6827995874858175 |
| 80 | main_category_ohe_electronics | 0.0059891877801082 | -366.6827995874858175 |
| 81 | main_category_ohe_stationery | -0.0094684679252110 | -366.6827995874858175 |
| 82 | main_category_ohe_fashion_bags_accessories | -0.1019402056305955 | -366.6827995874858175 |
| 83 | main_category_ohe_pet_shop | -0.1262163176897347 | -366.6827995874858175 |
| 84 | main_category_ohe_khong_xac_dinh | 0.0146682786535751 | -366.6827995874858175 |
| 85 | main_category_ohe_office_furniture | 0.1380053542030373 | -366.6827995874858175 |
| 86 | main_category_ohe_luggage_accessories | -0.4733289787591641 | -366.6827995874858175 |
| 87 | main_category_ohe_consoles_games | 0.0700322197325316 | -366.6827995874858175 |
| 88 | main_category_ohe_home_appliances | -0.3554197970818386 | -366.6827995874858175 |
| 89 | main_category_ohe_construction_tools_construction | -0.1525935467815309 | -366.6827995874858175 |
| 90 | main_category_ohe_musical_instruments | 0.0004703944811955 | -366.6827995874858175 |
| 91 | main_category_ohe_small_appliances | -0.3039814303327135 | -366.6827995874858175 |
| 92 | main_category_ohe_books_general_interest | 0.0470671686565745 | -366.6827995874858175 |
| 93 | main_category_ohe_home_construction | -0.1547365704029784 | -366.6827995874858175 |
| 94 | main_category_ohe_food | 0.0250649392258544 | -366.6827995874858175 |
| 95 | main_category_ohe_furniture_living_room | -0.0052232456368695 | -366.6827995874858175 |
| 96 | main_category_ohe_home_confort | 0.3631657125181724 | -366.6827995874858175 |
| 97 | main_category_ohe_audio | 0.4635011957442255 | -366.6827995874858175 |
| 98 | main_category_ohe_drinks | -0.2245917061644129 | -366.6827995874858175 |
| 99 | main_category_ohe_market_place | -0.3729986118824336 | -366.6827995874858175 |
| 100 | main_category_ohe_books_technical | 0.0343983443692313 | -366.6827995874858175 |
| 101 | main_category_ohe_air_conditioning | -0.3780467337176142 | -366.6827995874858175 |
| 102 | main_category_ohe_kitchen_dining_laundry_garden_furniture | -0.1893347304680681 | -366.6827995874858175 |
| 103 | main_category_ohe_construction_tools_lights | -0.0489625464743917 | -366.6827995874858175 |
| 104 | main_category_ohe_fashion_shoes | 0.0032815422775955 | -366.6827995874858175 |
| 105 | main_category_ohe_industry_commerce_and_business | -0.2178444866893242 | -366.6827995874858175 |
| 106 | main_category_ohe_food_drink | -0.5132183000405866 | -366.6827995874858175 |
| 107 | main_category_ohe_home_appliances_2 | -0.0731218979823773 | -366.6827995874858175 |
| 108 | main_category_ohe_fixed_telephony | -0.0498991076370915 | -366.6827995874858175 |
| 109 | main_category_ohe_costruction_tools_garden | -0.2824424046085227 | -366.6827995874858175 |
| 110 | main_category_ohe_art | -0.0418533202434145 | -366.6827995874858175 |
| 111 | main_category_ohe_computers | -0.4568374689175472 | -366.6827995874858175 |
| 112 | main_category_ohe_agro_industry_and_commerce | -0.5062215652761134 | -366.6827995874858175 |
| 113 | main_category_ohe_construction_tools_safety | -0.6109845259505168 | -366.6827995874858175 |
| 114 | main_category_ohe_signaling_and_security | -0.2758954715570681 | -366.6827995874858175 |
| 115 | main_category_ohe_fashion_underwear_beach | 0.3436632165595292 | -366.6827995874858175 |
| 116 | main_category_ohe_christmas_supplies | 0.4498624901761306 | -366.6827995874858175 |
| 117 | main_category_ohe_fashion_male_clothing | 0.1477957160989871 | -366.6827995874858175 |
| 118 | main_category_ohe_costruction_tools_tools | -0.3924865611108169 | -366.6827995874858175 |
| 119 | main_category_ohe_furniture_bedroom | 0.1275015386449367 | -366.6827995874858175 |
| 120 | main_category_ohe_tablets_printing_image | -0.3173478981015334 | -366.6827995874858175 |
| 121 | main_category_ohe_cine_photo | -0.1625014246435127 | -366.6827995874858175 |
| 122 | main_category_ohe_small_appliances_home_oven_and_coffee | -0.2648276914046671 | -366.6827995874858175 |
| 123 | main_category_ohe_dvds_blu_ray | 0.7919334872519282 | -366.6827995874858175 |
| 124 | main_category_ohe_books_imported | -0.2884731083570147 | -366.6827995874858175 |
| 125 | main_category_ohe_party_supplies | -1.5290826410502636 | -366.6827995874858175 |
| 126 | main_category_ohe_furniture_mattress_and_upholstery | 0.6089639191508253 | -366.6827995874858175 |
| 127 | main_category_ohe_music | -0.0438233820069232 | -366.6827995874858175 |
| 128 | main_category_ohe_fashio_female_clothing | 0.3971552112574074 | -366.6827995874858175 |
| 129 | main_category_ohe_flowers | -1.6824901515974289 | -366.6827995874858175 |
| 130 | main_category_ohe_arts_and_craftmanship | -0.2414464584770298 | -366.6827995874858175 |
| 131 | main_category_ohe_fashion_sport | -0.2759789538614862 | -366.6827995874858175 |
| 132 | main_category_ohe_home_comfort_2 | 1.0106743966841307 | -366.6827995874858175 |
| 133 | main_category_ohe_diapers_and_hygiene | -1.4629642118311532 | -366.6827995874858175 |
| 134 | main_category_ohe_la_cuisine | -1.3780873155375848 | -366.6827995874858175 |
| 135 | main_category_ohe_portateis_cozinha_e_preparadores_de_alimentos | 0.3059515789444776 | -366.6827995874858175 |
| 136 | main_category_ohe_cds_dvds_musicals | -1.4610350472914715 | -366.6827995874858175 |
| 137 | main_category_ohe_fashion_childrens_clothes | -1.5501957162924078 | -366.6827995874858175 |
| 138 | main_category_ohe_pc_gamer | -1.7459591385856210 | -366.6827995874858175 |
| 139 | main_category_ohe_security_and_services | -1.2909384831230626 | -366.6827995874858175 |
| 140 | main_category_ohe___unknown | 0.0000000000000000 | -366.6827995874858175 |

### Order A: `f46b842d9b4dfd29acf5eec998837ede`

| Index | Transformed feature | Value | Coefficient | Contribution |
| --- | --- | --- | --- | --- |
| 0 | item_count_filled | 1.0000000000000000 | -0.0728159701318056 | -0.0728159701318056 |
| 1 | product_count_filled | 1.0000000000000000 | -0.2436242860374153 | -0.2436242860374153 |
| 2 | seller_count_filled | 1.0000000000000000 | -0.8792444875416756 | -0.8792444875416756 |
| 3 | total_price_filled | 122.9899999999999949 | -0.0000509402828405 | -0.0062651453865587 |
| 4 | total_freight_filled | 17.0500000000000007 | 0.0002663690733723 | 0.0045415927009970 |
| 5 | average_item_price_filled | 122.9899999999999949 | 0.0003734099167293 | 0.0459256856585401 |
| 6 | total_weight_g_filled | 700.0000000000000000 | 0.0000119625389294 | 0.0083737772505648 |
| 7 | total_volume_cm3_filled | 5700.0000000000000000 | 0.0000012205708204 | 0.0069572536764329 |
| 8 | freight_ratio_filled | 0.1386000000000000 | -0.0290712534134062 | -0.0040292757230981 |
| 9 | purchase_year_filled | 2018.0000000000000000 | 0.1816513880658075 | 366.5725011167994580 |
| 10 | purchase_month_filled | 7.0000000000000000 | -0.0370578137239926 | -0.2594046960679481 |
| 11 | purchase_day_of_week_filled | 5.0000000000000000 | 0.0127842712277946 | 0.0639213561389728 |
| 12 | purchase_hour_filled | 22.0000000000000000 | 0.0057187616629516 | 0.1258127565849359 |
| 13 | estimated_delivery_days_filled | 21.0000000000000000 | -0.0492311382182447 | -1.0338539025831397 |
| 32 | customer_state_ohe_PI | 1.0000000000000000 | 0.7964059696512064 | 0.7964059696512064 |
| 53 | main_seller_state_ohe_MA | 1.0000000000000000 | 1.1476019106146522 | 1.1476019106146522 |
| 67 | main_category_ohe_health_beauty | 1.0000000000000000 | -0.0038540879090342 | -0.0038540879090342 |

Phép cộng đầy đủ:

```text
z = -366.68279958748582
  + (1 × -0.072815970131805643) [item_count_filled] = -0.072815970131805643
  + (1 × -0.24362428603741526) [product_count_filled] = -0.24362428603741526
  + (1 × -0.87924448754167561) [seller_count_filled] = -0.87924448754167561
  + (122.98999999999999 × -5.0940282840545632e-05) [total_price_filled] = -0.0062651453865587067
  + (17.050000000000001 × 0.00026636907337226049) [total_freight_filled] = 0.0045415927009970413
  + (122.98999999999999 × 0.00037340991672932808) [average_item_price_filled] = 0.045925685658540057
  + (700 × 1.1962538929378245e-05) [total_weight_g_filled] = 0.008373777250564771
  + (5700 × 1.2205708204268208e-06) [total_volume_cm3_filled] = 0.0069572536764328785
  + (0.1386 × -0.029071253413406172) [freight_ratio_filled] = -0.0040292757230980959
  + (2018 × 0.18165138806580747) [purchase_year_filled] = 366.57250111679946
  + (7 × -0.037057813723992583) [purchase_month_filled] = -0.25940469606794808
  + (5 × 0.012784271227794557) [purchase_day_of_week_filled] = 0.06392135613897279
  + (22 × 0.0057187616629516321) [purchase_hour_filled] = 0.1258127565849359
  + (21 × -0.049231138218244742) [estimated_delivery_days_filled] = -1.0338539025831397
  + (1 × 0.79640596965120636) [customer_state_ohe_PI] = 0.79640596965120636
  + (1 × 1.1476019106146522) [main_seller_state_ohe_MA] = 1.1476019106146522
  + (1 × -0.0038540879090342112) [main_category_ohe_health_beauty] = -0.0038540879090342112
  = -0.41385001979074332
probability_manual = 1 / (1 + exp(-z)) = 0.39798931891486933
probability_Spark = 0.39798931891486933
absolute_difference = 0
```

### Order B: `686c0ba20be3837a5041edbc39d3f9ae`

| Index | Transformed feature | Value | Coefficient | Contribution |
| --- | --- | --- | --- | --- |
| 0 | item_count_filled | 1.0000000000000000 | -0.0728159701318056 | -0.0728159701318056 |
| 1 | product_count_filled | 1.0000000000000000 | -0.2436242860374153 | -0.2436242860374153 |
| 2 | seller_count_filled | 1.0000000000000000 | -0.8792444875416756 | -0.8792444875416756 |
| 3 | total_price_filled | 114.0000000000000000 | -0.0000509402828405 | -0.0058071922438222 |
| 4 | total_freight_filled | 21.2500000000000000 | 0.0002663690733723 | 0.0056603428091605 |
| 5 | average_item_price_filled | 114.0000000000000000 | 0.0003734099167293 | 0.0425687305071434 |
| 6 | total_weight_g_filled | 350.0000000000000000 | 0.0000119625389294 | 0.0041868886252824 |
| 7 | total_volume_cm3_filled | 2040.0000000000000000 | 0.0000012205708204 | 0.0024899644736707 |
| 8 | freight_ratio_filled | 0.1864000000000000 | -0.0290712534134062 | -0.0054188816362589 |
| 9 | purchase_year_filled | 2017.0000000000000000 | 0.1816513880658075 | 366.3908497287336559 |
| 10 | purchase_month_filled | 3.0000000000000000 | -0.0370578137239926 | -0.1111734411719777 |
| 11 | purchase_day_of_week_filled | 1.0000000000000000 | 0.0127842712277946 | 0.0127842712277946 |
| 12 | purchase_hour_filled | 20.0000000000000000 | 0.0057187616629516 | 0.1143752332590326 |
| 13 | estimated_delivery_days_filled | 30.0000000000000000 | -0.0492311382182447 | -1.4769341465473422 |
| 29 | customer_state_ohe_MA | 1.0000000000000000 | 1.1577666353520508 | 1.1577666353520508 |
| 64 | main_seller_state_ohe_AM | 1.0000000000000000 | 2.7811139752492888 | 2.7811139752492888 |
| 73 | main_category_ohe_telephony | 1.0000000000000000 | 0.0091219442711533 | 0.0091219442711533 |

Phép cộng đầy đủ:

```text
z = -366.68279958748582
  + (1 × -0.072815970131805643) [item_count_filled] = -0.072815970131805643
  + (1 × -0.24362428603741526) [product_count_filled] = -0.24362428603741526
  + (1 × -0.87924448754167561) [seller_count_filled] = -0.87924448754167561
  + (114 × -5.0940282840545632e-05) [total_price_filled] = -0.0058071922438222021
  + (21.25 × 0.00026636907337226049) [total_freight_filled] = 0.0056603428091605351
  + (114 × 0.00037340991672932808) [average_item_price_filled] = 0.042568730507143403
  + (350 × 1.1962538929378245e-05) [total_weight_g_filled] = 0.0041868886252823855
  + (2040 × 1.2205708204268208e-06) [total_volume_cm3_filled] = 0.0024899644736707145
  + (0.18640000000000001 × -0.029071253413406172) [freight_ratio_filled] = -0.0054188816362589105
  + (2017 × 0.18165138806580747) [purchase_year_filled] = 366.39084972873366
  + (3 × -0.037057813723992583) [purchase_month_filled] = -0.11117344117197775
  + (1 × 0.012784271227794557) [purchase_day_of_week_filled] = 0.012784271227794557
  + (20 × 0.0057187616629516321) [purchase_hour_filled] = 0.11437523325903265
  + (30 × -0.049231138218244742) [estimated_delivery_days_filled] = -1.4769341465473422
  + (1 × 1.1577666353520508) [customer_state_ohe_MA] = 1.1577666353520508
  + (1 × 2.7811139752492888) [main_seller_state_ohe_AM] = 2.7811139752492888
  + (1 × 0.0091219442711533115) [main_category_ohe_telephony] = 0.0091219442711533115
  = 1.0430997217121103
probability_manual = 1 / (1 + exp(-z)) = 0.73944765651953459
probability_Spark = 0.73944765651954547
absolute_difference = 1.0880185641326534e-14
```

### Order C: `9b1d71b20edcf15ab15e0bb4a932f23f`

| Index | Transformed feature | Value | Coefficient | Contribution |
| --- | --- | --- | --- | --- |
| 0 | item_count_filled | 1.0000000000000000 | -0.0728159701318056 | -0.0728159701318056 |
| 1 | product_count_filled | 1.0000000000000000 | -0.2436242860374153 | -0.2436242860374153 |
| 2 | seller_count_filled | 1.0000000000000000 | -0.8792444875416756 | -0.8792444875416756 |
| 3 | total_price_filled | 79.0000000000000000 | -0.0000509402828405 | -0.0040242823444031 |
| 4 | total_freight_filled | 16.3099999999999987 | 0.0002663690733723 | 0.0043444795867016 |
| 5 | average_item_price_filled | 79.0000000000000000 | 0.0003734099167293 | 0.0294993834216169 |
| 6 | total_weight_g_filled | 675.0000000000000000 | 0.0000119625389294 | 0.0080747137773303 |
| 7 | total_volume_cm3_filled | 3168.0000000000000000 | 0.0000012205708204 | 0.0038667683591122 |
| 8 | freight_ratio_filled | 0.2065000000000000 | -0.0290712534134062 | -0.0060032138298684 |
| 9 | purchase_year_filled | 2017.0000000000000000 | 0.1816513880658075 | 366.3908497287336559 |
| 10 | purchase_month_filled | 9.0000000000000000 | -0.0370578137239926 | -0.3335203235159332 |
| 11 | purchase_day_of_week_filled | 1.0000000000000000 | 0.0127842712277946 | 0.0127842712277946 |
| 12 | purchase_hour_filled | 9.0000000000000000 | 0.0057187616629516 | 0.0514688549665647 |
| 13 | estimated_delivery_days_filled | 19.0000000000000000 | -0.0492311382182447 | -0.9353916261466501 |
| 22 | customer_state_ohe_DF | 1.0000000000000000 | -0.2765665461519764 | -0.2765665461519764 |
| 43 | main_seller_state_ohe_SP | 1.0000000000000000 | 0.2038061232285540 | 0.2038061232285540 |
| 97 | main_category_ohe_audio | 1.0000000000000000 | 0.4635011957442255 | 0.4635011957442255 |

Phép cộng đầy đủ:

```text
z = -366.68279958748582
  + (1 × -0.072815970131805643) [item_count_filled] = -0.072815970131805643
  + (1 × -0.24362428603741526) [product_count_filled] = -0.24362428603741526
  + (1 × -0.87924448754167561) [seller_count_filled] = -0.87924448754167561
  + (79 × -5.0940282840545632e-05) [total_price_filled] = -0.0040242823444031047
  + (16.309999999999999 × 0.00026636907337226049) [total_freight_filled] = 0.0043444795867015685
  + (79 × 0.00037340991672932808) [average_item_price_filled] = 0.029499383421616919
  + (675 × 1.1962538929378245e-05) [total_weight_g_filled] = 0.0080747137773303154
  + (3168 × 1.2205708204268208e-06) [total_volume_cm3_filled] = 0.0038667683591121684
  + (0.20649999999999999 × -0.029071253413406172) [freight_ratio_filled] = -0.006003213829868374
  + (2017 × 0.18165138806580747) [purchase_year_filled] = 366.39084972873366
  + (9 × -0.037057813723992583) [purchase_month_filled] = -0.33352032351593325
  + (1 × 0.012784271227794557) [purchase_day_of_week_filled] = 0.012784271227794557
  + (9 × 0.0057187616629516321) [purchase_hour_filled] = 0.051468854966564691
  + (19 × -0.049231138218244742) [estimated_delivery_days_filled] = -0.93539162614665006
  + (1 × -0.2765665461519764) [customer_state_ohe_DF] = -0.2765665461519764
  + (1 × 0.20380612322855396) [main_seller_state_ohe_SP] = 0.20380612322855396
  + (1 × 0.46350119574422549) [main_category_ohe_audio] = 0.46350119574422549
  = -2.2657948041400005
probability_manual = 1 / (1 + exp(-z)) = 0.093995718010235374
probability_Spark = 0.093995718010240203
absolute_difference = 4.829470157119431e-15
```

### Order D: `c2bb89b5c1dd978d507284be78a04cb2`

| Index | Transformed feature | Value | Coefficient | Contribution |
| --- | --- | --- | --- | --- |
| 0 | item_count_filled | 2.0000000000000000 | -0.0728159701318056 | -0.1456319402636113 |
| 1 | product_count_filled | 1.0000000000000000 | -0.2436242860374153 | -0.2436242860374153 |
| 2 | seller_count_filled | 1.0000000000000000 | -0.8792444875416756 | -0.8792444875416756 |
| 3 | total_price_filled | 199.9799999999999898 | -0.0000509402828405 | -0.0101870377624523 |
| 4 | total_freight_filled | 122.8799999999999955 | 0.0002663690733723 | 0.0327314317359834 |
| 5 | average_item_price_filled | 99.9899999999999949 | 0.0003734099167293 | 0.0373372575737655 |
| 6 | total_weight_g_filled | 30000.0000000000000000 | 0.0000119625389294 | 0.3588761678813474 |
| 7 | total_volume_cm3_filled | 52500.0000000000000000 | 0.0000012205708204 | 0.0640799680724081 |
| 8 | freight_ratio_filled | 0.6145000000000000 | -0.0290712534134062 | -0.0178642852225381 |
| 9 | purchase_year_filled | 2017.0000000000000000 | 0.1816513880658075 | 366.3908497287336559 |
| 10 | purchase_month_filled | 5.0000000000000000 | -0.0370578137239926 | -0.1852890686199629 |
| 11 | purchase_day_of_week_filled | 3.0000000000000000 | 0.0127842712277946 | 0.0383528136833837 |
| 12 | purchase_hour_filled | 22.0000000000000000 | 0.0057187616629516 | 0.1258127565849359 |
| 13 | estimated_delivery_days_filled | 141.0000000000000000 | -0.0492311382182447 | -6.9415904887725084 |
| 15 | customer_state_ohe_SP | 1.0000000000000000 | -0.3735134168689600 | -0.3735134168689600 |
| 44 | main_seller_state_ohe_MG | 1.0000000000000000 | -0.2283490362824515 | -0.2283490362824515 |
| 71 | main_category_ohe_housewares | 1.0000000000000000 | -0.1795997184326425 | -0.1795997184326425 |

Phép cộng đầy đủ:

```text
z = -366.68279958748582
  + (2 × -0.072815970131805643) [item_count_filled] = -0.14563194026361129
  + (1 × -0.24362428603741526) [product_count_filled] = -0.24362428603741526
  + (1 × -0.87924448754167561) [seller_count_filled] = -0.87924448754167561
  + (199.97999999999999 × -5.0940282840545632e-05) [total_price_filled] = -0.010187037762452314
  + (122.88 × 0.00026636907337226049) [total_freight_filled] = 0.03273143173598337
  + (99.989999999999995 × 0.00037340991672932808) [average_item_price_filled] = 0.037337257573765513
  + (30000 × 1.1962538929378245e-05) [total_weight_g_filled] = 0.35887616788134735
  + (52500 × 1.2205708204268208e-06) [total_volume_cm3_filled] = 0.064079968072408094
  + (0.61450000000000005 × -0.029071253413406172) [freight_ratio_filled] = -0.017864285222538093
  + (2017 × 0.18165138806580747) [purchase_year_filled] = 366.39084972873366
  + (5 × -0.037057813723992583) [purchase_month_filled] = -0.18528906861996292
  + (3 × 0.012784271227794557) [purchase_day_of_week_filled] = 0.038352813683383669
  + (22 × 0.0057187616629516321) [purchase_hour_filled] = 0.1258127565849359
  + (141 × -0.049231138218244742) [estimated_delivery_days_filled] = -6.9415904887725084
  + (1 × -0.37351341686896) [customer_state_ohe_SP] = -0.37351341686896
  + (1 × -0.22834903628245151) [main_seller_state_ohe_MG] = -0.22834903628245151
  + (1 × -0.17959971843264252) [main_category_ohe_housewares] = -0.17959971843264252
  = -8.8396532290245773
probability_manual = 1 / (1 + exp(-z)) = 0.0001448519905773873
probability_Spark = 0.00014485199057734377
absolute_difference = 4.3530717225293003e-17
```

## PHẦN 7. RANDOM FOREST

Cấu hình final: số decision tree `30`, `maxDepth=6`, `seed=42`.

### Feature importance

| Index | Transformed feature | Feature importance |
| --- | --- | --- |
| 0 | item_count_filled | 0.0050646227353857 |
| 1 | product_count_filled | 0.0042832371586910 |
| 2 | seller_count_filled | 0.0029538074213928 |
| 3 | total_price_filled | 0.0144250244560203 |
| 4 | total_freight_filled | 0.0365693509460958 |
| 5 | average_item_price_filled | 0.0202540421360239 |
| 6 | total_weight_g_filled | 0.0118126360058513 |
| 7 | total_volume_cm3_filled | 0.0111023636917574 |
| 8 | freight_ratio_filled | 0.0141410175000053 |
| 9 | purchase_year_filled | 0.0349696895504907 |
| 10 | purchase_month_filled | 0.2424953863622225 |
| 11 | purchase_day_of_week_filled | 0.0094720232646159 |
| 12 | purchase_hour_filled | 0.0126290690835150 |
| 13 | estimated_delivery_days_filled | 0.0839576849239208 |
| 14 | customer_seller_same_state_filled | 0.0507789644621848 |
| 15 | customer_state_ohe_SP | 0.0519358056797792 |
| 16 | customer_state_ohe_RJ | 0.1041753475693115 |
| 17 | customer_state_ohe_MG | 0.0116162552859363 |
| 18 | customer_state_ohe_RS | 0.0000000000000000 |
| 19 | customer_state_ohe_PR | 0.0072089869843339 |
| 20 | customer_state_ohe_SC | 0.0007217266206324 |
| 21 | customer_state_ohe_BA | 0.0188825653687203 |
| 22 | customer_state_ohe_DF | 0.0003863536771469 |
| 23 | customer_state_ohe_ES | 0.0115007588743669 |
| 24 | customer_state_ohe_GO | 0.0003376058479920 |
| 25 | customer_state_ohe_PE | 0.0064031201050797 |
| 26 | customer_state_ohe_CE | 0.0262255158503718 |
| 27 | customer_state_ohe_PA | 0.0016667878980710 |
| 28 | customer_state_ohe_MT | 0.0000176768573516 |
| 29 | customer_state_ohe_MA | 0.0256577662371642 |
| 30 | customer_state_ohe_MS | 0.0000000000000000 |
| 31 | customer_state_ohe_PB | 0.0008432599369483 |
| 32 | customer_state_ohe_PI | 0.0006468208159073 |
| 33 | customer_state_ohe_RN | 0.0000000000000000 |
| 34 | customer_state_ohe_AL | 0.0158427291134006 |
| 35 | customer_state_ohe_SE | 0.0025835444199198 |
| 36 | customer_state_ohe_TO | 0.0000000000000000 |
| 37 | customer_state_ohe_RO | 0.0000000000000000 |
| 38 | customer_state_ohe_AM | 0.0000000000000000 |
| 39 | customer_state_ohe_AC | 0.0000000000000000 |
| 40 | customer_state_ohe_AP | 0.0000000000000000 |
| 41 | customer_state_ohe_RR | 0.0005347390386801 |
| 42 | customer_state_ohe___unknown | 0.0000000000000000 |
| 43 | main_seller_state_ohe_SP | 0.0279956793040533 |
| 44 | main_seller_state_ohe_MG | 0.0044558282202477 |
| 45 | main_seller_state_ohe_PR | 0.0061190062919849 |
| 46 | main_seller_state_ohe_RJ | 0.0054175550241733 |
| 47 | main_seller_state_ohe_SC | 0.0012212614797786 |
| 48 | main_seller_state_ohe_RS | 0.0017144082812605 |
| 49 | main_seller_state_ohe_DF | 0.0002543346627876 |
| 50 | main_seller_state_ohe_BA | 0.0002796719413803 |
| 51 | main_seller_state_ohe_GO | 0.0001291743905657 |
| 52 | main_seller_state_ohe_PE | 0.0000000000000000 |
| 53 | main_seller_state_ohe_MA | 0.0230361692116746 |
| 54 | main_seller_state_ohe_ES | 0.0000000000000000 |
| 55 | main_seller_state_ohe_MT | 0.0016860408209146 |
| 56 | main_seller_state_ohe_CE | 0.0000000000000000 |
| 57 | main_seller_state_ohe_MS | 0.0000000000000000 |
| 58 | main_seller_state_ohe_RN | 0.0000505239086512 |
| 59 | main_seller_state_ohe_PB | 0.0006019687762016 |
| 60 | main_seller_state_ohe_RO | 0.0000000000000000 |
| 61 | main_seller_state_ohe_PI | 0.0000000000000000 |
| 62 | main_seller_state_ohe_PA | 0.0000000000000000 |
| 63 | main_seller_state_ohe_SE | 0.0000000000000000 |
| 64 | main_seller_state_ohe_AM | 0.0025965314065828 |
| 65 | main_seller_state_ohe___unknown | 0.0000000000000000 |
| 66 | main_category_ohe_bed_bath_table | 0.0013499337822798 |
| 67 | main_category_ohe_health_beauty | 0.0024740398489063 |
| 68 | main_category_ohe_sports_leisure | 0.0037578675355428 |
| 69 | main_category_ohe_computers_accessories | 0.0000000000000000 |
| 70 | main_category_ohe_furniture_decor | 0.0014201186735282 |
| 71 | main_category_ohe_housewares | 0.0045214987254041 |
| 72 | main_category_ohe_watches_gifts | 0.0005578972635343 |
| 73 | main_category_ohe_telephony | 0.0015380570126052 |
| 74 | main_category_ohe_auto | 0.0009494066446624 |
| 75 | main_category_ohe_toys | 0.0014184151106747 |
| 76 | main_category_ohe_cool_stuff | 0.0021678043543160 |
| 77 | main_category_ohe_garden_tools | 0.0000000000000000 |
| 78 | main_category_ohe_perfumery | 0.0001172068749018 |
| 79 | main_category_ohe_baby | 0.0021190338263406 |
| 80 | main_category_ohe_electronics | 0.0006783173978223 |
| 81 | main_category_ohe_stationery | 0.0004565173781002 |
| 82 | main_category_ohe_fashion_bags_accessories | 0.0020906932851160 |
| 83 | main_category_ohe_pet_shop | 0.0003111402226853 |
| 84 | main_category_ohe_khong_xac_dinh | 0.0025784869705454 |
| 85 | main_category_ohe_office_furniture | 0.0003162909430999 |
| 86 | main_category_ohe_luggage_accessories | 0.0001027118089607 |
| 87 | main_category_ohe_consoles_games | 0.0009543779259234 |
| 88 | main_category_ohe_home_appliances | 0.0021304051550582 |
| 89 | main_category_ohe_construction_tools_construction | 0.0000000000000000 |
| 90 | main_category_ohe_musical_instruments | 0.0000776948079460 |
| 91 | main_category_ohe_small_appliances | 0.0003054736932277 |
| 92 | main_category_ohe_books_general_interest | 0.0027543929571394 |
| 93 | main_category_ohe_home_construction | 0.0008520930853690 |
| 94 | main_category_ohe_food | 0.0003574363597895 |
| 95 | main_category_ohe_furniture_living_room | 0.0015314436685547 |
| 96 | main_category_ohe_home_confort | 0.0032478596030066 |
| 97 | main_category_ohe_audio | 0.0028432277265900 |
| 98 | main_category_ohe_drinks | 0.0000000000000000 |
| 99 | main_category_ohe_market_place | 0.0000000000000000 |
| 100 | main_category_ohe_books_technical | 0.0037822583451731 |
| 101 | main_category_ohe_air_conditioning | 0.0008402629726531 |
| 102 | main_category_ohe_kitchen_dining_laundry_garden_furniture | 0.0007552715081676 |
| 103 | main_category_ohe_construction_tools_lights | 0.0018982816422541 |
| 104 | main_category_ohe_fashion_shoes | 0.0000000000000000 |
| 105 | main_category_ohe_industry_commerce_and_business | 0.0021598193565216 |
| 106 | main_category_ohe_food_drink | 0.0000770257225036 |
| 107 | main_category_ohe_home_appliances_2 | 0.0018299578904423 |
| 108 | main_category_ohe_fixed_telephony | 0.0004188257700047 |
| 109 | main_category_ohe_costruction_tools_garden | 0.0037826980641810 |
| 110 | main_category_ohe_art | 0.0000000000000000 |
| 111 | main_category_ohe_computers | 0.0013292212011037 |
| 112 | main_category_ohe_agro_industry_and_commerce | 0.0006021020651758 |
| 113 | main_category_ohe_construction_tools_safety | 0.0000000000000000 |
| 114 | main_category_ohe_signaling_and_security | 0.0077965121674431 |
| 115 | main_category_ohe_fashion_underwear_beach | 0.0011854469788165 |
| 116 | main_category_ohe_christmas_supplies | 0.0016892269625734 |
| 117 | main_category_ohe_fashion_male_clothing | 0.0000000000000000 |
| 118 | main_category_ohe_costruction_tools_tools | 0.0000000000000000 |
| 119 | main_category_ohe_furniture_bedroom | 0.0007958657043941 |
| 120 | main_category_ohe_tablets_printing_image | 0.0000000000000000 |
| 121 | main_category_ohe_cine_photo | 0.0002438607981668 |
| 122 | main_category_ohe_small_appliances_home_oven_and_coffee | 0.0032761399615238 |
| 123 | main_category_ohe_dvds_blu_ray | 0.0007962702273429 |
| 124 | main_category_ohe_books_imported | 0.0000000000000000 |
| 125 | main_category_ohe_party_supplies | 0.0000000000000000 |
| 126 | main_category_ohe_furniture_mattress_and_upholstery | 0.0011220401769650 |
| 127 | main_category_ohe_music | 0.0035356811870785 |
| 128 | main_category_ohe_fashio_female_clothing | 0.0000000000000000 |
| 129 | main_category_ohe_flowers | 0.0000000000000000 |
| 130 | main_category_ohe_arts_and_craftmanship | 0.0000000000000000 |
| 131 | main_category_ohe_fashion_sport | 0.0000000000000000 |
| 132 | main_category_ohe_home_comfort_2 | 0.0000000000000000 |
| 133 | main_category_ohe_diapers_and_hygiene | 0.0000000000000000 |
| 134 | main_category_ohe_la_cuisine | 0.0000000000000000 |
| 135 | main_category_ohe_portateis_cozinha_e_preparadores_de_alimentos | 0.0004489490563415 |
| 136 | main_category_ohe_cds_dvds_musicals | 0.0000000000000000 |
| 137 | main_category_ohe_fashion_childrens_clothes | 0.0000000000000000 |
| 138 | main_category_ohe_pc_gamer | 0.0000000000000000 |
| 139 | main_category_ohe_security_and_services | 0.0000000000000000 |
| 140 | main_category_ohe___unknown | 0.0000000000000000 |

### Tree details

| Tree | Tree weight | Depth | Num nodes |
| --- | --- | --- | --- |
| 0 | 1.000000000000 | 6 | 43 |
| 1 | 1.000000000000 | 6 | 29 |
| 2 | 1.000000000000 | 6 | 23 |
| 3 | 1.000000000000 | 6 | 49 |
| 4 | 1.000000000000 | 6 | 53 |
| 5 | 1.000000000000 | 6 | 45 |
| 6 | 1.000000000000 | 6 | 49 |
| 7 | 1.000000000000 | 6 | 47 |
| 8 | 1.000000000000 | 6 | 67 |
| 9 | 1.000000000000 | 6 | 21 |
| 10 | 1.000000000000 | 6 | 31 |
| 11 | 1.000000000000 | 6 | 49 |
| 12 | 1.000000000000 | 6 | 37 |
| 13 | 1.000000000000 | 6 | 31 |
| 14 | 1.000000000000 | 6 | 43 |
| 15 | 1.000000000000 | 6 | 35 |
| 16 | 1.000000000000 | 6 | 53 |
| 17 | 1.000000000000 | 6 | 43 |
| 18 | 1.000000000000 | 6 | 53 |
| 19 | 1.000000000000 | 6 | 35 |
| 20 | 1.000000000000 | 6 | 27 |
| 21 | 1.000000000000 | 6 | 45 |
| 22 | 1.000000000000 | 6 | 39 |
| 23 | 1.000000000000 | 6 | 25 |
| 24 | 1.000000000000 | 6 | 41 |
| 25 | 1.000000000000 | 6 | 23 |
| 26 | 1.000000000000 | 6 | 29 |
| 27 | 1.000000000000 | 6 | 53 |
| 28 | 1.000000000000 | 6 | 39 |
| 29 | 1.000000000000 | 6 | 33 |

PySpark 4.1.2 cung cấp API công khai `trees`, `treeWeights`, `predictRaw`, `predictProbability`, `leafCol` và `predictLeaf`. Vì vậy báo cáo dùng `predictProbability` công khai của từng decision tree để kiểm tra đúng phép tổng có trọng số mà Random Forest dùng tạo model `rawPrediction`; đồng thời vẫn xuất `rawPrediction` riêng của từng decision tree.

### Order A: `f46b842d9b4dfd29acf5eec998837ede`

`probability_manual = rawPrediction[1] / (rawPrediction[0] + rawPrediction[1])` = 4.0051717279829058 / (25.994828272017099 + 4.0051717279829058) = 0.13350572426609683.

probability_Spark = `0.13350572426609683`; absolute_difference = `0`; prediction_common = `1` tại common threshold `0.094`.

| Tree | Leaf | Raw[0] | Raw[1] | Tree probability[0] | Tree probability[1] | Weight | Contribution[0] | Contribution[1] |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 9.0 | 61277.0000000000000000 | 4255.0000000000000000 | 0.9350698895196240 | 0.0649301104803760 | 1.000000000000 | 0.9350698895196240 | 0.0649301104803760 |
| 1 | 0.0 | 18027.0000000000000000 | 1069.0000000000000000 | 0.9440196899874319 | 0.0559803100125681 | 1.000000000000 | 0.9440196899874319 | 0.0559803100125681 |
| 2 | 11.0 | 66081.0000000000000000 | 4848.0000000000000000 | 0.9316499598189739 | 0.0683500401810261 | 1.000000000000 | 0.9316499598189739 | 0.0683500401810261 |
| 3 | 13.0 | 31561.0000000000000000 | 2421.0000000000000000 | 0.9287564004472957 | 0.0712435995527044 | 1.000000000000 | 0.9287564004472957 | 0.0712435995527044 |
| 4 | 12.0 | 6.0000000000000000 | 9.0000000000000000 | 0.4000000000000000 | 0.6000000000000000 | 1.000000000000 | 0.4000000000000000 | 0.6000000000000000 |
| 5 | 20.0 | 62693.0000000000000000 | 4695.0000000000000000 | 0.9303288419303140 | 0.0696711580696860 | 1.000000000000 | 0.9303288419303140 | 0.0696711580696860 |
| 6 | 24.0 | 28317.0000000000000000 | 2657.0000000000000000 | 0.9142183767030413 | 0.0857816232969587 | 1.000000000000 | 0.9142183767030413 | 0.0857816232969587 |
| 7 | 12.0 | 7128.0000000000000000 | 501.0000000000000000 | 0.9343295320487613 | 0.0656704679512387 | 1.000000000000 | 0.9343295320487613 | 0.0656704679512387 |
| 8 | 26.0 | 60918.0000000000000000 | 4777.0000000000000000 | 0.9272851815206636 | 0.0727148184793363 | 1.000000000000 | 0.9272851815206636 | 0.0727148184793363 |
| 9 | 4.0 | 45694.0000000000000000 | 3950.0000000000000000 | 0.9204334864233341 | 0.0795665135766659 | 1.000000000000 | 0.9204334864233341 | 0.0795665135766659 |
| 10 | 15.0 | 29819.0000000000000000 | 1813.0000000000000000 | 0.9426846231664138 | 0.0573153768335862 | 1.000000000000 | 0.9426846231664138 | 0.0573153768335862 |
| 11 | 12.0 | 26014.0000000000000000 | 1715.0000000000000000 | 0.9381513938475964 | 0.0618486061524036 | 1.000000000000 | 0.9381513938475964 | 0.0618486061524036 |
| 12 | 7.0 | 33249.0000000000000000 | 2986.0000000000000000 | 0.9175934869601214 | 0.0824065130398786 | 1.000000000000 | 0.9175934869601214 | 0.0824065130398786 |
| 13 | 15.0 | 57470.0000000000000000 | 4194.0000000000000000 | 0.9319862480539699 | 0.0680137519460301 | 1.000000000000 | 0.9319862480539699 | 0.0680137519460301 |
| 14 | 20.0 | 60253.0000000000000000 | 3691.0000000000000000 | 0.9422776179156762 | 0.0577223820843238 | 1.000000000000 | 0.9422776179156762 | 0.0577223820843238 |
| 15 | 6.0 | 18246.0000000000000000 | 879.0000000000000000 | 0.9540392156862745 | 0.0459607843137255 | 1.000000000000 | 0.9540392156862745 | 0.0459607843137255 |
| 16 | 23.0 | 44686.0000000000000000 | 2271.0000000000000000 | 0.9516366037012586 | 0.0483633962987414 | 1.000000000000 | 0.9516366037012586 | 0.0483633962987414 |
| 17 | 3.0 | 31.0000000000000000 | 40.0000000000000000 | 0.4366197183098591 | 0.5633802816901409 | 1.000000000000 | 0.4366197183098591 | 0.5633802816901409 |
| 18 | 20.0 | 220.0000000000000000 | 21.0000000000000000 | 0.9128630705394191 | 0.0871369294605809 | 1.000000000000 | 0.9128630705394191 | 0.0871369294605809 |
| 19 | 3.0 | 40.0000000000000000 | 15.0000000000000000 | 0.7272727272727273 | 0.2727272727272727 | 1.000000000000 | 0.7272727272727273 | 0.2727272727272727 |
| 20 | 13.0 | 30665.0000000000000000 | 1867.0000000000000000 | 0.9426103528833149 | 0.0573896471166851 | 1.000000000000 | 0.9426103528833149 | 0.0573896471166851 |
| 21 | 21.0 | 36463.0000000000000000 | 3143.0000000000000000 | 0.9206433368681513 | 0.0793566631318487 | 1.000000000000 | 0.9206433368681513 | 0.0793566631318487 |
| 22 | 18.0 | 31149.0000000000000000 | 1532.0000000000000000 | 0.9531226094672746 | 0.0468773905327254 | 1.000000000000 | 0.9531226094672746 | 0.0468773905327254 |
| 23 | 2.0 | 67.0000000000000000 | 20.0000000000000000 | 0.7701149425287356 | 0.2298850574712644 | 1.000000000000 | 0.7701149425287356 | 0.2298850574712644 |
| 24 | 6.0 | 22.0000000000000000 | 11.0000000000000000 | 0.6666666666666666 | 0.3333333333333333 | 1.000000000000 | 0.6666666666666666 | 0.3333333333333333 |
| 25 | 5.0 | 31926.0000000000000000 | 2884.0000000000000000 | 0.9171502441827061 | 0.0828497558172939 | 1.000000000000 | 0.9171502441827061 | 0.0828497558172939 |
| 26 | 14.0 | 31886.0000000000000000 | 1966.0000000000000000 | 0.9419236677301194 | 0.0580763322698807 | 1.000000000000 | 0.9419236677301194 | 0.0580763322698807 |
| 27 | 21.0 | 23415.0000000000000000 | 1298.0000000000000000 | 0.9474770363776150 | 0.0525229636223850 | 1.000000000000 | 0.9474770363776150 | 0.0525229636223850 |
| 28 | 8.0 | 13805.0000000000000000 | 600.0000000000000000 | 0.9583477959041999 | 0.0416522040958001 | 1.000000000000 | 0.9583477959041999 | 0.0416522040958001 |
| 29 | 2.0 | 60.0000000000000000 | 48.0000000000000000 | 0.5555555555555556 | 0.4444444444444444 | 1.000000000000 | 0.5555555555555556 | 0.4444444444444444 |

Tổng contribution theo treeWeights = `[25.994828272017095, 4.0051717279829049]`, khớp model rawPrediction.

### Order B: `686c0ba20be3837a5041edbc39d3f9ae`

`probability_manual = rawPrediction[1] / (rawPrediction[0] + rawPrediction[1])` = 3.8650550253086964 / (26.134944974691305 + 3.8650550253086964) = 0.12883516751028987.

probability_Spark = `0.12883516751028987`; absolute_difference = `0`; prediction_common = `1` tại common threshold `0.094`.

| Tree | Leaf | Raw[0] | Raw[1] | Tree probability[0] | Tree probability[1] | Weight | Contribution[0] | Contribution[1] |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 9.0 | 61277.0000000000000000 | 4255.0000000000000000 | 0.9350698895196240 | 0.0649301104803760 | 1.000000000000 | 0.9350698895196240 | 0.0649301104803760 |
| 1 | 0.0 | 18027.0000000000000000 | 1069.0000000000000000 | 0.9440196899874319 | 0.0559803100125681 | 1.000000000000 | 0.9440196899874319 | 0.0559803100125681 |
| 2 | 11.0 | 66081.0000000000000000 | 4848.0000000000000000 | 0.9316499598189739 | 0.0683500401810261 | 1.000000000000 | 0.9316499598189739 | 0.0683500401810261 |
| 3 | 13.0 | 31561.0000000000000000 | 2421.0000000000000000 | 0.9287564004472957 | 0.0712435995527044 | 1.000000000000 | 0.9287564004472957 | 0.0712435995527044 |
| 4 | 16.0 | 253.0000000000000000 | 51.0000000000000000 | 0.8322368421052632 | 0.1677631578947368 | 1.000000000000 | 0.8322368421052632 | 0.1677631578947368 |
| 5 | 20.0 | 62693.0000000000000000 | 4695.0000000000000000 | 0.9303288419303140 | 0.0696711580696860 | 1.000000000000 | 0.9303288419303140 | 0.0696711580696860 |
| 6 | 16.0 | 15325.0000000000000000 | 559.0000000000000000 | 0.9648073533115085 | 0.0351926466884916 | 1.000000000000 | 0.9648073533115085 | 0.0351926466884916 |
| 7 | 15.0 | 5389.0000000000000000 | 198.0000000000000000 | 0.9645605870771433 | 0.0354394129228566 | 1.000000000000 | 0.9645605870771433 | 0.0354394129228566 |
| 8 | 26.0 | 60918.0000000000000000 | 4777.0000000000000000 | 0.9272851815206636 | 0.0727148184793363 | 1.000000000000 | 0.9272851815206636 | 0.0727148184793363 |
| 9 | 4.0 | 45694.0000000000000000 | 3950.0000000000000000 | 0.9204334864233341 | 0.0795665135766659 | 1.000000000000 | 0.9204334864233341 | 0.0795665135766659 |
| 10 | 11.0 | 17914.0000000000000000 | 2304.0000000000000000 | 0.8860421406667326 | 0.1139578593332674 | 1.000000000000 | 0.8860421406667326 | 0.1139578593332674 |
| 11 | 9.0 | 22775.0000000000000000 | 1103.0000000000000000 | 0.9538068514951001 | 0.0461931485048999 | 1.000000000000 | 0.9538068514951001 | 0.0461931485048999 |
| 12 | 7.0 | 33249.0000000000000000 | 2986.0000000000000000 | 0.9175934869601214 | 0.0824065130398786 | 1.000000000000 | 0.9175934869601214 | 0.0824065130398786 |
| 13 | 15.0 | 57470.0000000000000000 | 4194.0000000000000000 | 0.9319862480539699 | 0.0680137519460301 | 1.000000000000 | 0.9319862480539699 | 0.0680137519460301 |
| 14 | 20.0 | 60253.0000000000000000 | 3691.0000000000000000 | 0.9422776179156762 | 0.0577223820843238 | 1.000000000000 | 0.9422776179156762 | 0.0577223820843238 |
| 15 | 0.0 | 29572.0000000000000000 | 1767.0000000000000000 | 0.9436165799802163 | 0.0563834200197837 | 1.000000000000 | 0.9436165799802163 | 0.0563834200197837 |
| 16 | 15.0 | 0.0000000000000000 | 1.0000000000000000 | 0.0000000000000000 | 1.0000000000000000 | 1.000000000000 | 0.0000000000000000 | 1.0000000000000000 |
| 17 | 8.0 | 18737.0000000000000000 | 1168.0000000000000000 | 0.9413212760612911 | 0.0586787239387089 | 1.000000000000 | 0.9413212760612911 | 0.0586787239387089 |
| 18 | 16.0 | 996.0000000000000000 | 278.0000000000000000 | 0.7817896389324961 | 0.2182103610675039 | 1.000000000000 | 0.7817896389324961 | 0.2182103610675039 |
| 19 | 17.0 | 59510.0000000000000000 | 3941.0000000000000000 | 0.9378890797623363 | 0.0621109202376637 | 1.000000000000 | 0.9378890797623363 | 0.0621109202376637 |
| 20 | 9.0 | 2075.0000000000000000 | 315.0000000000000000 | 0.8682008368200836 | 0.1317991631799163 | 1.000000000000 | 0.8682008368200836 | 0.1317991631799163 |
| 21 | 19.0 | 250.0000000000000000 | 31.0000000000000000 | 0.8896797153024911 | 0.1103202846975089 | 1.000000000000 | 0.8896797153024911 | 0.1103202846975089 |
| 22 | 11.0 | 42.0000000000000000 | 23.0000000000000000 | 0.6461538461538462 | 0.3538461538461539 | 1.000000000000 | 0.6461538461538462 | 0.3538461538461539 |
| 23 | 11.0 | 17588.0000000000000000 | 2191.0000000000000000 | 0.8892259467111583 | 0.1107740532888417 | 1.000000000000 | 0.8892259467111583 | 0.1107740532888417 |
| 24 | 14.0 | 37288.0000000000000000 | 2837.0000000000000000 | 0.9292959501557633 | 0.0707040498442368 | 1.000000000000 | 0.9292959501557633 | 0.0707040498442368 |
| 25 | 5.0 | 31926.0000000000000000 | 2884.0000000000000000 | 0.9171502441827061 | 0.0828497558172939 | 1.000000000000 | 0.9171502441827061 | 0.0828497558172939 |
| 26 | 4.0 | 455.0000000000000000 | 104.0000000000000000 | 0.8139534883720930 | 0.1860465116279070 | 1.000000000000 | 0.8139534883720930 | 0.1860465116279070 |
| 27 | 18.0 | 8165.0000000000000000 | 1208.0000000000000000 | 0.8711191720900459 | 0.1288808279099541 | 1.000000000000 | 0.8711191720900459 | 0.1288808279099541 |
| 28 | 5.0 | 15673.0000000000000000 | 2146.0000000000000000 | 0.8795667545877995 | 0.1204332454122005 | 1.000000000000 | 0.8795667545877995 | 0.1204332454122005 |
| 29 | 16.0 | 23689.0000000000000000 | 2197.0000000000000000 | 0.9151278683458240 | 0.0848721316541760 | 1.000000000000 | 0.9151278683458240 | 0.0848721316541760 |

Tổng contribution theo treeWeights = `[26.134944974691305, 3.8650550253086968]`, khớp model rawPrediction.

### Order C: `9b1d71b20edcf15ab15e0bb4a932f23f`

`probability_manual = rawPrediction[1] / (rawPrediction[0] + rawPrediction[1])` = 1.867649535389295 / (28.132350464610706 + 1.867649535389295) = 0.062254984512976501.

probability_Spark = `0.062254984512976501`; absolute_difference = `0`; prediction_common = `0` tại common threshold `0.094`.

| Tree | Leaf | Raw[0] | Raw[1] | Tree probability[0] | Tree probability[1] | Weight | Contribution[0] | Contribution[1] |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 9.0 | 61277.0000000000000000 | 4255.0000000000000000 | 0.9350698895196240 | 0.0649301104803760 | 1.000000000000 | 0.9350698895196240 | 0.0649301104803760 |
| 1 | 2.0 | 976.0000000000000000 | 73.0000000000000000 | 0.9304099142040038 | 0.0695900857959962 | 1.000000000000 | 0.9304099142040038 | 0.0695900857959962 |
| 2 | 11.0 | 66081.0000000000000000 | 4848.0000000000000000 | 0.9316499598189739 | 0.0683500401810261 | 1.000000000000 | 0.9316499598189739 | 0.0683500401810261 |
| 3 | 13.0 | 31561.0000000000000000 | 2421.0000000000000000 | 0.9287564004472957 | 0.0712435995527044 | 1.000000000000 | 0.9287564004472957 | 0.0712435995527044 |
| 4 | 20.0 | 63603.0000000000000000 | 4909.0000000000000000 | 0.9283483185427370 | 0.0716516814572630 | 1.000000000000 | 0.9283483185427370 | 0.0716516814572630 |
| 5 | 20.0 | 62693.0000000000000000 | 4695.0000000000000000 | 0.9303288419303140 | 0.0696711580696860 | 1.000000000000 | 0.9303288419303140 | 0.0696711580696860 |
| 6 | 12.0 | 444.0000000000000000 | 7.0000000000000000 | 0.9844789356984479 | 0.0155210643015521 | 1.000000000000 | 0.9844789356984479 | 0.0155210643015521 |
| 7 | 20.0 | 32094.0000000000000000 | 2015.0000000000000000 | 0.9409246826350817 | 0.0590753173649183 | 1.000000000000 | 0.9409246826350817 | 0.0590753173649183 |
| 8 | 26.0 | 60918.0000000000000000 | 4777.0000000000000000 | 0.9272851815206636 | 0.0727148184793363 | 1.000000000000 | 0.9272851815206636 | 0.0727148184793363 |
| 9 | 4.0 | 45694.0000000000000000 | 3950.0000000000000000 | 0.9204334864233341 | 0.0795665135766659 | 1.000000000000 | 0.9204334864233341 | 0.0795665135766659 |
| 10 | 15.0 | 29819.0000000000000000 | 1813.0000000000000000 | 0.9426846231664138 | 0.0573153768335862 | 1.000000000000 | 0.9426846231664138 | 0.0573153768335862 |
| 11 | 9.0 | 22775.0000000000000000 | 1103.0000000000000000 | 0.9538068514951001 | 0.0461931485048999 | 1.000000000000 | 0.9538068514951001 | 0.0461931485048999 |
| 12 | 7.0 | 33249.0000000000000000 | 2986.0000000000000000 | 0.9175934869601214 | 0.0824065130398786 | 1.000000000000 | 0.9175934869601214 | 0.0824065130398786 |
| 13 | 15.0 | 57470.0000000000000000 | 4194.0000000000000000 | 0.9319862480539699 | 0.0680137519460301 | 1.000000000000 | 0.9319862480539699 | 0.0680137519460301 |
| 14 | 20.0 | 60253.0000000000000000 | 3691.0000000000000000 | 0.9422776179156762 | 0.0577223820843238 | 1.000000000000 | 0.9422776179156762 | 0.0577223820843238 |
| 15 | 0.0 | 29572.0000000000000000 | 1767.0000000000000000 | 0.9436165799802163 | 0.0563834200197837 | 1.000000000000 | 0.9436165799802163 | 0.0563834200197837 |
| 16 | 23.0 | 44686.0000000000000000 | 2271.0000000000000000 | 0.9516366037012586 | 0.0483633962987414 | 1.000000000000 | 0.9516366037012586 | 0.0483633962987414 |
| 17 | 21.0 | 50078.0000000000000000 | 3975.0000000000000000 | 0.9264610659907868 | 0.0735389340092132 | 1.000000000000 | 0.9264610659907868 | 0.0735389340092132 |
| 18 | 23.0 | 44402.0000000000000000 | 2309.0000000000000000 | 0.9505683886022565 | 0.0494316113977436 | 1.000000000000 | 0.9505683886022565 | 0.0494316113977436 |
| 19 | 17.0 | 59510.0000000000000000 | 3941.0000000000000000 | 0.9378890797623363 | 0.0621109202376637 | 1.000000000000 | 0.9378890797623363 | 0.0621109202376637 |
| 20 | 13.0 | 30665.0000000000000000 | 1867.0000000000000000 | 0.9426103528833149 | 0.0573896471166851 | 1.000000000000 | 0.9426103528833149 | 0.0573896471166851 |
| 21 | 20.0 | 30504.0000000000000000 | 1830.0000000000000000 | 0.9434032287994062 | 0.0565967712005938 | 1.000000000000 | 0.9434032287994062 | 0.0565967712005938 |
| 22 | 18.0 | 31149.0000000000000000 | 1532.0000000000000000 | 0.9531226094672746 | 0.0468773905327254 | 1.000000000000 | 0.9531226094672746 | 0.0468773905327254 |
| 23 | 12.0 | 53695.0000000000000000 | 2923.0000000000000000 | 0.9483733088417111 | 0.0516266911582889 | 1.000000000000 | 0.9483733088417111 | 0.0516266911582889 |
| 24 | 14.0 | 37288.0000000000000000 | 2837.0000000000000000 | 0.9292959501557633 | 0.0707040498442368 | 1.000000000000 | 0.9292959501557633 | 0.0707040498442368 |
| 25 | 5.0 | 31926.0000000000000000 | 2884.0000000000000000 | 0.9171502441827061 | 0.0828497558172939 | 1.000000000000 | 0.9171502441827061 | 0.0828497558172939 |
| 26 | 14.0 | 31886.0000000000000000 | 1966.0000000000000000 | 0.9419236677301194 | 0.0580763322698807 | 1.000000000000 | 0.9419236677301194 | 0.0580763322698807 |
| 27 | 21.0 | 23415.0000000000000000 | 1298.0000000000000000 | 0.9474770363776150 | 0.0525229636223850 | 1.000000000000 | 0.9474770363776150 | 0.0525229636223850 |
| 28 | 12.0 | 30759.0000000000000000 | 2045.0000000000000000 | 0.9376600414583587 | 0.0623399585416413 | 1.000000000000 | 0.9376600414583587 | 0.0623399585416413 |
| 29 | 16.0 | 23689.0000000000000000 | 2197.0000000000000000 | 0.9151278683458240 | 0.0848721316541760 | 1.000000000000 | 0.9151278683458240 | 0.0848721316541760 |

Tổng contribution theo treeWeights = `[28.132350464610706, 1.8676495353892952]`, khớp model rawPrediction.

### Order D: `c2bb89b5c1dd978d507284be78a04cb2`

`probability_manual = rawPrediction[1] / (rawPrediction[0] + rawPrediction[1])` = 1.6480278416965073 / (28.351972158303493 + 1.6480278416965073) = 0.054934261389883575.

probability_Spark = `0.054934261389883575`; absolute_difference = `0`; prediction_common = `0` tại common threshold `0.094`.

| Tree | Leaf | Raw[0] | Raw[1] | Tree probability[0] | Tree probability[1] | Weight | Contribution[0] | Contribution[1] |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 9.0 | 61277.0000000000000000 | 4255.0000000000000000 | 0.9350698895196240 | 0.0649301104803760 | 1.000000000000 | 0.9350698895196240 | 0.0649301104803760 |
| 1 | 5.0 | 1310.0000000000000000 | 151.0000000000000000 | 0.8966461327857632 | 0.1033538672142368 | 1.000000000000 | 0.8966461327857632 | 0.1033538672142368 |
| 2 | 11.0 | 66081.0000000000000000 | 4848.0000000000000000 | 0.9316499598189739 | 0.0683500401810261 | 1.000000000000 | 0.9316499598189739 | 0.0683500401810261 |
| 3 | 13.0 | 31561.0000000000000000 | 2421.0000000000000000 | 0.9287564004472957 | 0.0712435995527044 | 1.000000000000 | 0.9287564004472957 | 0.0712435995527044 |
| 4 | 26.0 | 2366.0000000000000000 | 124.0000000000000000 | 0.9502008032128514 | 0.0497991967871486 | 1.000000000000 | 0.9502008032128514 | 0.0497991967871486 |
| 5 | 22.0 | 7267.0000000000000000 | 248.0000000000000000 | 0.9669993346640053 | 0.0330006653359947 | 1.000000000000 | 0.9669993346640053 | 0.0330006653359947 |
| 6 | 17.0 | 3010.0000000000000000 | 161.0000000000000000 | 0.9492273730684326 | 0.0507726269315673 | 1.000000000000 | 0.9492273730684326 | 0.0507726269315673 |
| 7 | 15.0 | 5389.0000000000000000 | 198.0000000000000000 | 0.9645605870771433 | 0.0354394129228566 | 1.000000000000 | 0.9645605870771433 | 0.0354394129228566 |
| 8 | 33.0 | 3214.0000000000000000 | 141.0000000000000000 | 0.9579731743666170 | 0.0420268256333830 | 1.000000000000 | 0.9579731743666170 | 0.0420268256333830 |
| 9 | 4.0 | 45694.0000000000000000 | 3950.0000000000000000 | 0.9204334864233341 | 0.0795665135766659 | 1.000000000000 | 0.9204334864233341 | 0.0795665135766659 |
| 10 | 14.0 | 2190.0000000000000000 | 142.0000000000000000 | 0.9391080617495712 | 0.0608919382504288 | 1.000000000000 | 0.9391080617495712 | 0.0608919382504288 |
| 11 | 9.0 | 22775.0000000000000000 | 1103.0000000000000000 | 0.9538068514951001 | 0.0461931485048999 | 1.000000000000 | 0.9538068514951001 | 0.0461931485048999 |
| 12 | 3.0 | 2393.0000000000000000 | 143.0000000000000000 | 0.9436119873817035 | 0.0563880126182965 | 1.000000000000 | 0.9436119873817035 | 0.0563880126182965 |
| 13 | 15.0 | 57470.0000000000000000 | 4194.0000000000000000 | 0.9319862480539699 | 0.0680137519460301 | 1.000000000000 | 0.9319862480539699 | 0.0680137519460301 |
| 14 | 20.0 | 60253.0000000000000000 | 3691.0000000000000000 | 0.9422776179156762 | 0.0577223820843238 | 1.000000000000 | 0.9422776179156762 | 0.0577223820843238 |
| 15 | 17.0 | 6931.0000000000000000 | 361.0000000000000000 | 0.9504936917169501 | 0.0495063082830499 | 1.000000000000 | 0.9504936917169501 | 0.0495063082830499 |
| 16 | 26.0 | 561.0000000000000000 | 25.0000000000000000 | 0.9573378839590444 | 0.0426621160409556 | 1.000000000000 | 0.9573378839590444 | 0.0426621160409556 |
| 17 | 9.0 | 2050.0000000000000000 | 36.0000000000000000 | 0.9827420901246404 | 0.0172579098753595 | 1.000000000000 | 0.9827420901246404 | 0.0172579098753595 |
| 18 | 10.0 | 281.0000000000000000 | 1.0000000000000000 | 0.9964539007092199 | 0.0035460992907801 | 1.000000000000 | 0.9964539007092199 | 0.0035460992907801 |
| 19 | 17.0 | 59510.0000000000000000 | 3941.0000000000000000 | 0.9378890797623363 | 0.0621109202376637 | 1.000000000000 | 0.9378890797623363 | 0.0621109202376637 |
| 20 | 3.0 | 31106.0000000000000000 | 1512.0000000000000000 | 0.9536452265620210 | 0.0463547734379790 | 1.000000000000 | 0.9536452265620210 | 0.0463547734379790 |
| 21 | 20.0 | 30504.0000000000000000 | 1830.0000000000000000 | 0.9434032287994062 | 0.0565967712005938 | 1.000000000000 | 0.9434032287994062 | 0.0565967712005938 |
| 22 | 19.0 | 17760.0000000000000000 | 1190.0000000000000000 | 0.9372031662269129 | 0.0627968337730871 | 1.000000000000 | 0.9372031662269129 | 0.0627968337730871 |
| 23 | 12.0 | 53695.0000000000000000 | 2923.0000000000000000 | 0.9483733088417111 | 0.0516266911582889 | 1.000000000000 | 0.9483733088417111 | 0.0516266911582889 |
| 24 | 14.0 | 37288.0000000000000000 | 2837.0000000000000000 | 0.9292959501557633 | 0.0707040498442368 | 1.000000000000 | 0.9292959501557633 | 0.0707040498442368 |
| 25 | 10.0 | 5334.0000000000000000 | 447.0000000000000000 | 0.9226777374156721 | 0.0773222625843280 | 1.000000000000 | 0.9226777374156721 | 0.0773222625843280 |
| 26 | 14.0 | 31886.0000000000000000 | 1966.0000000000000000 | 0.9419236677301194 | 0.0580763322698807 | 1.000000000000 | 0.9419236677301194 | 0.0580763322698807 |
| 27 | 17.0 | 29969.0000000000000000 | 1466.0000000000000000 | 0.9533640846190552 | 0.0466359153809448 | 1.000000000000 | 0.9533640846190552 | 0.0466359153809448 |
| 28 | 19.0 | 5949.0000000000000000 | 270.0000000000000000 | 0.9565846599131693 | 0.0434153400868307 | 1.000000000000 | 0.9565846599131693 | 0.0434153400868307 |
| 29 | 12.0 | 1799.0000000000000000 | 139.0000000000000000 | 0.9282765737874097 | 0.0717234262125903 | 1.000000000000 | 0.9282765737874097 | 0.0717234262125903 |

Tổng contribution theo treeWeights = `[28.351972158303493, 1.6480278416965075]`, khớp model rawPrediction.

## PHẦN 8. CÁCH CHỌN COMMON THRESHOLD

Coarse thử 0.01 đến 0.49 với bước 0.01. Refine lấy ±0.03 quanh candidate coarse tốt nhất, bước 0.001, và chặn candidate trong khoảng 0.001 đến 0.499. Cùng một danh sách threshold được áp dụng cho hai model.

Candidate chỉ hợp lệ nếu alert rate của cả Logistic Regression và Random Forest không vượt 20%. Xếp hạng lần lượt: average_f1 cao nhất; minimum_f1 cao hơn; average_recall cao hơn; average_alert_rate thấp hơn; common threshold cao hơn.

Baseline majority class không tham gia chọn common threshold và không tham gia chọn model demo; baseline chỉ là mốc so sánh bắt buộc trên cùng data split.

`average_f1 = (F1_Logistic_Regression + F1_Random_Forest) / 2`. Tổng số candidate đã thử: `110` (49 coarse + 61 refine).

### Top 10 candidate hợp lệ

| Hạng | Common threshold | LR F1 | RF F1 | average_f1 | minimum_f1 | average_recall | average_alert_rate |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 0.094 | 0.223906135419 | 0.220057306590 | 0.221981721005 | 0.220057306590 | 0.310410697230 | 0.122088091354 |
| 2 | 0.095 | 0.224201930215 | 0.208182912154 | 0.216192421185 | 0.208182912154 | 0.298949379179 | 0.117748776509 |
| 3 | 0.096 | 0.224458983392 | 0.198998748436 | 0.211728865914 | 0.198998748436 | 0.288920725883 | 0.113474714519 |
| 4 | 0.097 | 0.224155578301 | 0.194087403599 | 0.209121490950 | 0.194087403599 | 0.281279847182 | 0.109951060359 |
| 5 | 0.098 | 0.224245577523 | 0.192913385827 | 0.208579481675 | 0.192913385827 | 0.276026743075 | 0.106818923328 |
| 6 | 0.099 | 0.224333597255 | 0.179226069246 | 0.201779833251 | 0.179226069246 | 0.265998089780 | 0.103360522023 |
| 7 | 0.100 | 0.224779234680 | 0.169798190675 | 0.197288712678 | 0.169798190675 | 0.258834765998 | 0.100489396411 |
| 8 | 0.101 | 0.224884290770 | 0.160057678443 | 0.192470984607 | 0.160057678443 | 0.250238777459 | 0.096769983687 |
| 9 | 0.102 | 0.225422320687 | 0.151582045622 | 0.188502183154 | 0.151582045622 | 0.243553008596 | 0.093833605220 |
| 10 | 0.103 | 0.224719101124 | 0.146816479401 | 0.185767790262 | 0.146816479401 | 0.237822349570 | 0.091386623165 |

Common threshold được khóa từ validation: **0.094**. Test không tham gia chọn threshold.

- Candidate liền trước `0.093`: Không hợp lệ vì alert rate của ít nhất một model vượt 20%: Logistic Regression=20.332790%, Random Forest=5.050571%.
- Candidate liền sau `0.095`: Bị xếp sau tại tiêu chí `average_f1`: 0.216192421185 thấp hơn giá trị được chọn 0.221981721005.

### So sánh threshold thủ công

Bảng dưới đây tính lại metrics tại các threshold cấu hình trong `THRESHOLD_TEST_THU_CONG`. Bảng chỉ dùng validation; test không được dò lại ở các threshold khác sau khi common threshold đã khóa.

| Tập | Model | Threshold | TP | TN | FP | FN | Precision | Recall | F1 | AUC | Alert rate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| validation | Logistic Regression | 0.090 | 483 | 11431 | 2847 | 564 | 14.504505% | 46.131805% | 22.069911% | 0.695851 | 21.729201% |
| validation | Random Forest | 0.090 | 261 | 13436 | 842 | 786 | 23.662738% | 24.928367% | 24.279070% | 0.684502 | 7.197390% |
| validation | Logistic Regression | 0.094 | 458 | 11692 | 2586 | 589 | 15.045992% | 43.744031% | 22.390614% | 0.695851 | 19.862969% |
| validation | Random Forest | 0.094 | 192 | 13772 | 506 | 855 | 27.507163% | 18.338109% | 22.005731% | 0.684502 | 4.554649% |
| validation | Logistic Regression | 0.100 | 420 | 12008 | 2270 | 627 | 15.613383% | 40.114613% | 22.477923% | 0.695851 | 17.553018% |
| validation | Random Forest | 0.100 | 122 | 14010 | 268 | 925 | 31.282051% | 11.652340% | 16.979819% | 0.684502 | 2.544861% |

## PHẦN 9. CONFUSION MATRIX TRÊN TEST

### Baseline majority class

| Actual / Prediction | Không cảnh báo | Cảnh báo | Tổng actual |
| --- | --- | --- | --- |
| not late | 17830 | 0 | 17830 |
| late | 1261 | 0 | 1261 |
| Tổng prediction | 19091 | 0 | 19091 |

### Logistic Regression

| Actual / Prediction | Không cảnh báo | Cảnh báo | Tổng actual |
| --- | --- | --- | --- |
| not late | 14473 | 3357 | 17830 |
| late | 708 | 553 | 1261 |
| Tổng prediction | 15181 | 3910 | 19091 |

### Random Forest

| Actual / Prediction | Không cảnh báo | Cảnh báo | Tổng actual |
| --- | --- | --- | --- |
| not late | 17057 | 773 | 17830 |
| late | 1020 | 241 | 1261 |
| Tổng prediction | 18077 | 1014 | 19091 |

## PHẦN 10. TOÀN BỘ CÔNG THỨC VÀ THAY SỐ TEST

### Baseline majority class

- Accuracy = (TP + TN) / N = (0 + 17830) / 19091 = 0.933947933581 = 93.394793%.
- Precision = TP / (TP + FP) = 0 / (0 + 0) = 0.000000000000 = 0.000000%.
- Recall = TP / (TP + FN) = 0 / (0 + 1261) = 0.000000000000 = 0.000000%.
- Specificity = TN / (TN + FP) = 17830 / (17830 + 0) = 1.000000000000 = 100.000000%.
- FPR = FP / (FP + TN) = 0 / (0 + 17830) = 0.000000000000 = 0.000000%.
- F1 = 2TP / (2TP + FP + FN) = (2 × 0) / (2 × 0 + 0 + 1261) = 0.000000000000 = 0.000000%.
- alert rate = (TP + FP) / N = (0 + 0) / 19091 = 0.000000000000 = 0.000000%.
- prevalence = (TP + FN) / N = (0 + 1261) / 19091 = 0.066052066419 = 6.605207%.

### Logistic Regression

- Accuracy = (TP + TN) / N = (553 + 14473) / 19091 = 0.787072442512 = 78.707244%.
- Precision = TP / (TP + FP) = 553 / (553 + 3357) = 0.141432225064 = 14.143223%.
- Recall = TP / (TP + FN) = 553 / (553 + 708) = 0.438540840603 = 43.854084%.
- Specificity = TN / (TN + FP) = 14473 / (14473 + 3357) = 0.811721817162 = 81.172182%.
- FPR = FP / (FP + TN) = 3357 / (3357 + 14473) = 0.188278182838 = 18.827818%.
- F1 = 2TP / (2TP + FP + FN) = (2 × 553) / (2 × 553 + 3357 + 708) = 0.213885128602 = 21.388513%.
- alert rate = (TP + FP) / N = (553 + 3357) / 19091 = 0.204808548531 = 20.480855%.
- prevalence = (TP + FN) / N = (553 + 708) / 19091 = 0.066052066419 = 6.605207%.

### Random Forest

- Accuracy = (TP + TN) / N = (241 + 17057) / 19091 = 0.906081399612 = 90.608140%.
- Precision = TP / (TP + FP) = 241 / (241 + 773) = 0.237672583826 = 23.767258%.
- Recall = TP / (TP + FN) = 241 / (241 + 1020) = 0.191118160190 = 19.111816%.
- Specificity = TN / (TN + FP) = 17057 / (17057 + 773) = 0.956646102075 = 95.664610%.
- FPR = FP / (FP + TN) = 773 / (773 + 17057) = 0.043353897925 = 4.335390%.
- F1 = 2TP / (2TP + FP + FN) = (2 × 241) / (2 × 241 + 773 + 1020) = 0.211868131868 = 21.186813%.
- alert rate = (TP + FP) / N = (241 + 773) / 19091 = 0.053114032790 = 5.311403%.
- prevalence = (TP + FN) / N = (241 + 1020) / 19091 = 0.066052066419 = 6.605207%.

## PHẦN 11. KIỂM CHỨNG AUC

Mọi probability score được sắp giảm dần, các score bằng nhau được gộp thành một ROC point. Diện tích từng bước: `trapezoid_area_i = (FPR_i+1 - FPR_i) × (TPR_i+1 + TPR_i) / 2`. BinaryClassificationEvaluator dùng `numBins=0` để so với đầy đủ ROC points.

### Baseline majority class

AUC_Spark = `0.5`; AUC_manual = `0.5`; độ lệch = `0`; tổng ROC points = `2`; tổng trapezoid = `1`.

| Bước | FPR_i | TPR_i | FPR_i+1 | TPR_i+1 | Trapezoid area |
| --- | --- | --- | --- | --- | --- |
| 0 | 0.000000000000 | 0.000000000000 | 1.000000000000 | 1.000000000000 | 0.5000000000000000 |

Toàn bộ bước nằm trong `outputs/tables/05_auc_trapezoids_baseline.csv`.

### Logistic Regression

AUC_Spark = `0.7018961350991807`; AUC_manual = `0.70189613509918103`; độ lệch = `3.3306690738754696e-16`; tổng ROC points = `19081`; tổng trapezoid = `19080`.

| Bước | FPR_i | TPR_i | FPR_i+1 | TPR_i+1 | Trapezoid area |
| --- | --- | --- | --- | --- | --- |
| 0 | 0.000000000000 | 0.000000000000 | 0.000056085250 | 0.000000000000 | 0.0000000000000000 |
| 1 | 0.000056085250 | 0.000000000000 | 0.000112170499 | 0.000000000000 | 0.0000000000000000 |
| 2 | 0.000112170499 | 0.000000000000 | 0.000168255749 | 0.000000000000 | 0.0000000000000000 |
| 3 | 0.000168255749 | 0.000000000000 | 0.000224340998 | 0.000000000000 | 0.0000000000000000 |
| 4 | 0.000224340998 | 0.000000000000 | 0.000224340998 | 0.000793021412 | 0.0000000000000000 |
| 5 | 0.000224340998 | 0.000793021412 | 0.000280426248 | 0.000793021412 | 0.0000000444768038 |
| 6 | 0.000280426248 | 0.000793021412 | 0.000280426248 | 0.001586042823 | 0.0000000000000000 |
| 7 | 0.000280426248 | 0.001586042823 | 0.000336511497 | 0.001586042823 | 0.0000000889536076 |
| 8 | 0.000336511497 | 0.001586042823 | 0.000392596747 | 0.001586042823 | 0.0000000889536076 |
| 9 | 0.000392596747 | 0.001586042823 | 0.000392596747 | 0.002379064235 | 0.0000000000000000 |
| 19070 | 0.999439147504 | 1.000000000000 | 0.999495232754 | 1.000000000000 | 0.0000560852495793 |
| 19071 | 0.999495232754 | 1.000000000000 | 0.999551318003 | 1.000000000000 | 0.0000560852495795 |
| 19072 | 0.999551318003 | 1.000000000000 | 0.999607403253 | 1.000000000000 | 0.0000560852495793 |
| 19073 | 0.999607403253 | 1.000000000000 | 0.999663488503 | 1.000000000000 | 0.0000560852495793 |
| 19074 | 0.999663488503 | 1.000000000000 | 0.999719573752 | 1.000000000000 | 0.0000560852495793 |
| 19075 | 0.999719573752 | 1.000000000000 | 0.999775659002 | 1.000000000000 | 0.0000560852495793 |
| 19076 | 0.999775659002 | 1.000000000000 | 0.999831744251 | 1.000000000000 | 0.0000560852495793 |
| 19077 | 0.999831744251 | 1.000000000000 | 0.999887829501 | 1.000000000000 | 0.0000560852495795 |
| 19078 | 0.999887829501 | 1.000000000000 | 0.999943914750 | 1.000000000000 | 0.0000560852495793 |
| 19079 | 0.999943914750 | 1.000000000000 | 1.000000000000 | 1.000000000000 | 0.0000560852495793 |

Toàn bộ bước nằm trong `outputs/tables/05_auc_trapezoids_logistic_regression.csv`.

### Random Forest

AUC_Spark = `0.68728405955799809`; AUC_manual = `0.68728405955799843`; độ lệch = `3.3306690738754696e-16`; tổng ROC points = `7104`; tổng trapezoid = `7103`.

| Bước | FPR_i | TPR_i | FPR_i+1 | TPR_i+1 | Trapezoid area |
| --- | --- | --- | --- | --- | --- |
| 0 | 0.000000000000 | 0.000000000000 | 0.000056085250 | 0.000000000000 | 0.0000000000000000 |
| 1 | 0.000056085250 | 0.000000000000 | 0.000056085250 | 0.000793021412 | 0.0000000000000000 |
| 2 | 0.000056085250 | 0.000793021412 | 0.000056085250 | 0.001586042823 | 0.0000000000000000 |
| 3 | 0.000056085250 | 0.001586042823 | 0.000056085250 | 0.002379064235 | 0.0000000000000000 |
| 4 | 0.000056085250 | 0.002379064235 | 0.000056085250 | 0.003172085646 | 0.0000000000000000 |
| 5 | 0.000056085250 | 0.003172085646 | 0.000112170499 | 0.003172085646 | 0.0000001779072152 |
| 6 | 0.000112170499 | 0.003172085646 | 0.000112170499 | 0.003965107058 | 0.0000000000000000 |
| 7 | 0.000112170499 | 0.003965107058 | 0.000168255749 | 0.003965107058 | 0.0000002223840190 |
| 8 | 0.000168255749 | 0.003965107058 | 0.000224340998 | 0.003965107058 | 0.0000002223840190 |
| 9 | 0.000224340998 | 0.003965107058 | 0.000280426248 | 0.003965107058 | 0.0000002223840190 |
| 7093 | 0.999270891755 | 1.000000000000 | 0.999495232754 | 1.000000000000 | 0.0002243409983174 |
| 7094 | 0.999495232754 | 1.000000000000 | 0.999551318003 | 1.000000000000 | 0.0000560852495795 |
| 7095 | 0.999551318003 | 1.000000000000 | 0.999607403253 | 1.000000000000 | 0.0000560852495793 |
| 7096 | 0.999607403253 | 1.000000000000 | 0.999663488503 | 1.000000000000 | 0.0000560852495793 |
| 7097 | 0.999663488503 | 1.000000000000 | 0.999719573752 | 1.000000000000 | 0.0000560852495793 |
| 7098 | 0.999719573752 | 1.000000000000 | 0.999775659002 | 1.000000000000 | 0.0000560852495793 |
| 7099 | 0.999775659002 | 1.000000000000 | 0.999831744251 | 1.000000000000 | 0.0000560852495793 |
| 7100 | 0.999831744251 | 1.000000000000 | 0.999887829501 | 1.000000000000 | 0.0000560852495795 |
| 7101 | 0.999887829501 | 1.000000000000 | 0.999943914750 | 1.000000000000 | 0.0000560852495793 |
| 7102 | 0.999943914750 | 1.000000000000 | 1.000000000000 | 1.000000000000 | 0.0000560852495793 |

Toàn bộ bước nằm trong `outputs/tables/05_auc_trapezoids_random_forest.csv`.

## PHẦN 12. SO SÁNH BASELINE VÀ HAI MODEL TRÊN CÙNG TEST SPLIT

| Phương pháp | Threshold | TP | TN | FP | FN | Accuracy | Precision | Recall | Specificity | FPR | F1 | AUC | Alert rate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Baseline majority class | Không áp dụng | 0 | 17830 | 0 | 1261 | 93.394793% | 0.000000% | 0.000000% | 100.000000% | 0.000000% | 0.000000% | 0.500000 | 0.000000% |
| Logistic Regression | 0.094 | 553 | 14473 | 3357 | 708 | 78.707244% | 14.143223% | 43.854084% | 81.172182% | 18.827818% | 21.388513% | 0.701896 | 20.480855% |
| Random Forest | 0.094 | 241 | 17057 | 773 | 1020 | 90.608140% | 23.767258% | 19.111816% | 95.664610% | 4.335390% | 21.186813% | 0.687284 | 5.311403% |

Baseline có thể đạt Accuracy cao do class imbalance nhưng Recall và F1 bằng 0 khi không cảnh báo order late nào. Vì vậy không được kết luận phương pháp tốt hơn chỉ dựa trên Accuracy.

## PHẦN 13. BỐN ORDER THẬT A, B, C, D

### A — `f46b842d9b4dfd29acf5eec998837ede`

Quy tắc: True Positive có probability_late cao nhất. Thay thế: không.

| Feature gốc | Giá trị thật |
| --- | --- |
| customer_state | PI |
| main_seller_state | MA |
| main_category | health_beauty |
| item_count | 1 |
| product_count | 1 |
| seller_count | 1 |
| total_price | 122.99 |
| total_freight | 17.05 |
| average_item_price | 122.99 |
| total_weight_g | 700 |
| total_volume_cm3 | 5700 |
| freight_ratio | 0.1386 |
| purchase_year | 2018 |
| purchase_month | 7 |
| purchase_day_of_week | 5 |
| purchase_hour | 22 |
| estimated_delivery_days | 21 |
| customer_seller_same_state | 0 |

| Label | LR probability | RF probability | Common threshold | LR prediction | RF prediction | LR result | RF result | Vị trí model demo |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 0.3979893189148693 | 0.1335057242660968 | 0.094 | 1 | 1 | TP | TP | bằng hoặc cao hơn common threshold |

Phép tính Logistic Regression đầy đủ nằm ở PHẦN 6; phép tính Random Forest và 30 leaf index nằm ở PHẦN 7. Các bảng CSV breakdown chứa toàn bộ dòng máy đọc được.

### B — `686c0ba20be3837a5041edbc39d3f9ae`

Quy tắc: False Positive có probability_late cao nhất. Thay thế: không.

| Feature gốc | Giá trị thật |
| --- | --- |
| customer_state | MA |
| main_seller_state | AM |
| main_category | telephony |
| item_count | 1 |
| product_count | 1 |
| seller_count | 1 |
| total_price | 114.0 |
| total_freight | 21.25 |
| average_item_price | 114.0 |
| total_weight_g | 350 |
| total_volume_cm3 | 2040 |
| freight_ratio | 0.1864 |
| purchase_year | 2017 |
| purchase_month | 3 |
| purchase_day_of_week | 1 |
| purchase_hour | 20 |
| estimated_delivery_days | 30 |
| customer_seller_same_state | 0 |

| Label | LR probability | RF probability | Common threshold | LR prediction | RF prediction | LR result | RF result | Vị trí model demo |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0.7394476565195455 | 0.1288351675102899 | 0.094 | 1 | 1 | FP | FP | bằng hoặc cao hơn common threshold |

Phép tính Logistic Regression đầy đủ nằm ở PHẦN 6; phép tính Random Forest và 30 leaf index nằm ở PHẦN 7. Các bảng CSV breakdown chứa toàn bộ dòng máy đọc được.

### C — `9b1d71b20edcf15ab15e0bb4a932f23f`

Quy tắc: False Negative gần common threshold nhất ở phía dưới. Thay thế: không.

| Feature gốc | Giá trị thật |
| --- | --- |
| customer_state | DF |
| main_seller_state | SP |
| main_category | audio |
| item_count | 1 |
| product_count | 1 |
| seller_count | 1 |
| total_price | 79.0 |
| total_freight | 16.31 |
| average_item_price | 79.0 |
| total_weight_g | 675 |
| total_volume_cm3 | 3168 |
| freight_ratio | 0.2065 |
| purchase_year | 2017 |
| purchase_month | 9 |
| purchase_day_of_week | 1 |
| purchase_hour | 9 |
| estimated_delivery_days | 19 |
| customer_seller_same_state | 0 |

| Label | LR probability | RF probability | Common threshold | LR prediction | RF prediction | LR result | RF result | Vị trí model demo |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 0.0939957180102402 | 0.0622549845129765 | 0.094 | 0 | 0 | FN | FN | thấp hơn common threshold |

Phép tính Logistic Regression đầy đủ nằm ở PHẦN 6; phép tính Random Forest và 30 leaf index nằm ở PHẦN 7. Các bảng CSV breakdown chứa toàn bộ dòng máy đọc được.

### D — `c2bb89b5c1dd978d507284be78a04cb2`

Quy tắc: True Negative có probability_late thấp nhất. Thay thế: không.

| Feature gốc | Giá trị thật |
| --- | --- |
| customer_state | SP |
| main_seller_state | MG |
| main_category | housewares |
| item_count | 2 |
| product_count | 1 |
| seller_count | 1 |
| total_price | 199.98 |
| total_freight | 122.88 |
| average_item_price | 99.99 |
| total_weight_g | 30000 |
| total_volume_cm3 | 52500 |
| freight_ratio | 0.6145 |
| purchase_year | 2017 |
| purchase_month | 5 |
| purchase_day_of_week | 3 |
| purchase_hour | 22 |
| estimated_delivery_days | 141 |
| customer_seller_same_state | 0 |

| Label | LR probability | RF probability | Common threshold | LR prediction | RF prediction | LR result | RF result | Vị trí model demo |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0.0001448519905773 | 0.0549342613898836 | 0.094 | 0 | 0 | TN | TN | thấp hơn common threshold |

Phép tính Logistic Regression đầy đủ nằm ở PHẦN 6; phép tính Random Forest và 30 leaf index nằm ở PHẦN 7. Các bảng CSV breakdown chứa toàn bộ dòng máy đọc được.

## PHẦN 14. MODEL ĐƯỢC CHỌN CHO DEMO

Quy tắc tại common threshold trên validation: F1 cao hơn; nếu bằng nhau thì Recall cao hơn; tiếp theo Precision cao hơn; tiếp theo AUC cao hơn; cuối cùng dùng thứ tự tên cố định để bảo đảm tái lập.

| Model | F1 | Recall | Precision | AUC | Alert rate |
| --- | --- | --- | --- | --- | --- |
| Logistic Regression | 0.223906135419 | 0.437440305635 | 0.150459921156 | 0.695851 | 19.862969% |
| Random Forest | 0.220057306590 | 0.183381088825 | 0.275071633238 | 0.684502 | 4.554649% |

Model được khóa cho demo là **Logistic Regression**, common threshold **0.094**. Quyết định này chỉ dùng validation, không dùng test. Demo chỉ đưa order vào nhóm ưu tiên kiểm tra; không khẳng định order chắc chắn giao trễ.

## PHẦN 15. HẠN CHẾ

- Dataset có class imbalance; prevalence của class late chỉ là 6.773090%.
- Precision có thể thấp khi cân bằng Precision và Recall bằng F1 dưới giới hạn alert rate.
- Dataset thiếu một số dữ liệu logistics vận hành có thể ảnh hưởng trực tiếp tới giao hàng.
- Probability score chưa chắc là calibrated probability vì quy trình chưa thực hiện probability calibration.
- Common threshold là quy ước so sánh chung cho hai model, không phải threshold tối ưu tuyệt đối cho mọi dataset.
- Kết quả quan sát không chứng minh quan hệ nhân quả.
- Không dùng model để tự động thông báo khách hàng; score chỉ hỗ trợ ưu tiên kiểm tra nội bộ.

## PHẦN 16. KẾT LUẬN

Baseline majority class dự đoán cố định label `0` trên test, đạt Accuracy `0.933948` nhưng Recall `0.000000`, F1 `0.000000` và AUC `0.500000`. Lần chạy này khóa common threshold `0.094` từ validation. Logistic Regression đạt F1 `0.213885`, Recall `0.438541`, Precision `0.141432`, AUC `0.701896`; Random Forest đạt F1 `0.211868`, Recall `0.191118`, Precision `0.237673`, AUC `0.687284`. Hai model có Recall/F1/AUC cao hơn baseline, dù Accuracy có thể thấp hơn do đã phát sinh cảnh báo. Các nhận định chỉ áp dụng cho data split, feature, cấu hình và quy tắc threshold được ghi trong báo cáo này.

# PHỤ LỤC

## A. Toàn bộ Logistic Regression coefficients

| Index | Transformed feature | Coefficient |
| --- | --- | --- |
| 0 | item_count_filled | -0.0728159701318056 |
| 1 | product_count_filled | -0.2436242860374153 |
| 2 | seller_count_filled | -0.8792444875416756 |
| 3 | total_price_filled | -0.0000509402828405 |
| 4 | total_freight_filled | 0.0002663690733723 |
| 5 | average_item_price_filled | 0.0003734099167293 |
| 6 | total_weight_g_filled | 0.0000119625389294 |
| 7 | total_volume_cm3_filled | 0.0000012205708204 |
| 8 | freight_ratio_filled | -0.0290712534134062 |
| 9 | purchase_year_filled | 0.1816513880658075 |
| 10 | purchase_month_filled | -0.0370578137239926 |
| 11 | purchase_day_of_week_filled | 0.0127842712277946 |
| 12 | purchase_hour_filled | 0.0057187616629516 |
| 13 | estimated_delivery_days_filled | -0.0492311382182447 |
| 14 | customer_seller_same_state_filled | -0.5807172667005529 |
| 15 | customer_state_ohe_SP | -0.3735134168689600 |
| 16 | customer_state_ohe_RJ | 0.5710734116339560 |
| 17 | customer_state_ohe_MG | -0.3995234276708031 |
| 18 | customer_state_ohe_RS | -0.0046755299331931 |
| 19 | customer_state_ohe_PR | -0.5005240076942024 |
| 20 | customer_state_ohe_SC | 0.1728781728044807 |
| 21 | customer_state_ohe_BA | 0.7336239201067468 |
| 22 | customer_state_ohe_DF | -0.2765665461519764 |
| 23 | customer_state_ohe_ES | 0.3688767539160139 |
| 24 | customer_state_ohe_GO | 0.0005842233254661 |
| 25 | customer_state_ohe_PE | 0.5254016663786383 |
| 26 | customer_state_ohe_CE | 0.9579962028314398 |
| 27 | customer_state_ohe_PA | 0.8909918841477619 |
| 28 | customer_state_ohe_MT | 0.0746552877791018 |
| 29 | customer_state_ohe_MA | 1.1577666353520508 |
| 30 | customer_state_ohe_MS | 0.2733674811605025 |
| 31 | customer_state_ohe_PB | 0.6496602429997137 |
| 32 | customer_state_ohe_PI | 0.7964059696512064 |
| 33 | customer_state_ohe_RN | 0.5624528942309469 |
| 34 | customer_state_ohe_AL | 1.4430432722591877 |
| 35 | customer_state_ohe_SE | 0.9767258484063803 |
| 36 | customer_state_ohe_TO | 0.4879499318421651 |
| 37 | customer_state_ohe_RO | -0.1110653306292368 |
| 38 | customer_state_ohe_AM | -0.1601611942098955 |
| 39 | customer_state_ohe_AC | -0.1183202561430085 |
| 40 | customer_state_ohe_AP | 0.1998021884194558 |
| 41 | customer_state_ohe_RR | 1.1989196754423321 |
| 42 | customer_state_ohe___unknown | 0.0000000000000000 |
| 43 | main_seller_state_ohe_SP | 0.2038061232285540 |
| 44 | main_seller_state_ohe_MG | -0.2283490362824515 |
| 45 | main_seller_state_ohe_PR | -0.0898308659238170 |
| 46 | main_seller_state_ohe_RJ | 0.0166892772679467 |
| 47 | main_seller_state_ohe_SC | -0.1321573984085392 |
| 48 | main_seller_state_ohe_RS | -0.4007651159531262 |
| 49 | main_seller_state_ohe_DF | -0.1671148457326668 |
| 50 | main_seller_state_ohe_BA | -0.3842772526409082 |
| 51 | main_seller_state_ohe_GO | -0.6006652221394472 |
| 52 | main_seller_state_ohe_PE | -0.7052045730053843 |
| 53 | main_seller_state_ohe_MA | 1.1476019106146522 |
| 54 | main_seller_state_ohe_ES | -0.0653681872095911 |
| 55 | main_seller_state_ohe_MT | -0.2850513138536655 |
| 56 | main_seller_state_ohe_CE | -0.9298383887509989 |
| 57 | main_seller_state_ohe_MS | -0.1893730001138626 |
| 58 | main_seller_state_ohe_RN | 0.3275715222775277 |
| 59 | main_seller_state_ohe_PB | -0.7638438459307757 |
| 60 | main_seller_state_ohe_RO | -1.2318708383223536 |
| 61 | main_seller_state_ohe_PI | -1.8995231792136000 |
| 62 | main_seller_state_ohe_PA | -1.2617977005650489 |
| 63 | main_seller_state_ohe_SE | -1.4288165054268738 |
| 64 | main_seller_state_ohe_AM | 2.7811139752492888 |
| 65 | main_seller_state_ohe___unknown | 0.0000000000000000 |
| 66 | main_category_ohe_bed_bath_table | 0.1919777635412221 |
| 67 | main_category_ohe_health_beauty | -0.0038540879090342 |
| 68 | main_category_ohe_sports_leisure | 0.0053152608607145 |
| 69 | main_category_ohe_computers_accessories | 0.0754051437736786 |
| 70 | main_category_ohe_furniture_decor | 0.1143279574911856 |
| 71 | main_category_ohe_housewares | -0.1795997184326425 |
| 72 | main_category_ohe_watches_gifts | -0.0171809173659974 |
| 73 | main_category_ohe_telephony | 0.0091219442711533 |
| 74 | main_category_ohe_auto | 0.0152381941384413 |
| 75 | main_category_ohe_toys | 0.0061277203308880 |
| 76 | main_category_ohe_cool_stuff | -0.1857455917756038 |
| 77 | main_category_ohe_garden_tools | 0.0464693086719049 |
| 78 | main_category_ohe_perfumery | 0.0054305158902073 |
| 79 | main_category_ohe_baby | 0.1917838903228999 |
| 80 | main_category_ohe_electronics | 0.0059891877801082 |
| 81 | main_category_ohe_stationery | -0.0094684679252110 |
| 82 | main_category_ohe_fashion_bags_accessories | -0.1019402056305955 |
| 83 | main_category_ohe_pet_shop | -0.1262163176897347 |
| 84 | main_category_ohe_khong_xac_dinh | 0.0146682786535751 |
| 85 | main_category_ohe_office_furniture | 0.1380053542030373 |
| 86 | main_category_ohe_luggage_accessories | -0.4733289787591641 |
| 87 | main_category_ohe_consoles_games | 0.0700322197325316 |
| 88 | main_category_ohe_home_appliances | -0.3554197970818386 |
| 89 | main_category_ohe_construction_tools_construction | -0.1525935467815309 |
| 90 | main_category_ohe_musical_instruments | 0.0004703944811955 |
| 91 | main_category_ohe_small_appliances | -0.3039814303327135 |
| 92 | main_category_ohe_books_general_interest | 0.0470671686565745 |
| 93 | main_category_ohe_home_construction | -0.1547365704029784 |
| 94 | main_category_ohe_food | 0.0250649392258544 |
| 95 | main_category_ohe_furniture_living_room | -0.0052232456368695 |
| 96 | main_category_ohe_home_confort | 0.3631657125181724 |
| 97 | main_category_ohe_audio | 0.4635011957442255 |
| 98 | main_category_ohe_drinks | -0.2245917061644129 |
| 99 | main_category_ohe_market_place | -0.3729986118824336 |
| 100 | main_category_ohe_books_technical | 0.0343983443692313 |
| 101 | main_category_ohe_air_conditioning | -0.3780467337176142 |
| 102 | main_category_ohe_kitchen_dining_laundry_garden_furniture | -0.1893347304680681 |
| 103 | main_category_ohe_construction_tools_lights | -0.0489625464743917 |
| 104 | main_category_ohe_fashion_shoes | 0.0032815422775955 |
| 105 | main_category_ohe_industry_commerce_and_business | -0.2178444866893242 |
| 106 | main_category_ohe_food_drink | -0.5132183000405866 |
| 107 | main_category_ohe_home_appliances_2 | -0.0731218979823773 |
| 108 | main_category_ohe_fixed_telephony | -0.0498991076370915 |
| 109 | main_category_ohe_costruction_tools_garden | -0.2824424046085227 |
| 110 | main_category_ohe_art | -0.0418533202434145 |
| 111 | main_category_ohe_computers | -0.4568374689175472 |
| 112 | main_category_ohe_agro_industry_and_commerce | -0.5062215652761134 |
| 113 | main_category_ohe_construction_tools_safety | -0.6109845259505168 |
| 114 | main_category_ohe_signaling_and_security | -0.2758954715570681 |
| 115 | main_category_ohe_fashion_underwear_beach | 0.3436632165595292 |
| 116 | main_category_ohe_christmas_supplies | 0.4498624901761306 |
| 117 | main_category_ohe_fashion_male_clothing | 0.1477957160989871 |
| 118 | main_category_ohe_costruction_tools_tools | -0.3924865611108169 |
| 119 | main_category_ohe_furniture_bedroom | 0.1275015386449367 |
| 120 | main_category_ohe_tablets_printing_image | -0.3173478981015334 |
| 121 | main_category_ohe_cine_photo | -0.1625014246435127 |
| 122 | main_category_ohe_small_appliances_home_oven_and_coffee | -0.2648276914046671 |
| 123 | main_category_ohe_dvds_blu_ray | 0.7919334872519282 |
| 124 | main_category_ohe_books_imported | -0.2884731083570147 |
| 125 | main_category_ohe_party_supplies | -1.5290826410502636 |
| 126 | main_category_ohe_furniture_mattress_and_upholstery | 0.6089639191508253 |
| 127 | main_category_ohe_music | -0.0438233820069232 |
| 128 | main_category_ohe_fashio_female_clothing | 0.3971552112574074 |
| 129 | main_category_ohe_flowers | -1.6824901515974289 |
| 130 | main_category_ohe_arts_and_craftmanship | -0.2414464584770298 |
| 131 | main_category_ohe_fashion_sport | -0.2759789538614862 |
| 132 | main_category_ohe_home_comfort_2 | 1.0106743966841307 |
| 133 | main_category_ohe_diapers_and_hygiene | -1.4629642118311532 |
| 134 | main_category_ohe_la_cuisine | -1.3780873155375848 |
| 135 | main_category_ohe_portateis_cozinha_e_preparadores_de_alimentos | 0.3059515789444776 |
| 136 | main_category_ohe_cds_dvds_musicals | -1.4610350472914715 |
| 137 | main_category_ohe_fashion_childrens_clothes | -1.5501957162924078 |
| 138 | main_category_ohe_pc_gamer | -1.7459591385856210 |
| 139 | main_category_ohe_security_and_services | -1.2909384831230626 |
| 140 | main_category_ohe___unknown | 0.0000000000000000 |

## B. Toàn bộ Random Forest feature importance

| Index | Transformed feature | Feature importance |
| --- | --- | --- |
| 0 | item_count_filled | 0.0050646227353857 |
| 1 | product_count_filled | 0.0042832371586910 |
| 2 | seller_count_filled | 0.0029538074213928 |
| 3 | total_price_filled | 0.0144250244560203 |
| 4 | total_freight_filled | 0.0365693509460958 |
| 5 | average_item_price_filled | 0.0202540421360239 |
| 6 | total_weight_g_filled | 0.0118126360058513 |
| 7 | total_volume_cm3_filled | 0.0111023636917574 |
| 8 | freight_ratio_filled | 0.0141410175000053 |
| 9 | purchase_year_filled | 0.0349696895504907 |
| 10 | purchase_month_filled | 0.2424953863622225 |
| 11 | purchase_day_of_week_filled | 0.0094720232646159 |
| 12 | purchase_hour_filled | 0.0126290690835150 |
| 13 | estimated_delivery_days_filled | 0.0839576849239208 |
| 14 | customer_seller_same_state_filled | 0.0507789644621848 |
| 15 | customer_state_ohe_SP | 0.0519358056797792 |
| 16 | customer_state_ohe_RJ | 0.1041753475693115 |
| 17 | customer_state_ohe_MG | 0.0116162552859363 |
| 18 | customer_state_ohe_RS | 0.0000000000000000 |
| 19 | customer_state_ohe_PR | 0.0072089869843339 |
| 20 | customer_state_ohe_SC | 0.0007217266206324 |
| 21 | customer_state_ohe_BA | 0.0188825653687203 |
| 22 | customer_state_ohe_DF | 0.0003863536771469 |
| 23 | customer_state_ohe_ES | 0.0115007588743669 |
| 24 | customer_state_ohe_GO | 0.0003376058479920 |
| 25 | customer_state_ohe_PE | 0.0064031201050797 |
| 26 | customer_state_ohe_CE | 0.0262255158503718 |
| 27 | customer_state_ohe_PA | 0.0016667878980710 |
| 28 | customer_state_ohe_MT | 0.0000176768573516 |
| 29 | customer_state_ohe_MA | 0.0256577662371642 |
| 30 | customer_state_ohe_MS | 0.0000000000000000 |
| 31 | customer_state_ohe_PB | 0.0008432599369483 |
| 32 | customer_state_ohe_PI | 0.0006468208159073 |
| 33 | customer_state_ohe_RN | 0.0000000000000000 |
| 34 | customer_state_ohe_AL | 0.0158427291134006 |
| 35 | customer_state_ohe_SE | 0.0025835444199198 |
| 36 | customer_state_ohe_TO | 0.0000000000000000 |
| 37 | customer_state_ohe_RO | 0.0000000000000000 |
| 38 | customer_state_ohe_AM | 0.0000000000000000 |
| 39 | customer_state_ohe_AC | 0.0000000000000000 |
| 40 | customer_state_ohe_AP | 0.0000000000000000 |
| 41 | customer_state_ohe_RR | 0.0005347390386801 |
| 42 | customer_state_ohe___unknown | 0.0000000000000000 |
| 43 | main_seller_state_ohe_SP | 0.0279956793040533 |
| 44 | main_seller_state_ohe_MG | 0.0044558282202477 |
| 45 | main_seller_state_ohe_PR | 0.0061190062919849 |
| 46 | main_seller_state_ohe_RJ | 0.0054175550241733 |
| 47 | main_seller_state_ohe_SC | 0.0012212614797786 |
| 48 | main_seller_state_ohe_RS | 0.0017144082812605 |
| 49 | main_seller_state_ohe_DF | 0.0002543346627876 |
| 50 | main_seller_state_ohe_BA | 0.0002796719413803 |
| 51 | main_seller_state_ohe_GO | 0.0001291743905657 |
| 52 | main_seller_state_ohe_PE | 0.0000000000000000 |
| 53 | main_seller_state_ohe_MA | 0.0230361692116746 |
| 54 | main_seller_state_ohe_ES | 0.0000000000000000 |
| 55 | main_seller_state_ohe_MT | 0.0016860408209146 |
| 56 | main_seller_state_ohe_CE | 0.0000000000000000 |
| 57 | main_seller_state_ohe_MS | 0.0000000000000000 |
| 58 | main_seller_state_ohe_RN | 0.0000505239086512 |
| 59 | main_seller_state_ohe_PB | 0.0006019687762016 |
| 60 | main_seller_state_ohe_RO | 0.0000000000000000 |
| 61 | main_seller_state_ohe_PI | 0.0000000000000000 |
| 62 | main_seller_state_ohe_PA | 0.0000000000000000 |
| 63 | main_seller_state_ohe_SE | 0.0000000000000000 |
| 64 | main_seller_state_ohe_AM | 0.0025965314065828 |
| 65 | main_seller_state_ohe___unknown | 0.0000000000000000 |
| 66 | main_category_ohe_bed_bath_table | 0.0013499337822798 |
| 67 | main_category_ohe_health_beauty | 0.0024740398489063 |
| 68 | main_category_ohe_sports_leisure | 0.0037578675355428 |
| 69 | main_category_ohe_computers_accessories | 0.0000000000000000 |
| 70 | main_category_ohe_furniture_decor | 0.0014201186735282 |
| 71 | main_category_ohe_housewares | 0.0045214987254041 |
| 72 | main_category_ohe_watches_gifts | 0.0005578972635343 |
| 73 | main_category_ohe_telephony | 0.0015380570126052 |
| 74 | main_category_ohe_auto | 0.0009494066446624 |
| 75 | main_category_ohe_toys | 0.0014184151106747 |
| 76 | main_category_ohe_cool_stuff | 0.0021678043543160 |
| 77 | main_category_ohe_garden_tools | 0.0000000000000000 |
| 78 | main_category_ohe_perfumery | 0.0001172068749018 |
| 79 | main_category_ohe_baby | 0.0021190338263406 |
| 80 | main_category_ohe_electronics | 0.0006783173978223 |
| 81 | main_category_ohe_stationery | 0.0004565173781002 |
| 82 | main_category_ohe_fashion_bags_accessories | 0.0020906932851160 |
| 83 | main_category_ohe_pet_shop | 0.0003111402226853 |
| 84 | main_category_ohe_khong_xac_dinh | 0.0025784869705454 |
| 85 | main_category_ohe_office_furniture | 0.0003162909430999 |
| 86 | main_category_ohe_luggage_accessories | 0.0001027118089607 |
| 87 | main_category_ohe_consoles_games | 0.0009543779259234 |
| 88 | main_category_ohe_home_appliances | 0.0021304051550582 |
| 89 | main_category_ohe_construction_tools_construction | 0.0000000000000000 |
| 90 | main_category_ohe_musical_instruments | 0.0000776948079460 |
| 91 | main_category_ohe_small_appliances | 0.0003054736932277 |
| 92 | main_category_ohe_books_general_interest | 0.0027543929571394 |
| 93 | main_category_ohe_home_construction | 0.0008520930853690 |
| 94 | main_category_ohe_food | 0.0003574363597895 |
| 95 | main_category_ohe_furniture_living_room | 0.0015314436685547 |
| 96 | main_category_ohe_home_confort | 0.0032478596030066 |
| 97 | main_category_ohe_audio | 0.0028432277265900 |
| 98 | main_category_ohe_drinks | 0.0000000000000000 |
| 99 | main_category_ohe_market_place | 0.0000000000000000 |
| 100 | main_category_ohe_books_technical | 0.0037822583451731 |
| 101 | main_category_ohe_air_conditioning | 0.0008402629726531 |
| 102 | main_category_ohe_kitchen_dining_laundry_garden_furniture | 0.0007552715081676 |
| 103 | main_category_ohe_construction_tools_lights | 0.0018982816422541 |
| 104 | main_category_ohe_fashion_shoes | 0.0000000000000000 |
| 105 | main_category_ohe_industry_commerce_and_business | 0.0021598193565216 |
| 106 | main_category_ohe_food_drink | 0.0000770257225036 |
| 107 | main_category_ohe_home_appliances_2 | 0.0018299578904423 |
| 108 | main_category_ohe_fixed_telephony | 0.0004188257700047 |
| 109 | main_category_ohe_costruction_tools_garden | 0.0037826980641810 |
| 110 | main_category_ohe_art | 0.0000000000000000 |
| 111 | main_category_ohe_computers | 0.0013292212011037 |
| 112 | main_category_ohe_agro_industry_and_commerce | 0.0006021020651758 |
| 113 | main_category_ohe_construction_tools_safety | 0.0000000000000000 |
| 114 | main_category_ohe_signaling_and_security | 0.0077965121674431 |
| 115 | main_category_ohe_fashion_underwear_beach | 0.0011854469788165 |
| 116 | main_category_ohe_christmas_supplies | 0.0016892269625734 |
| 117 | main_category_ohe_fashion_male_clothing | 0.0000000000000000 |
| 118 | main_category_ohe_costruction_tools_tools | 0.0000000000000000 |
| 119 | main_category_ohe_furniture_bedroom | 0.0007958657043941 |
| 120 | main_category_ohe_tablets_printing_image | 0.0000000000000000 |
| 121 | main_category_ohe_cine_photo | 0.0002438607981668 |
| 122 | main_category_ohe_small_appliances_home_oven_and_coffee | 0.0032761399615238 |
| 123 | main_category_ohe_dvds_blu_ray | 0.0007962702273429 |
| 124 | main_category_ohe_books_imported | 0.0000000000000000 |
| 125 | main_category_ohe_party_supplies | 0.0000000000000000 |
| 126 | main_category_ohe_furniture_mattress_and_upholstery | 0.0011220401769650 |
| 127 | main_category_ohe_music | 0.0035356811870785 |
| 128 | main_category_ohe_fashio_female_clothing | 0.0000000000000000 |
| 129 | main_category_ohe_flowers | 0.0000000000000000 |
| 130 | main_category_ohe_arts_and_craftmanship | 0.0000000000000000 |
| 131 | main_category_ohe_fashion_sport | 0.0000000000000000 |
| 132 | main_category_ohe_home_comfort_2 | 0.0000000000000000 |
| 133 | main_category_ohe_diapers_and_hygiene | 0.0000000000000000 |
| 134 | main_category_ohe_la_cuisine | 0.0000000000000000 |
| 135 | main_category_ohe_portateis_cozinha_e_preparadores_de_alimentos | 0.0004489490563415 |
| 136 | main_category_ohe_cds_dvds_musicals | 0.0000000000000000 |
| 137 | main_category_ohe_fashion_childrens_clothes | 0.0000000000000000 |
| 138 | main_category_ohe_pc_gamer | 0.0000000000000000 |
| 139 | main_category_ohe_security_and_services | 0.0000000000000000 |
| 140 | main_category_ohe___unknown | 0.0000000000000000 |

## C. Tree details và model.toDebugString

| Tree | Weight | Depth | Num nodes |
| --- | --- | --- | --- |
| 0 | 1.000000000000 | 6 | 43 |
| 1 | 1.000000000000 | 6 | 29 |
| 2 | 1.000000000000 | 6 | 23 |
| 3 | 1.000000000000 | 6 | 49 |
| 4 | 1.000000000000 | 6 | 53 |
| 5 | 1.000000000000 | 6 | 45 |
| 6 | 1.000000000000 | 6 | 49 |
| 7 | 1.000000000000 | 6 | 47 |
| 8 | 1.000000000000 | 6 | 67 |
| 9 | 1.000000000000 | 6 | 21 |
| 10 | 1.000000000000 | 6 | 31 |
| 11 | 1.000000000000 | 6 | 49 |
| 12 | 1.000000000000 | 6 | 37 |
| 13 | 1.000000000000 | 6 | 31 |
| 14 | 1.000000000000 | 6 | 43 |
| 15 | 1.000000000000 | 6 | 35 |
| 16 | 1.000000000000 | 6 | 53 |
| 17 | 1.000000000000 | 6 | 43 |
| 18 | 1.000000000000 | 6 | 53 |
| 19 | 1.000000000000 | 6 | 35 |
| 20 | 1.000000000000 | 6 | 27 |
| 21 | 1.000000000000 | 6 | 45 |
| 22 | 1.000000000000 | 6 | 39 |
| 23 | 1.000000000000 | 6 | 25 |
| 24 | 1.000000000000 | 6 | 41 |
| 25 | 1.000000000000 | 6 | 23 |
| 26 | 1.000000000000 | 6 | 29 |
| 27 | 1.000000000000 | 6 | 53 |
| 28 | 1.000000000000 | 6 | 39 |
| 29 | 1.000000000000 | 6 | 33 |

```text
RandomForestClassificationModel: uid=RandomForestClassifier_181fa93252ee, numTrees=30, numClasses=2, numFeatures=141
  Tree 0 (weight 1.0):
    If (feature 3 <= 226.985)
     If (feature 23 in {1.0})
      If (feature 43 in {0.0})
       Predict: 0.0
      Else (feature 43 not in {0.0})
       If (feature 74 in {1.0})
        If (feature 11 <= 2.5)
         If (feature 8 <= 0.40765)
          Predict: 0.0
         Else (feature 8 > 0.40765)
          Predict: 1.0
        Else (feature 11 > 2.5)
         Predict: 0.0
       Else (feature 74 not in {1.0})
        If (feature 71 in {1.0})
         If (feature 10 <= 3.5)
          Predict: 1.0
         Else (feature 10 > 3.5)
          Predict: 0.0
        Else (feature 71 not in {1.0})
         Predict: 0.0
     Else (feature 23 not in {1.0})
      If (feature 26 in {1.0})
       If (feature 109 in {1.0})
        Predict: 1.0
       Else (feature 109 not in {1.0})
        Predict: 0.0
      Else (feature 26 not in {1.0})
       Predict: 0.0
    Else (feature 3 > 226.985)
     If (feature 4 <= 16.145)
      If (feature 78 in {1.0})
       If (feature 11 <= 6.5)
        If (feature 16 in {1.0})
         Predict: 0.0
        Else (feature 16 not in {1.0})
         If (feature 6 <= 199.5)
          Predict: 1.0
         Else (feature 6 > 199.5)
          Predict: 0.0
       Else (feature 11 > 6.5)
        If (feature 12 <= 14.5)
         Predict: 0.0
        Else (feature 12 > 14.5)
         If (feature 3 <= 275.45)
          Predict: 0.0
         Else (feature 3 > 275.45)
          Predict: 1.0
      Else (feature 78 not in {1.0})
       If (feature 26 in {1.0})
        Predict: 1.0
       Else (feature 26 not in {1.0})
        Predict: 0.0
     Else (feature 4 > 16.145)
      If (feature 16 in {1.0})
       Predict: 0.0
      Else (feature 16 not in {1.0})
       If (feature 29 in {1.0})
        Predict: 0.0
       Else (feature 29 not in {1.0})
        If (feature 64 in {1.0})
         Predict: 1.0
        Else (feature 64 not in {1.0})
         Predict: 0.0
  Tree 1 (weight 1.0):
    If (feature 14 <= 0.5)
     If (feature 6 <= 13825.0)
      If (feature 43 in {0.0})
       Predict: 0.0
      Else (feature 43 not in {0.0})
       If (feature 5 <= 249.325)
        If (feature 22 in {1.0})
         If (feature 105 in {1.0})
          Predict: 1.0
         Else (feature 105 not in {1.0})
          Predict: 0.0
        Else (feature 22 not in {1.0})
         Predict: 0.0
       Else (feature 5 > 249.325)
        Predict: 0.0
     Else (feature 6 > 13825.0)
      Predict: 0.0
    Else (feature 14 > 0.5)
     If (feature 29 in {1.0})
      If (feature 4 <= 10.135000000000002)
       Predict: 0.0
      Else (feature 4 > 10.135000000000002)
       If (feature 4 <= 14.105)
        Predict: 1.0
       Else (feature 4 > 14.105)
        If (feature 10 <= 3.5)
         Predict: 1.0
        Else (feature 10 > 3.5)
         Predict: 0.0
     Else (feature 29 not in {1.0})
      If (feature 15 in {0.0})
       Predict: 0.0
      Else (feature 15 not in {0.0})
       If (feature 74 in {1.0})
        If (feature 5 <= 489.385)
         Predict: 0.0
        Else (feature 5 > 489.385)
         If (feature 6 <= 3004.0)
          Predict: 1.0
         Else (feature 6 > 3004.0)
          Predict: 0.0
       Else (feature 74 not in {1.0})
        Predict: 0.0
  Tree 2 (weight 1.0):
    If (feature 45 in {1.0})
     If (feature 68 in {1.0})
      If (feature 8 <= 0.76195)
       Predict: 0.0
      Else (feature 8 > 0.76195)
       If (feature 5 <= 59.915)
        If (feature 26 in {0.0})
         Predict: 0.0
        Else (feature 26 not in {0.0})
         Predict: 1.0
       Else (feature 5 > 59.915)
        If (feature 16 in {1.0})
         Predict: 0.0
        Else (feature 16 not in {1.0})
         Predict: 1.0
     Else (feature 68 not in {1.0})
      If (feature 3 <= 59.925)
       If (feature 25 in {1.0})
        If (feature 10 <= 5.5)
         Predict: 0.0
        Else (feature 10 > 5.5)
         If (feature 13 <= 35.5)
          Predict: 0.0
         Else (feature 13 > 35.5)
          Predict: 1.0
       Else (feature 25 not in {1.0})
        Predict: 0.0
      Else (feature 3 > 59.925)
       If (feature 114 in {1.0})
        Predict: 1.0
       Else (feature 114 not in {1.0})
        Predict: 0.0
    Else (feature 45 not in {1.0})
     Predict: 0.0
  Tree 3 (weight 1.0):
    If (feature 14 <= 0.5)
     If (feature 16 in {1.0})
      If (feature 10 <= 3.5)
       If (feature 43 in {0.0})
        Predict: 0.0
       Else (feature 43 not in {0.0})
        If (feature 94 in {1.0})
         Predict: 1.0
        Else (feature 94 not in {1.0})
         Predict: 0.0
      Else (feature 10 > 3.5)
       Predict: 0.0
     Else (feature 16 not in {1.0})
      If (feature 45 in {1.0})
       If (feature 11 <= 1.5)
        If (feature 26 in {1.0})
         If (feature 12 <= 17.5)
          Predict: 0.0
         Else (feature 12 > 17.5)
          Predict: 1.0
        Else (feature 26 not in {1.0})
         Predict: 0.0
       Else (feature 11 > 1.5)
        If (feature 109 in {1.0})
         If (feature 11 <= 5.5)
          Predict: 1.0
         Else (feature 11 > 5.5)
          Predict: 0.0
        Else (feature 109 not in {1.0})
         Predict: 0.0
      Else (feature 45 not in {1.0})
       If (feature 21 in {1.0})
        If (feature 5 <= 489.385)
         Predict: 0.0
        Else (feature 5 > 489.385)
         If (feature 4 <= 19.915)
          Predict: 1.0
         Else (feature 4 > 19.915)
          Predict: 0.0
       Else (feature 21 not in {1.0})
        Predict: 0.0
    Else (feature 14 > 0.5)
     If (feature 3 <= 69.985)
      If (feature 4 <= 67.2)
       Predict: 0.0
      Else (feature 4 > 67.2)
       If (feature 10 <= 9.5)
        Predict: 0.0
       Else (feature 10 > 9.5)
        If (feature 12 <= 10.5)
         Predict: 1.0
        Else (feature 12 > 10.5)
         Predict: 0.0
     Else (feature 3 > 69.985)
      If (feature 13 <= 8.5)
       If (feature 95 in {1.0})
        If (feature 12 <= 11.5)
         Predict: 0.0
        Else (feature 12 > 11.5)
         Predict: 1.0
       Else (feature 95 not in {1.0})
        Predict: 0.0
      Else (feature 13 > 8.5)
       If (feature 43 in {0.0})
        If (feature 21 in {1.0})
         If (feature 5 <= 164.945)
          Predict: 1.0
         Else (feature 5 > 164.945)
          Predict: 0.0
        Else (feature 21 not in {1.0})
         Predict: 0.0
       Else (feature 43 not in {0.0})
        Predict: 0.0
  Tree 4 (weight 1.0):
    If (feature 53 in {1.0})
     If (feature 6 <= 365.5)
      If (feature 12 <= 19.5)
       If (feature 4 <= 14.105)
        If (feature 21 in {1.0})
         Predict: 0.0
        Else (feature 21 not in {1.0})
         Predict: 1.0
       Else (feature 4 > 14.105)
        If (feature 23 in {1.0})
         Predict: 1.0
        Else (feature 23 not in {1.0})
         Predict: 0.0
      Else (feature 12 > 19.5)
       If (feature 12 <= 21.5)
        If (feature 5 <= 64.995)
         Predict: 1.0
        Else (feature 5 > 64.995)
         If (feature 12 <= 20.5)
          Predict: 0.0
         Else (feature 12 > 20.5)
          Predict: 1.0
       Else (feature 12 > 21.5)
        Predict: 0.0
     Else (feature 6 > 365.5)
      If (feature 28 in {1.0})
       Predict: 0.0
      Else (feature 28 not in {1.0})
       If (feature 10 <= 5.5)
        If (feature 10 <= 4.5)
         Predict: 0.0
        Else (feature 10 > 4.5)
         If (feature 0 <= 1.5)
          Predict: 1.0
         Else (feature 0 > 1.5)
          Predict: 0.0
       Else (feature 10 > 5.5)
        If (feature 8 <= 0.44345)
         If (feature 13 <= 22.5)
          Predict: 1.0
         Else (feature 13 > 22.5)
          Predict: 0.0
        Else (feature 8 > 0.44345)
         Predict: 0.0
    Else (feature 53 not in {1.0})
     If (feature 29 in {1.0})
      If (feature 75 in {1.0})
       Predict: 0.0
      Else (feature 75 not in {1.0})
       If (feature 9 <= 2017.5)
        Predict: 0.0
       Else (feature 9 > 2017.5)
        If (feature 44 in {1.0})
         If (feature 10 <= 3.5)
          Predict: 1.0
         Else (feature 10 > 3.5)
          Predict: 0.0
        Else (feature 44 not in {1.0})
         Predict: 0.0
     Else (feature 29 not in {1.0})
      If (feature 13 <= 34.5)
       Predict: 0.0
      Else (feature 13 > 34.5)
       If (feature 3 <= 159.985)
        If (feature 102 in {1.0})
         If (feature 13 <= 35.5)
          Predict: 1.0
         Else (feature 13 > 35.5)
          Predict: 0.0
        Else (feature 102 not in {1.0})
         Predict: 0.0
       Else (feature 3 > 159.985)
        If (feature 107 in {1.0})
         If (feature 23 in {0.0})
          Predict: 0.0
         Else (feature 23 not in {0.0})
          Predict: 1.0
        Else (feature 107 not in {1.0})
         Predict: 0.0
  Tree 5 (weight 1.0):
    If (feature 26 in {1.0})
     If (feature 10 <= 4.5)
      If (feature 84 in {1.0})
       If (feature 0 <= 1.5)
        Predict: 0.0
       Else (feature 0 > 1.5)
        Predict: 1.0
      Else (feature 84 not in {1.0})
       If (feature 11 <= 4.5)
        If (feature 3 <= 128.95)
         Predict: 0.0
        Else (feature 3 > 128.95)
         If (feature 10 <= 2.5)
          Predict: 0.0
         Else (feature 10 > 2.5)
          Predict: 1.0
       Else (feature 11 > 4.5)
        If (feature 6 <= 585.0)
         If (feature 13 <= 27.5)
          Predict: 1.0
         Else (feature 13 > 27.5)
          Predict: 0.0
        Else (feature 6 > 585.0)
         Predict: 0.0
     Else (feature 10 > 4.5)
      If (feature 85 in {1.0})
       If (feature 4 <= 19.145)
        Predict: 1.0
       Else (feature 4 > 19.145)
        Predict: 0.0
      Else (feature 85 not in {1.0})
       If (feature 10 <= 10.5)
        Predict: 0.0
       Else (feature 10 > 10.5)
        If (feature 8 <= 0.6396999999999999)
         If (feature 45 in {1.0})
          Predict: 1.0
         Else (feature 45 not in {1.0})
          Predict: 0.0
        Else (feature 8 > 0.6396999999999999)
         Predict: 0.0
    Else (feature 26 not in {1.0})
     If (feature 25 in {1.0})
      If (feature 11 <= 5.5)
       If (feature 111 in {1.0})
        Predict: 1.0
       Else (feature 111 not in {1.0})
        Predict: 0.0
      Else (feature 11 > 5.5)
       If (feature 8 <= 0.15225)
        Predict: 0.0
       Else (feature 8 > 0.15225)
        If (feature 73 in {1.0})
         If (feature 8 <= 0.17285)
          Predict: 1.0
         Else (feature 8 > 0.17285)
          Predict: 0.0
        Else (feature 73 not in {1.0})
         Predict: 0.0
     Else (feature 25 not in {1.0})
      If (feature 13 <= 34.5)
       Predict: 0.0
      Else (feature 13 > 34.5)
       If (feature 64 in {1.0})
        Predict: 1.0
       Else (feature 64 not in {1.0})
        Predict: 0.0
  Tree 6 (weight 1.0):
    If (feature 4 <= 13.705)
     If (feature 34 in {1.0})
      If (feature 10 <= 4.5)
       Predict: 1.0
      Else (feature 10 > 4.5)
       Predict: 0.0
     Else (feature 34 not in {1.0})
      If (feature 21 in {1.0})
       If (feature 8 <= 0.10894999999999999)
        Predict: 0.0
       Else (feature 8 > 0.10894999999999999)
        If (feature 4 <= 7.945)
         Predict: 0.0
        Else (feature 4 > 7.945)
         If (feature 7 <= 3196.0)
          Predict: 0.0
         Else (feature 7 > 3196.0)
          Predict: 1.0
      Else (feature 21 not in {1.0})
       If (feature 23 in {1.0})
        If (feature 90 in {1.0})
         Predict: 0.0
        Else (feature 90 not in {1.0})
         If (feature 13 <= 15.5)
          Predict: 1.0
         Else (feature 13 > 15.5)
          Predict: 0.0
       Else (feature 23 not in {1.0})
        If (feature 51 in {1.0})
         If (feature 5 <= 22.04)
          Predict: 1.0
         Else (feature 5 > 22.04)
          Predict: 0.0
        Else (feature 51 not in {1.0})
         Predict: 0.0
    Else (feature 4 > 13.705)
     If (feature 9 <= 2017.5)
      If (feature 10 <= 10.5)
       If (feature 22 in {1.0})
        If (feature 12 <= 22.5)
         Predict: 0.0
        Else (feature 12 > 22.5)
         If (feature 48 in {0.0})
          Predict: 0.0
         Else (feature 48 not in {0.0})
          Predict: 1.0
       Else (feature 22 not in {1.0})
        If (feature 12 <= 20.5)
         If (feature 13 <= 8.5)
          Predict: 1.0
         Else (feature 13 > 8.5)
          Predict: 0.0
        Else (feature 12 > 20.5)
         Predict: 0.0
      Else (feature 10 > 10.5)
       If (feature 119 in {1.0})
        Predict: 1.0
       Else (feature 119 not in {1.0})
        Predict: 0.0
     Else (feature 9 > 2017.5)
      If (feature 26 in {1.0})
       If (feature 3 <= 49.195)
        Predict: 0.0
       Else (feature 3 > 49.195)
        If (feature 6 <= 148.5)
         If (feature 43 in {1.0})
          Predict: 0.0
         Else (feature 43 not in {1.0})
          Predict: 1.0
        Else (feature 6 > 148.5)
         Predict: 0.0
      Else (feature 26 not in {1.0})
       Predict: 0.0
  Tree 7 (weight 1.0):
    If (feature 16 in {1.0})
     If (feature 46 in {1.0})
      If (feature 11 <= 5.5)
       Predict: 0.0
      Else (feature 11 > 5.5)
       If (feature 10 <= 3.5)
        If (feature 6 <= 2008.5)
         Predict: 0.0
        Else (feature 6 > 2008.5)
         If (feature 13 <= 18.5)
          Predict: 0.0
         Else (feature 13 > 18.5)
          Predict: 1.0
       Else (feature 10 > 3.5)
        Predict: 0.0
     Else (feature 46 not in {1.0})
      If (feature 2 <= 1.5)
       Predict: 0.0
      Else (feature 2 > 1.5)
       If (feature 71 in {0.0})
        Predict: 0.0
       Else (feature 71 not in {0.0})
        If (feature 8 <= 0.44345)
         Predict: 1.0
        Else (feature 8 > 0.44345)
         Predict: 0.0
    Else (feature 16 not in {1.0})
     If (feature 43 in {0.0})
      If (feature 13 <= 29.5)
       If (feature 9 <= 2017.5)
        If (feature 116 in {1.0})
         If (feature 12 <= 15.5)
          Predict: 0.0
         Else (feature 12 > 15.5)
          Predict: 1.0
        Else (feature 116 not in {1.0})
         Predict: 0.0
       Else (feature 9 > 2017.5)
        Predict: 0.0
      Else (feature 13 > 29.5)
       If (feature 127 in {1.0})
        If (feature 19 in {1.0})
         Predict: 0.0
        Else (feature 19 not in {1.0})
         Predict: 1.0
       Else (feature 127 not in {1.0})
        Predict: 0.0
     Else (feature 43 not in {0.0})
      If (feature 27 in {1.0})
       If (feature 6 <= 9525.0)
        If (feature 97 in {1.0})
         Predict: 1.0
        Else (feature 97 not in {1.0})
         If (feature 2 <= 1.5)
          Predict: 0.0
         Else (feature 2 > 1.5)
          Predict: 1.0
       Else (feature 6 > 9525.0)
        Predict: 0.0
      Else (feature 27 not in {1.0})
       If (feature 7 <= 17728.5)
        Predict: 0.0
       Else (feature 7 > 17728.5)
        If (feature 23 in {1.0})
         If (feature 101 in {1.0})
          Predict: 1.0
         Else (feature 101 not in {1.0})
          Predict: 0.0
        Else (feature 23 not in {1.0})
         Predict: 0.0
  Tree 8 (weight 1.0):
    If (feature 47 in {1.0})
     If (feature 4 <= 35.75)
      If (feature 8 <= 0.10894999999999999)
       If (feature 9 <= 2017.5)
        If (feature 3 <= 139.17000000000002)
         If (feature 82 in {0.0})
          Predict: 0.0
         Else (feature 82 not in {0.0})
          Predict: 1.0
        Else (feature 3 > 139.17000000000002)
         If (feature 107 in {1.0})
          Predict: 1.0
         Else (feature 107 not in {1.0})
          Predict: 0.0
       Else (feature 9 > 2017.5)
        If (feature 4 <= 31.689999999999998)
         Predict: 0.0
        Else (feature 4 > 31.689999999999998)
         If (feature 10 <= 1.5)
          Predict: 0.0
         Else (feature 10 > 1.5)
          Predict: 1.0
      Else (feature 8 > 0.10894999999999999)
       If (feature 4 <= 11.844999999999999)
        If (feature 3 <= 49.195)
         Predict: 0.0
        Else (feature 3 > 49.195)
         If (feature 5 <= 51.91)
          Predict: 1.0
         Else (feature 5 > 51.91)
          Predict: 0.0
       Else (feature 4 > 11.844999999999999)
        If (feature 16 in {1.0})
         Predict: 0.0
        Else (feature 16 not in {1.0})
         If (feature 127 in {1.0})
          Predict: 1.0
         Else (feature 127 not in {1.0})
          Predict: 0.0
     Else (feature 4 > 35.75)
      If (feature 66 in {1.0})
       If (feature 4 <= 50.879999999999995)
        Predict: 0.0
       Else (feature 4 > 50.879999999999995)
        If (feature 16 in {0.0})
         Predict: 0.0
        Else (feature 16 not in {0.0})
         If (feature 13 <= 14.5)
          Predict: 1.0
         Else (feature 13 > 14.5)
          Predict: 0.0
      Else (feature 66 not in {1.0})
       If (feature 7 <= 11404.0)
        If (feature 12 <= 21.5)
         If (feature 70 in {1.0})
          Predict: 1.0
         Else (feature 70 not in {1.0})
          Predict: 0.0
        Else (feature 12 > 21.5)
         Predict: 0.0
       Else (feature 7 > 11404.0)
        Predict: 0.0
    Else (feature 47 not in {1.0})
     If (feature 13 <= 34.5)
      If (feature 25 in {1.0})
       If (feature 92 in {1.0})
        If (feature 5 <= 32.995000000000005)
         Predict: 1.0
        Else (feature 5 > 32.995000000000005)
         Predict: 0.0
       Else (feature 92 not in {1.0})
        If (feature 55 in {1.0})
         Predict: 1.0
        Else (feature 55 not in {1.0})
         If (feature 95 in {1.0})
          Predict: 1.0
         Else (feature 95 not in {1.0})
          Predict: 0.0
      Else (feature 25 not in {1.0})
       Predict: 0.0
     Else (feature 13 > 34.5)
      If (feature 4 <= 23.705)
       If (feature 70 in {1.0})
        If (feature 6 <= 13825.0)
         Predict: 0.0
        Else (feature 6 > 13825.0)
         Predict: 1.0
       Else (feature 70 not in {1.0})
        Predict: 0.0
      Else (feature 4 > 23.705)
       If (feature 53 in {1.0})
        Predict: 0.0
       Else (feature 53 not in {1.0})
        If (feature 27 in {1.0})
         Predict: 0.0
        Else (feature 27 not in {1.0})
         If (feature 115 in {1.0})
          Predict: 1.0
         Else (feature 115 not in {1.0})
          Predict: 0.0
  Tree 9 (weight 1.0):
    If (feature 14 <= 0.5)
     If (feature 34 in {1.0})
      If (feature 96 in {1.0})
       Predict: 1.0
      Else (feature 96 not in {1.0})
       If (feature 13 <= 31.5)
        If (feature 5 <= 32.995000000000005)
         Predict: 1.0
        Else (feature 5 > 32.995000000000005)
         Predict: 0.0
       Else (feature 13 > 31.5)
        Predict: 0.0
     Else (feature 34 not in {1.0})
      Predict: 0.0
    Else (feature 14 > 0.5)
     If (feature 15 in {0.0})
      Predict: 0.0
     Else (feature 15 not in {0.0})
      If (feature 7 <= 45092.0)
       If (feature 82 in {1.0})
        Predict: 0.0
       Else (feature 82 not in {1.0})
        If (feature 13 <= 8.5)
         If (feature 10 <= 2.5)
          Predict: 1.0
         Else (feature 10 > 2.5)
          Predict: 0.0
        Else (feature 13 > 8.5)
         Predict: 0.0
      Else (feature 7 > 45092.0)
       Predict: 0.0
  Tree 10 (weight 1.0):
    If (feature 23 in {1.0})
     If (feature 8 <= 0.3506)
      If (feature 9 <= 2017.5)
       Predict: 0.0
      Else (feature 9 > 2017.5)
       If (feature 68 in {1.0})
        Predict: 0.0
       Else (feature 68 not in {1.0})
        If (feature 81 in {1.0})
         Predict: 0.0
        Else (feature 81 not in {1.0})
         If (feature 100 in {1.0})
          Predict: 1.0
         Else (feature 100 not in {1.0})
          Predict: 0.0
     Else (feature 8 > 0.3506)
      If (feature 43 in {0.0})
       If (feature 13 <= 24.5)
        Predict: 0.0
       Else (feature 13 > 24.5)
        If (feature 73 in {0.0})
         Predict: 0.0
        Else (feature 73 not in {0.0})
         Predict: 1.0
      Else (feature 43 not in {0.0})
       If (feature 7 <= 2301.5)
        Predict: 0.0
       Else (feature 7 > 2301.5)
        If (feature 87 in {1.0})
         Predict: 1.0
        Else (feature 87 not in {1.0})
         Predict: 0.0
    Else (feature 23 not in {1.0})
     If (feature 10 <= 3.5)
      Predict: 0.0
     Else (feature 10 > 3.5)
      If (feature 15 in {1.0})
       If (feature 7 <= 45092.0)
        Predict: 0.0
       Else (feature 7 > 45092.0)
        If (feature 97 in {1.0})
         Predict: 1.0
        Else (feature 97 not in {1.0})
         Predict: 0.0
      Else (feature 15 not in {1.0})
       Predict: 0.0
  Tree 11 (weight 1.0):
    If (feature 5 <= 149.89)
     If (feature 13 <= 8.5)
      If (feature 8 <= 0.0727)
       If (feature 5 <= 128.975)
        Predict: 0.0
       Else (feature 5 > 128.975)
        If (feature 6 <= 719.5)
         Predict: 0.0
        Else (feature 6 > 719.5)
         If (feature 68 in {0.0})
          Predict: 0.0
         Else (feature 68 not in {0.0})
          Predict: 1.0
      Else (feature 8 > 0.0727)
       Predict: 0.0
     Else (feature 13 > 8.5)
      If (feature 16 in {1.0})
       If (feature 97 in {1.0})
        If (feature 13 <= 25.5)
         If (feature 8 <= 0.76195)
          Predict: 1.0
         Else (feature 8 > 0.76195)
          Predict: 0.0
        Else (feature 13 > 25.5)
         Predict: 0.0
       Else (feature 97 not in {1.0})
        Predict: 0.0
      Else (feature 16 not in {1.0})
       If (feature 9 <= 2017.5)
        Predict: 0.0
       Else (feature 9 > 2017.5)
        If (feature 34 in {1.0})
         If (feature 3 <= 119.82499999999999)
          Predict: 0.0
         Else (feature 3 > 119.82499999999999)
          Predict: 1.0
        Else (feature 34 not in {1.0})
         Predict: 0.0
    Else (feature 5 > 149.89)
     If (feature 9 <= 2017.5)
      If (feature 16 in {1.0})
       If (feature 85 in {1.0})
        Predict: 0.0
       Else (feature 85 not in {1.0})
        If (feature 79 in {1.0})
         If (feature 13 <= 12.5)
          Predict: 1.0
         Else (feature 13 > 12.5)
          Predict: 0.0
        Else (feature 79 not in {1.0})
         Predict: 0.0
      Else (feature 16 not in {1.0})
       If (feature 92 in {1.0})
        Predict: 1.0
       Else (feature 92 not in {1.0})
        Predict: 0.0
     Else (feature 9 > 2017.5)
      If (feature 13 <= 31.5)
       If (feature 25 in {1.0})
        If (feature 66 in {1.0})
         If (feature 6 <= 500.5)
          Predict: 1.0
         Else (feature 6 > 500.5)
          Predict: 0.0
        Else (feature 66 not in {1.0})
         If (feature 79 in {1.0})
          Predict: 1.0
         Else (feature 79 not in {1.0})
          Predict: 0.0
       Else (feature 25 not in {1.0})
        Predict: 0.0
      Else (feature 13 > 31.5)
       Predict: 0.0
  Tree 12 (weight 1.0):
    If (feature 15 in {1.0})
     If (feature 53 in {1.0})
      Predict: 0.0
     Else (feature 53 not in {1.0})
      If (feature 6 <= 6962.5)
       Predict: 0.0
      Else (feature 6 > 6962.5)
       If (feature 5 <= 319.45)
        If (feature 3 <= 14.55)
         Predict: 1.0
        Else (feature 3 > 14.55)
         Predict: 0.0
       Else (feature 5 > 319.45)
        Predict: 0.0
    Else (feature 15 not in {1.0})
     If (feature 6 <= 3004.0)
      If (feature 34 in {1.0})
       If (feature 64 in {1.0})
        Predict: 1.0
       Else (feature 64 not in {1.0})
        Predict: 0.0
      Else (feature 34 not in {1.0})
       Predict: 0.0
     Else (feature 6 > 3004.0)
      If (feature 8 <= 0.3257)
       If (feature 29 in {1.0})
        If (feature 71 in {1.0})
         Predict: 1.0
        Else (feature 71 not in {1.0})
         Predict: 0.0
       Else (feature 29 not in {1.0})
        If (feature 83 in {1.0})
         If (feature 19 in {1.0})
          Predict: 1.0
         Else (feature 19 not in {1.0})
          Predict: 0.0
        Else (feature 83 not in {1.0})
         Predict: 0.0
      Else (feature 8 > 0.3257)
       If (feature 16 in {1.0})
        If (feature 8 <= 0.44345)
         Predict: 0.0
        Else (feature 8 > 0.44345)
         If (feature 96 in {1.0})
          Predict: 1.0
         Else (feature 96 not in {1.0})
          Predict: 0.0
       Else (feature 16 not in {1.0})
        If (feature 35 in {1.0})
         If (feature 12 <= 8.5)
          Predict: 0.0
         Else (feature 12 > 8.5)
          Predict: 1.0
        Else (feature 35 not in {1.0})
         Predict: 0.0
  Tree 13 (weight 1.0):
    If (feature 19 in {1.0})
     If (feature 12 <= 1.5)
      If (feature 14 <= 0.5)
       If (feature 75 in {1.0})
        If (feature 8 <= 0.10894999999999999)
         Predict: 1.0
        Else (feature 8 > 0.10894999999999999)
         Predict: 0.0
       Else (feature 75 not in {1.0})
        If (feature 6 <= 5541.0)
         If (feature 105 in {1.0})
          Predict: 1.0
         Else (feature 105 not in {1.0})
          Predict: 0.0
        Else (feature 6 > 5541.0)
         If (feature 0 <= 1.5)
          Predict: 0.0
         Else (feature 0 > 1.5)
          Predict: 1.0
      Else (feature 14 > 0.5)
       Predict: 0.0
     Else (feature 12 > 1.5)
      If (feature 122 in {1.0})
       Predict: 1.0
      Else (feature 122 not in {1.0})
       Predict: 0.0
    Else (feature 19 not in {1.0})
     If (feature 17 in {1.0})
      Predict: 0.0
     Else (feature 17 not in {1.0})
      If (feature 23 in {1.0})
       Predict: 0.0
      Else (feature 23 not in {1.0})
       If (feature 26 in {1.0})
        If (feature 9 <= 2017.5)
         If (feature 93 in {1.0})
          Predict: 1.0
         Else (feature 93 not in {1.0})
          Predict: 0.0
        Else (feature 9 > 2017.5)
         If (feature 84 in {1.0})
          Predict: 1.0
         Else (feature 84 not in {1.0})
          Predict: 0.0
       Else (feature 26 not in {1.0})
        Predict: 0.0
  Tree 14 (weight 1.0):
    If (feature 34 in {1.0})
     If (feature 12 <= 15.5)
      If (feature 97 in {1.0})
       Predict: 1.0
      Else (feature 97 not in {1.0})
       If (feature 96 in {1.0})
        Predict: 1.0
       Else (feature 96 not in {1.0})
        Predict: 0.0
     Else (feature 12 > 15.5)
      If (feature 10 <= 7.5)
       If (feature 58 in {1.0})
        Predict: 0.0
       Else (feature 58 not in {1.0})
        If (feature 67 in {1.0})
         If (feature 4 <= 19.145)
          Predict: 0.0
         Else (feature 4 > 19.145)
          Predict: 1.0
        Else (feature 67 not in {1.0})
         If (feature 68 in {1.0})
          Predict: 1.0
         Else (feature 68 not in {1.0})
          Predict: 0.0
      Else (feature 10 > 7.5)
       If (feature 45 in {1.0})
        Predict: 1.0
       Else (feature 45 not in {1.0})
        Predict: 0.0
    Else (feature 34 not in {1.0})
     If (feature 2 <= 1.5)
      If (feature 16 in {1.0})
       If (feature 50 in {1.0})
        If (feature 10 <= 2.5)
         If (feature 12 <= 15.5)
          Predict: 0.0
         Else (feature 12 > 15.5)
          Predict: 1.0
        Else (feature 10 > 2.5)
         Predict: 0.0
       Else (feature 50 not in {1.0})
        If (feature 126 in {1.0})
         If (feature 3 <= 78.275)
          Predict: 1.0
         Else (feature 3 > 78.275)
          Predict: 0.0
        Else (feature 126 not in {1.0})
         Predict: 0.0
      Else (feature 16 not in {1.0})
       If (feature 21 in {1.0})
        If (feature 4 <= 11.844999999999999)
         If (feature 67 in {1.0})
          Predict: 1.0
         Else (feature 67 not in {1.0})
          Predict: 0.0
        Else (feature 4 > 11.844999999999999)
         If (feature 96 in {1.0})
          Predict: 1.0
         Else (feature 96 not in {1.0})
          Predict: 0.0
       Else (feature 21 not in {1.0})
        Predict: 0.0
     Else (feature 2 > 1.5)
      Predict: 0.0
  Tree 15 (weight 1.0):
    If (feature 0 <= 1.5)
     If (feature 9 <= 2017.5)
      Predict: 0.0
     Else (feature 9 > 2017.5)
      If (feature 10 <= 3.5)
       If (feature 19 in {1.0})
        If (feature 53 in {1.0})
         Predict: 1.0
        Else (feature 53 not in {1.0})
         Predict: 0.0
       Else (feature 19 not in {1.0})
        Predict: 0.0
      Else (feature 10 > 3.5)
       If (feature 17 in {1.0})
        Predict: 0.0
       Else (feature 17 not in {1.0})
        If (feature 7 <= 45092.0)
         If (feature 41 in {1.0})
          Predict: 1.0
         Else (feature 41 not in {1.0})
          Predict: 0.0
        Else (feature 7 > 45092.0)
         Predict: 0.0
    Else (feature 0 > 1.5)
     If (feature 32 in {1.0})
      If (feature 5 <= 69.935)
       If (feature 1 <= 1.5)
        Predict: 0.0
       Else (feature 1 > 1.5)
        Predict: 1.0
      Else (feature 5 > 69.935)
       Predict: 0.0
     Else (feature 32 not in {1.0})
      If (feature 23 in {1.0})
       If (feature 7 <= 834.0)
        Predict: 1.0
       Else (feature 7 > 834.0)
        If (feature 76 in {1.0})
         Predict: 1.0
        Else (feature 76 not in {1.0})
         Predict: 0.0
      Else (feature 23 not in {1.0})
       If (feature 81 in {1.0})
        If (feature 7 <= 88180.0)
         Predict: 0.0
        Else (feature 7 > 88180.0)
         If (feature 3 <= 149.835)
          Predict: 1.0
         Else (feature 3 > 149.835)
          Predict: 0.0
       Else (feature 81 not in {1.0})
        Predict: 0.0
  Tree 16 (weight 1.0):
    If (feature 19 in {1.0})
     If (feature 14 <= 0.5)
      If (feature 8 <= 0.10894999999999999)
       If (feature 122 in {1.0})
        Predict: 0.0
       Else (feature 122 not in {1.0})
        If (feature 10 <= 3.5)
         If (feature 82 in {1.0})
          Predict: 1.0
         Else (feature 82 not in {1.0})
          Predict: 0.0
        Else (feature 10 > 3.5)
         Predict: 0.0
      Else (feature 8 > 0.10894999999999999)
       If (feature 5 <= 249.325)
        Predict: 0.0
       Else (feature 5 > 249.325)
        If (feature 9 <= 2017.5)
         Predict: 0.0
        Else (feature 9 > 2017.5)
         If (feature 107 in {1.0})
          Predict: 1.0
         Else (feature 107 not in {1.0})
          Predict: 0.0
     Else (feature 14 > 0.5)
      If (feature 92 in {1.0})
       Predict: 0.0
      Else (feature 92 not in {1.0})
       If (feature 6 <= 2512.5)
        Predict: 0.0
       Else (feature 6 > 2512.5)
        If (feature 8 <= 0.3506)
         Predict: 0.0
        Else (feature 8 > 0.3506)
         Predict: 1.0
    Else (feature 19 not in {1.0})
     If (feature 10 <= 3.5)
      If (feature 13 <= 16.5)
       Predict: 0.0
      Else (feature 13 > 16.5)
       If (feature 9 <= 2017.5)
        If (feature 96 in {1.0})
         If (feature 8 <= 0.3506)
          Predict: 0.0
         Else (feature 8 > 0.3506)
          Predict: 1.0
        Else (feature 96 not in {1.0})
         If (feature 64 in {1.0})
          Predict: 1.0
         Else (feature 64 not in {1.0})
          Predict: 0.0
       Else (feature 9 > 2017.5)
        Predict: 0.0
     Else (feature 10 > 3.5)
      If (feature 21 in {1.0})
       If (feature 12 <= 19.5)
        Predict: 0.0
       Else (feature 12 > 19.5)
        If (feature 92 in {1.0})
         Predict: 1.0
        Else (feature 92 not in {1.0})
         Predict: 0.0
      Else (feature 21 not in {1.0})
       If (feature 7 <= 45092.0)
        If (feature 34 in {1.0})
         If (feature 6 <= 148.5)
          Predict: 1.0
         Else (feature 6 > 148.5)
          Predict: 0.0
        Else (feature 34 not in {1.0})
         Predict: 0.0
       Else (feature 7 > 45092.0)
        If (feature 8 <= 0.44345)
         Predict: 0.0
        Else (feature 8 > 0.44345)
         If (feature 82 in {1.0})
          Predict: 1.0
         Else (feature 82 not in {1.0})
          Predict: 0.0
  Tree 17 (weight 1.0):
    If (feature 43 in {0.0})
     If (feature 53 in {1.0})
      If (feature 27 in {1.0})
       Predict: 0.0
      Else (feature 27 not in {1.0})
       If (feature 13 <= 23.5)
        If (feature 4 <= 12.475000000000001)
         Predict: 0.0
        Else (feature 4 > 12.475000000000001)
         If (feature 17 in {1.0})
          Predict: 0.0
         Else (feature 17 not in {1.0})
          Predict: 1.0
       Else (feature 13 > 23.5)
        If (feature 35 in {1.0})
         If (feature 13 <= 28.5)
          Predict: 1.0
         Else (feature 13 > 28.5)
          Predict: 0.0
        Else (feature 35 not in {1.0})
         Predict: 0.0
     Else (feature 53 not in {1.0})
      If (feature 13 <= 36.5)
       If (feature 9 <= 2016.5)
        Predict: 1.0
       Else (feature 9 > 2016.5)
        Predict: 0.0
      Else (feature 13 > 36.5)
       If (feature 3 <= 549.9449999999999)
        Predict: 0.0
       Else (feature 3 > 549.9449999999999)
        If (feature 64 in {1.0})
         Predict: 1.0
        Else (feature 64 not in {1.0})
         Predict: 0.0
    Else (feature 43 not in {0.0})
     If (feature 29 in {1.0})
      If (feature 8 <= 0.20905)
       If (feature 8 <= 0.17285)
        If (feature 76 in {1.0})
         Predict: 1.0
        Else (feature 76 not in {1.0})
         Predict: 0.0
       Else (feature 8 > 0.17285)
        If (feature 11 <= 2.5)
         Predict: 0.0
        Else (feature 11 > 2.5)
         If (feature 6 <= 2512.5)
          Predict: 1.0
         Else (feature 6 > 2512.5)
          Predict: 0.0
      Else (feature 8 > 0.20905)
       If (feature 11 <= 6.5)
        Predict: 0.0
       Else (feature 11 > 6.5)
        If (feature 0 <= 1.5)
         Predict: 0.0
        Else (feature 0 > 1.5)
         If (feature 72 in {1.0})
          Predict: 0.0
         Else (feature 72 not in {1.0})
          Predict: 1.0
     Else (feature 29 not in {1.0})
      Predict: 0.0
  Tree 18 (weight 1.0):
    If (feature 44 in {1.0})
     If (feature 10 <= 3.5)
      If (feature 105 in {1.0})
       If (feature 8 <= 0.036699999999999997)
        Predict: 1.0
       Else (feature 8 > 0.036699999999999997)
        Predict: 0.0
      Else (feature 105 not in {1.0})
       If (feature 5 <= 32.995000000000005)
        Predict: 0.0
       Else (feature 5 > 32.995000000000005)
        If (feature 7 <= 625.5)
         If (feature 67 in {0.0})
          Predict: 0.0
         Else (feature 67 not in {0.0})
          Predict: 1.0
        Else (feature 7 > 625.5)
         Predict: 0.0
     Else (feature 10 > 3.5)
      If (feature 8 <= 0.12004999999999999)
       Predict: 0.0
      Else (feature 8 > 0.12004999999999999)
       If (feature 13 <= 37.5)
        If (feature 31 in {1.0})
         If (feature 11 <= 2.5)
          Predict: 1.0
         Else (feature 11 > 2.5)
          Predict: 0.0
        Else (feature 31 not in {1.0})
         Predict: 0.0
       Else (feature 13 > 37.5)
        Predict: 0.0
    Else (feature 44 not in {1.0})
     If (feature 10 <= 3.5)
      If (feature 5 <= 51.91)
       If (feature 19 in {1.0})
        If (feature 91 in {1.0})
         Predict: 1.0
        Else (feature 91 not in {1.0})
         Predict: 0.0
       Else (feature 19 not in {1.0})
        Predict: 0.0
      Else (feature 5 > 51.91)
       If (feature 7 <= 3615.0)
        If (feature 4 <= 16.145)
         Predict: 0.0
        Else (feature 4 > 16.145)
         If (feature 108 in {1.0})
          Predict: 1.0
         Else (feature 108 not in {1.0})
          Predict: 0.0
       Else (feature 7 > 3615.0)
        Predict: 0.0
     Else (feature 10 > 3.5)
      If (feature 32 in {1.0})
       If (feature 12 <= 7.5)
        If (feature 4 <= 50.879999999999995)
         Predict: 1.0
        Else (feature 4 > 50.879999999999995)
         Predict: 0.0
       Else (feature 12 > 7.5)
        If (feature 1 <= 1.5)
         Predict: 0.0
        Else (feature 1 > 1.5)
         If (feature 46 in {1.0})
          Predict: 0.0
         Else (feature 46 not in {1.0})
          Predict: 1.0
      Else (feature 32 not in {1.0})
       If (feature 5 <= 249.325)
        Predict: 0.0
       Else (feature 5 > 249.325)
        If (feature 70 in {1.0})
         If (feature 49 in {1.0})
          Predict: 1.0
         Else (feature 49 not in {1.0})
          Predict: 0.0
        Else (feature 70 not in {1.0})
         Predict: 0.0
  Tree 19 (weight 1.0):
    If (feature 53 in {1.0})
     If (feature 8 <= 0.22425)
      If (feature 19 in {1.0})
       Predict: 1.0
      Else (feature 19 not in {1.0})
       If (feature 3 <= 66.89500000000001)
        If (feature 11 <= 4.5)
         Predict: 1.0
        Else (feature 11 > 4.5)
         Predict: 0.0
       Else (feature 3 > 66.89500000000001)
        Predict: 0.0
     Else (feature 8 > 0.22425)
      If (feature 16 in {1.0})
       Predict: 0.0
      Else (feature 16 not in {1.0})
       If (feature 3 <= 226.985)
        If (feature 14 <= 0.5)
         Predict: 0.0
        Else (feature 14 > 0.5)
         Predict: 1.0
       Else (feature 3 > 226.985)
        Predict: 1.0
    Else (feature 53 not in {1.0})
     If (feature 16 in {1.0})
      If (feature 71 in {1.0})
       Predict: 0.0
      Else (feature 71 not in {1.0})
       If (feature 46 in {1.0})
        Predict: 0.0
       Else (feature 46 not in {1.0})
        If (feature 43 in {0.0})
         If (feature 103 in {1.0})
          Predict: 1.0
         Else (feature 103 not in {1.0})
          Predict: 0.0
        Else (feature 43 not in {0.0})
         Predict: 0.0
     Else (feature 16 not in {1.0})
      If (feature 48 in {1.0})
       Predict: 0.0
      Else (feature 48 not in {1.0})
       If (feature 47 in {1.0})
        If (feature 29 in {1.0})
         Predict: 0.0
        Else (feature 29 not in {1.0})
         If (feature 127 in {1.0})
          Predict: 1.0
         Else (feature 127 not in {1.0})
          Predict: 0.0
       Else (feature 47 not in {1.0})
        Predict: 0.0
  Tree 20 (weight 1.0):
    If (feature 15 in {1.0})
     If (feature 126 in {1.0})
      If (feature 12 <= 15.5)
       Predict: 0.0
      Else (feature 12 > 15.5)
       If (feature 9 <= 2017.5)
        Predict: 0.0
       Else (feature 9 > 2017.5)
        Predict: 1.0
     Else (feature 126 not in {1.0})
      Predict: 0.0
    Else (feature 15 not in {1.0})
     If (feature 10 <= 3.5)
      If (feature 43 in {0.0})
       If (feature 17 in {1.0})
        If (feature 67 in {1.0})
         If (feature 4 <= 7.945)
          Predict: 1.0
         Else (feature 4 > 7.945)
          Predict: 0.0
        Else (feature 67 not in {1.0})
         Predict: 0.0
       Else (feature 17 not in {1.0})
        If (feature 4 <= 16.605)
         If (feature 112 in {1.0})
          Predict: 1.0
         Else (feature 112 not in {1.0})
          Predict: 0.0
        Else (feature 4 > 16.605)
         Predict: 0.0
      Else (feature 43 not in {0.0})
       If (feature 103 in {1.0})
        If (feature 6 <= 1051.5)
         Predict: 1.0
        Else (feature 6 > 1051.5)
         Predict: 0.0
       Else (feature 103 not in {1.0})
        Predict: 0.0
     Else (feature 10 > 3.5)
      Predict: 0.0
  Tree 21 (weight 1.0):
    If (feature 23 in {1.0})
     If (feature 100 in {1.0})
      If (feature 43 in {0.0})
       Predict: 0.0
      Else (feature 43 not in {0.0})
       If (feature 5 <= 89.89500000000001)
        Predict: 1.0
       Else (feature 5 > 89.89500000000001)
        Predict: 0.0
     Else (feature 100 not in {1.0})
      If (feature 13 <= 24.5)
       If (feature 43 in {0.0})
        If (feature 53 in {1.0})
         Predict: 1.0
        Else (feature 53 not in {1.0})
         If (feature 111 in {1.0})
          Predict: 1.0
         Else (feature 111 not in {1.0})
          Predict: 0.0
       Else (feature 43 not in {0.0})
        Predict: 0.0
      Else (feature 13 > 24.5)
       If (feature 0 <= 3.5)
        If (feature 4 <= 19.915)
         If (feature 53 in {1.0})
          Predict: 1.0
         Else (feature 53 not in {1.0})
          Predict: 0.0
        Else (feature 4 > 19.915)
         Predict: 0.0
       Else (feature 0 > 3.5)
        If (feature 11 <= 3.5)
         Predict: 1.0
        Else (feature 11 > 3.5)
         If (feature 11 <= 5.5)
          Predict: 0.0
         Else (feature 11 > 5.5)
          Predict: 1.0
    Else (feature 23 not in {1.0})
     If (feature 26 in {1.0})
      If (feature 11 <= 2.5)
       If (feature 86 in {1.0})
        Predict: 0.0
       Else (feature 86 not in {1.0})
        If (feature 88 in {1.0})
         If (feature 10 <= 4.5)
          Predict: 1.0
         Else (feature 10 > 4.5)
          Predict: 0.0
        Else (feature 88 not in {1.0})
         Predict: 0.0
      Else (feature 11 > 2.5)
       Predict: 0.0
     Else (feature 26 not in {1.0})
      If (feature 1 <= 1.5)
       If (feature 9 <= 2017.5)
        If (feature 29 in {1.0})
         If (feature 71 in {1.0})
          Predict: 1.0
         Else (feature 71 not in {1.0})
          Predict: 0.0
        Else (feature 29 not in {1.0})
         Predict: 0.0
       Else (feature 9 > 2017.5)
        Predict: 0.0
      Else (feature 1 > 1.5)
       Predict: 0.0
  Tree 22 (weight 1.0):
    If (feature 45 in {1.0})
     If (feature 34 in {1.0})
      If (feature 112 in {1.0})
       Predict: 1.0
      Else (feature 112 not in {1.0})
       If (feature 9 <= 2017.5)
        Predict: 0.0
       Else (feature 9 > 2017.5)
        If (feature 13 <= 31.5)
         If (feature 12 <= 10.5)
          Predict: 0.0
         Else (feature 12 > 10.5)
          Predict: 1.0
        Else (feature 13 > 31.5)
         Predict: 0.0
     Else (feature 34 not in {1.0})
      If (feature 14 <= 0.5)
       If (feature 5 <= 59.915)
        If (feature 8 <= 0.0727)
         Predict: 1.0
        Else (feature 8 > 0.0727)
         Predict: 0.0
       Else (feature 5 > 59.915)
        Predict: 0.0
      Else (feature 14 > 0.5)
       Predict: 0.0
    Else (feature 45 not in {1.0})
     If (feature 10 <= 3.5)
      If (feature 13 <= 32.5)
       If (feature 31 in {1.0})
        If (feature 95 in {1.0})
         Predict: 1.0
        Else (feature 95 not in {1.0})
         Predict: 0.0
       Else (feature 31 not in {1.0})
        If (feature 29 in {1.0})
         If (feature 11 <= 5.5)
          Predict: 0.0
         Else (feature 11 > 5.5)
          Predict: 1.0
        Else (feature 29 not in {1.0})
         Predict: 0.0
      Else (feature 13 > 32.5)
       Predict: 0.0
     Else (feature 10 > 3.5)
      If (feature 7 <= 13370.0)
       If (feature 13 <= 8.5)
        If (feature 21 in {1.0})
         Predict: 1.0
        Else (feature 21 not in {1.0})
         If (feature 6 <= 9525.0)
          Predict: 0.0
         Else (feature 6 > 9525.0)
          Predict: 1.0
       Else (feature 13 > 8.5)
        Predict: 0.0
      Else (feature 7 > 13370.0)
       Predict: 0.0
  Tree 23 (weight 1.0):
    If (feature 53 in {1.0})
     If (feature 31 in {1.0})
      If (feature 5 <= 69.935)
       Predict: 0.0
      Else (feature 5 > 69.935)
       Predict: 1.0
     Else (feature 31 not in {1.0})
      If (feature 4 <= 23.705)
       If (feature 8 <= 0.3506)
        Predict: 0.0
       Else (feature 8 > 0.3506)
        If (feature 10 <= 3.5)
         Predict: 1.0
        Else (feature 10 > 3.5)
         Predict: 0.0
      Else (feature 4 > 23.705)
       If (feature 0 <= 2.5)
        If (feature 7 <= 7971.0)
         If (feature 29 in {1.0})
          Predict: 1.0
         Else (feature 29 not in {1.0})
          Predict: 0.0
        Else (feature 7 > 7971.0)
         Predict: 0.0
       Else (feature 0 > 2.5)
        Predict: 1.0
    Else (feature 53 not in {1.0})
     If (feature 10 <= 3.5)
      If (feature 21 in {1.0})
       If (feature 109 in {1.0})
        Predict: 1.0
       Else (feature 109 not in {1.0})
        Predict: 0.0
      Else (feature 21 not in {1.0})
       Predict: 0.0
     Else (feature 10 > 3.5)
      Predict: 0.0
  Tree 24 (weight 1.0):
    If (feature 53 in {1.0})
     If (feature 29 in {1.0})
      Predict: 0.0
     Else (feature 29 not in {1.0})
      If (feature 4 <= 25.805)
       If (feature 24 in {1.0})
        If (feature 13 <= 21.5)
         Predict: 1.0
        Else (feature 13 > 21.5)
         Predict: 0.0
       Else (feature 24 not in {1.0})
        If (feature 6 <= 402.0)
         If (feature 19 in {1.0})
          Predict: 1.0
         Else (feature 19 not in {1.0})
          Predict: 0.0
        Else (feature 6 > 402.0)
         If (feature 35 in {1.0})
          Predict: 1.0
         Else (feature 35 not in {1.0})
          Predict: 0.0
      Else (feature 4 > 25.805)
       If (feature 6 <= 402.0)
        If (feature 21 in {1.0})
         Predict: 1.0
        Else (feature 21 not in {1.0})
         Predict: 0.0
       Else (feature 6 > 402.0)
        Predict: 0.0
    Else (feature 53 not in {1.0})
     If (feature 14 <= 0.5)
      If (feature 16 in {1.0})
       If (feature 115 in {1.0})
        If (feature 10 <= 1.5)
         Predict: 0.0
        Else (feature 10 > 1.5)
         If (feature 4 <= 31.689999999999998)
          Predict: 1.0
         Else (feature 4 > 31.689999999999998)
          Predict: 0.0
       Else (feature 115 not in {1.0})
        Predict: 0.0
      Else (feature 16 not in {1.0})
       Predict: 0.0
     Else (feature 14 > 0.5)
      If (feature 135 in {1.0})
       If (feature 3 <= 23.994999999999997)
        Predict: 1.0
       Else (feature 3 > 23.994999999999997)
        Predict: 0.0
      Else (feature 135 not in {1.0})
       If (feature 79 in {1.0})
        Predict: 0.0
       Else (feature 79 not in {1.0})
        If (feature 0 <= 11.5)
         Predict: 0.0
        Else (feature 0 > 11.5)
         If (feature 12 <= 15.5)
          Predict: 0.0
         Else (feature 12 > 15.5)
          Predict: 1.0
  Tree 25 (weight 1.0):
    If (feature 7 <= 45092.0)
     If (feature 21 in {1.0})
      Predict: 0.0
     Else (feature 21 not in {1.0})
      If (feature 15 in {1.0})
       Predict: 0.0
      Else (feature 15 not in {1.0})
       If (feature 44 in {1.0})
        If (feature 6 <= 301.5)
         If (feature 7 <= 19838.0)
          Predict: 0.0
         Else (feature 7 > 19838.0)
          Predict: 1.0
        Else (feature 6 > 301.5)
         Predict: 0.0
       Else (feature 44 not in {1.0})
        Predict: 0.0
    Else (feature 7 > 45092.0)
     If (feature 16 in {1.0})
      Predict: 0.0
     Else (feature 16 not in {1.0})
      If (feature 1 <= 1.5)
       If (feature 116 in {1.0})
        If (feature 25 in {0.0})
         If (feature 12 <= 22.5)
          Predict: 0.0
         Else (feature 12 > 22.5)
          Predict: 1.0
        Else (feature 25 not in {0.0})
         Predict: 1.0
       Else (feature 116 not in {1.0})
        Predict: 0.0
      Else (feature 1 > 1.5)
       Predict: 0.0
  Tree 26 (weight 1.0):
    If (feature 29 in {1.0})
     If (feature 7 <= 834.0)
      If (feature 10 <= 1.5)
       If (feature 5 <= 25.955)
        Predict: 1.0
       Else (feature 5 > 25.955)
        Predict: 0.0
      Else (feature 10 > 1.5)
       Predict: 0.0
     Else (feature 7 > 834.0)
      If (feature 103 in {1.0})
       Predict: 1.0
      Else (feature 103 not in {1.0})
       Predict: 0.0
    Else (feature 29 not in {1.0})
     If (feature 10 <= 3.5)
      If (feature 4 <= 14.635000000000002)
       If (feature 9 <= 2017.5)
        If (feature 47 in {1.0})
         If (feature 20 in {0.0})
          Predict: 0.0
         Else (feature 20 not in {0.0})
          Predict: 1.0
        Else (feature 47 not in {1.0})
         Predict: 0.0
       Else (feature 9 > 2017.5)
        Predict: 0.0
      Else (feature 4 > 14.635000000000002)
       Predict: 0.0
     Else (feature 10 > 3.5)
      If (feature 4 <= 15.735)
       If (feature 4 <= 7.635)
        If (feature 46 in {1.0})
         If (feature 12 <= 10.5)
          Predict: 1.0
         Else (feature 12 > 10.5)
          Predict: 0.0
        Else (feature 46 not in {1.0})
         Predict: 0.0
       Else (feature 4 > 7.635)
        Predict: 0.0
      Else (feature 4 > 15.735)
       Predict: 0.0
  Tree 27 (weight 1.0):
    If (feature 16 in {1.0})
     If (feature 13 <= 30.5)
      If (feature 9 <= 2017.5)
       If (feature 106 in {1.0})
        If (feature 13 <= 18.5)
         If (feature 4 <= 15.105)
          Predict: 1.0
         Else (feature 4 > 15.105)
          Predict: 0.0
        Else (feature 13 > 18.5)
         If (feature 5 <= 18.924999999999997)
          Predict: 1.0
         Else (feature 5 > 18.924999999999997)
          Predict: 0.0
       Else (feature 106 not in {1.0})
        If (feature 46 in {1.0})
         Predict: 0.0
        Else (feature 46 not in {1.0})
         If (feature 121 in {1.0})
          Predict: 1.0
         Else (feature 121 not in {1.0})
          Predict: 0.0
      Else (feature 9 > 2017.5)
       If (feature 80 in {1.0})
        Predict: 0.0
       Else (feature 80 not in {1.0})
        If (feature 43 in {0.0})
         If (feature 59 in {1.0})
          Predict: 1.0
         Else (feature 59 not in {1.0})
          Predict: 0.0
        Else (feature 43 not in {0.0})
         If (feature 109 in {1.0})
          Predict: 1.0
         Else (feature 109 not in {1.0})
          Predict: 0.0
     Else (feature 13 > 30.5)
      If (feature 71 in {1.0})
       Predict: 0.0
      Else (feature 71 not in {1.0})
       If (feature 81 in {1.0})
        If (feature 9 <= 2017.5)
         If (feature 5 <= 164.945)
          Predict: 0.0
         Else (feature 5 > 164.945)
          Predict: 1.0
        Else (feature 9 > 2017.5)
         Predict: 0.0
       Else (feature 81 not in {1.0})
        Predict: 0.0
    Else (feature 16 not in {1.0})
     If (feature 1 <= 1.5)
      If (feature 15 in {1.0})
       Predict: 0.0
      Else (feature 15 not in {1.0})
       If (feature 10 <= 3.5)
        Predict: 0.0
       Else (feature 10 > 3.5)
        If (feature 123 in {1.0})
         If (feature 19 in {1.0})
          Predict: 0.0
         Else (feature 19 not in {1.0})
          Predict: 1.0
        Else (feature 123 not in {1.0})
         Predict: 0.0
     Else (feature 1 > 1.5)
      If (feature 84 in {1.0})
       If (feature 9 <= 2017.5)
        Predict: 0.0
       Else (feature 9 > 2017.5)
        If (feature 10 <= 3.5)
         If (feature 4 <= 31.689999999999998)
          Predict: 0.0
         Else (feature 4 > 31.689999999999998)
          Predict: 1.0
        Else (feature 10 > 3.5)
         Predict: 0.0
      Else (feature 84 not in {1.0})
       Predict: 0.0
  Tree 28 (weight 1.0):
    If (feature 13 <= 34.5)
     If (feature 10 <= 3.5)
      If (feature 34 in {1.0})
       If (feature 8 <= 1.02575)
        If (feature 9 <= 2017.5)
         Predict: 0.0
        Else (feature 9 > 2017.5)
         If (feature 11 <= 5.5)
          Predict: 1.0
         Else (feature 11 > 5.5)
          Predict: 0.0
       Else (feature 8 > 1.02575)
        If (feature 73 in {1.0})
         Predict: 0.0
        Else (feature 73 not in {1.0})
         Predict: 1.0
      Else (feature 34 not in {1.0})
       Predict: 0.0
     Else (feature 10 > 3.5)
      If (feature 43 in {0.0})
       If (feature 122 in {1.0})
        If (feature 12 <= 18.5)
         Predict: 0.0
        Else (feature 12 > 18.5)
         Predict: 1.0
       Else (feature 122 not in {1.0})
        Predict: 0.0
      Else (feature 43 not in {0.0})
       If (feature 17 in {1.0})
        If (feature 4 <= 67.2)
         Predict: 0.0
        Else (feature 4 > 67.2)
         If (feature 88 in {1.0})
          Predict: 1.0
         Else (feature 88 not in {1.0})
          Predict: 0.0
       Else (feature 17 not in {1.0})
        Predict: 0.0
    Else (feature 13 > 34.5)
     If (feature 66 in {1.0})
      Predict: 0.0
     Else (feature 66 not in {1.0})
      If (feature 5 <= 39.995000000000005)
       If (feature 7 <= 7066.0)
        Predict: 0.0
       Else (feature 7 > 7066.0)
        If (feature 74 in {1.0})
         If (feature 43 in {1.0})
          Predict: 0.0
         Else (feature 43 not in {1.0})
          Predict: 1.0
        Else (feature 74 not in {1.0})
         Predict: 0.0
      Else (feature 5 > 39.995000000000005)
       If (feature 126 in {1.0})
        Predict: 1.0
       Else (feature 126 not in {1.0})
        Predict: 0.0
  Tree 29 (weight 1.0):
    If (feature 53 in {1.0})
     If (feature 13 <= 24.5)
      If (feature 24 in {1.0})
       Predict: 1.0
      Else (feature 24 not in {1.0})
       If (feature 34 in {1.0})
        Predict: 1.0
       Else (feature 34 not in {1.0})
        Predict: 0.0
     Else (feature 13 > 24.5)
      If (feature 34 in {1.0})
       Predict: 1.0
      Else (feature 34 not in {1.0})
       Predict: 0.0
    Else (feature 53 not in {1.0})
     If (feature 16 in {1.0})
      If (feature 14 <= 0.5)
       If (feature 116 in {1.0})
        If (feature 8 <= 0.4958)
         Predict: 1.0
        Else (feature 8 > 0.4958)
         Predict: 0.0
       Else (feature 116 not in {1.0})
        Predict: 0.0
      Else (feature 14 > 0.5)
       Predict: 0.0
     Else (feature 16 not in {1.0})
      If (feature 15 in {1.0})
       If (feature 6 <= 9525.0)
        Predict: 0.0
       Else (feature 6 > 9525.0)
        If (feature 7 <= 1940.0)
         If (feature 11 <= 2.5)
          Predict: 0.0
         Else (feature 11 > 2.5)
          Predict: 1.0
        Else (feature 7 > 1940.0)
         Predict: 0.0
      Else (feature 15 not in {1.0})
       If (feature 17 in {1.0})
        If (feature 44 in {1.0})
         If (feature 88 in {1.0})
          Predict: 1.0
         Else (feature 88 not in {1.0})
          Predict: 0.0
        Else (feature 44 not in {1.0})
         Predict: 0.0
       Else (feature 17 not in {1.0})
        Predict: 0.0

```

## D. Line number code

| Marker | Dòng | Code | Tính năng |
| --- | --- | --- | --- |
| READ_DATASET | 2427-2432 | `data = (<br>            spark.read<br>            .option("header", True)<br>            .option("inferSchema", True)<br>            .csv(DATA_FILE.as_uri())<br>        ).cache()` | Đọc dataset |
| FEATURE_LEAKAGE | 270-277 | `feature_leakage = sorted(<br>        set(danh_sach_feature).intersection(COT_LEAKAGE_CAM)<br>    )<br>    feature_ngoai_danh_sach = sorted(<br>        set(danh_sach_feature) - set(cot_duoc_phep)<br>    )<br>    xac_nhan(<br>        "Không có feature leakage",` | Kiểm tra feature leakage |
| NAN_INFINITY | 291-298 | `ket_qua = df.agg(*[<br>        F.sum(<br>            F.when(<br>                F.isnan(F.col(ten_cot))<br>                \| (F.col(ten_cot) == float("inf"))<br>                \| (F.col(ten_cot) == float("-inf")),<br>                1,<br>            ).otherwise(0)` | Kiểm tra NaN/Infinity |
| LABEL_INPUT | 2466-2473 | `data_model = data.select(<br>            "order_id",<br>            *COT_PHAN_LOAI,<br>            *COT_SO,<br>            F.col("is_late").cast("double").alias("is_late"),<br>        )<br>        for ten_cot in COT_PHAN_LOAI:<br>            data_model = data_model.withColumn(` | Tạo label input |
| SPLIT_TRAIN_TEST | 2491-2495 | `train_full, test = data_model.randomSplit([0.8, 0.2], seed=SEED)<br>        train_full = train_full.orderBy("order_id").cache()<br>        test = test.orderBy("order_id").cache()<br>        so_train_full = train_full.count()<br>        so_test = test.count()` | Chia train_full/test |
| SPLIT_TRAIN_VALIDATION | 2497-2504 | `train_fit, validation = train_full.randomSplit(<br>            [0.8, 0.2],<br>            seed=SEED,<br>        )<br>        train_fit = train_fit.orderBy("order_id").cache()<br>        validation = validation.orderBy("order_id").cache()<br>        so_train_fit = train_fit.count()<br>        so_validation = validation.count()` | Chia train_fit/validation |
| STRING_INDEXER | 354-361 | `indexers = [<br>        StringIndexer(<br>            inputCol=cot,<br>            outputCol=f"{cot}_index",<br>            handleInvalid="keep",<br>        )<br>        for cot in COT_PHAN_LOAI<br>    ]` | StringIndexer |
| ONE_HOT_ENCODER | 363-367 | `encoder = OneHotEncoder(<br>        inputCols=cot_chi_so,<br>        outputCols=cot_ma_hoa,<br>        handleInvalid="keep",<br>    )` | OneHotEncoder |
| IMPUTER | 369-373 | `imputer = Imputer(<br>        inputCols=COT_SO,<br>        outputCols=cot_so_da_dien,<br>        strategy="median",<br>    )` | Imputer |
| VECTOR_ASSEMBLER | 375-380 | `assembler = VectorAssembler(<br>        inputCols=cot_so_da_dien + cot_ma_hoa,<br>        outputCol="features",<br>        handleInvalid="keep",<br>    )<br>    return Pipeline(stages=indexers + [encoder, imputer, assembler])` | VectorAssembler |
| BASELINE_MAJORITY | 674-681 | `counts = (<br>        df_train<br>        .groupBy("is_late")<br>        .count()<br>        .orderBy(F.col("count").desc(), F.col("is_late").asc())<br>        .collect()<br>    )<br>    xac_nhan(` | Tạo baseline |
| FIT_LOGISTIC | 575 | `model_logistic = logistic.fit(df_ready)` | Fit Logistic Regression |
| FIT_RANDOM_FOREST | 577-578 | `model_random_forest = random_forest.fit(df_ready)<br>    return model_logistic, model_random_forest` | Fit Random Forest |
| PROBABILITY_LATE | 584-590 | `return (<br>        model.transform(df_ready)<br>        .withColumn(<br>            "probability_late",<br>            vector_to_array("probability")[1],<br>        )<br>    )` | Tạo probability_late |
| THRESHOLD_SEARCH | 954-961 | `coarse_rows = [<br>        danh_gia_common_threshold(<br>            danh_sach_validation,<br>            threshold,<br>            "coarse",<br>            auc_logistic,<br>            auc_random_forest,<br>        )` | Tìm common threshold |
| SELECT_COMMON_THRESHOLD | 933-939 | `hop_le = [dong for dong in candidates if dong["hop_le_alert_rate"]]<br>    xac_nhan(<br>        "Có common threshold thỏa alert rate của cả hai model",<br>        bool(hop_le),<br>        f"số candidate hợp lệ={len(hop_le)}",<br>    )<br>    return max(hop_le, key=khoa_xep_hang_threshold)` | Chọn common threshold |
| MANUAL_THRESHOLD_COMPARISON | 884-891 | `thresholds = sorted({float(x) for x in danh_sach_threshold})<br>    xac_nhan(<br>        f"Threshold thử thủ công của {ten_tap} nằm trong [0, 1]",<br>        bool(thresholds) and all(0.0 <= x <= 1.0 for x in thresholds),<br>        f"thresholds={thresholds}",<br>    )<br>    rows = []<br>    for threshold in thresholds:` | So sánh threshold thủ công |
| PREDICTION_COMMON | 712-718 | `return df_probability.withColumn(<br>        "prediction_common",<br>        F.when(<br>            F.col("probability_late") >= F.lit(common_threshold),<br>            F.lit(1.0),<br>        ).otherwise(F.lit(0.0)),<br>    )` | Tạo prediction_common |
| CONFUSION_MATRIX | 728-735 | `dong = df_prediction.agg(<br>        F.sum(<br>            F.when(<br>                (F.col("is_late") == 1)<br>                & (F.col("prediction_common") == 1),<br>                1,<br>            ).otherwise(0)<br>        ).alias("tp"),` | Tính confusion matrix |
| METRIC_ACCURACY | 601 | `accuracy = chia_an_toan(tp + tn, n)` | Tính Accuracy |
| METRIC_PRECISION | 603 | `precision = chia_an_toan(tp, tp + fp)` | Tính Precision |
| METRIC_RECALL | 605 | `recall = chia_an_toan(tp, tp + fn)` | Tính Recall |
| METRIC_SPECIFICITY | 607 | `specificity = chia_an_toan(tn, tn + fp)` | Tính Specificity |
| METRIC_FPR | 609 | `fpr = chia_an_toan(fp, fp + tn)` | Tính FPR |
| METRIC_F1 | 611-612 | `f1 = chia_an_toan(2 * precision * recall, precision + recall)<br>    f1_truc_tiep = chia_an_toan(2 * tp, 2 * tp + fp + fn)` | Tính F1 |
| METRIC_ALERT_RATE | 614-621 | `alert_rate = chia_an_toan(tp + fp, n)<br>    prevalence = chia_an_toan(tp + fn, n)<br>    xac_nhan(<br>        "F1 theo hai công thức khớp nhau",<br>        abs(f1 - f1_truc_tiep) <= 1e-12,<br>        f"F1={f1:.17g}, F1 trực tiếp={f1_truc_tiep:.17g}",<br>    )<br>    cac_chi_so = [` | Tính alert rate |
| ROC_POINTS | 991-998 | `scores = [<br>        (<br>            float(dong["probability_late"]),<br>            int(dong["is_late"]),<br>            dong["order_id"],<br>        )<br>        for dong in df_probability.select(<br>            "order_id", "is_late", "probability_late"` | Tính ROC points |
| AUC_TRAPEZOID | 1051-1058 | `dien_tich = (<br>            (tiep_theo["fpr"] - hien_tai["fpr"])<br>            * (tiep_theo["tpr"] + hien_tai["tpr"])<br>            / 2<br>        )<br>        trapezoids.append({<br>            "model": ten_model,<br>            "trapezoid_index": index,` | Tính AUC bằng trapezoid |
| VERIFY_LOGISTIC_SCORE | 1235-1242 | `probability_manual = sigmoid_on_dinh(z_manual)<br>        probability_spark = float(dong["probability_logistic"])<br>        absolute_difference = abs(probability_manual - probability_spark)<br>        xac_nhan(<br>            f"Logistic Regression probability order {dong['alias']} khớp Spark",<br>            absolute_difference <= 1e-10,<br>            "manual="<br>            f"{probability_manual:.17g}, Spark={probability_spark:.17g}, "` | Kiểm chứng Logistic Regression probability |
| VERIFY_RANDOM_FOREST_SCORE | 1319-1326 | `probability_manual = chia_an_toan(raw_model[1], raw_total)<br>        probability_spark = float(dong["probability_random_forest"])<br>        probability_difference = abs(probability_manual - probability_spark)<br>        xac_nhan(<br>            f"Random Forest probability order {dong['alias']} khớp Spark",<br>            probability_difference <= 1e-10,<br>            "manual="<br>            f"{probability_manual:.17g}, Spark={probability_spark:.17g}, "` | Kiểm chứng Random Forest probability |
| SELECT_DEMO_ORDERS | 1117-1124 | `quy_tac = {<br>        "A": (<br>            (F.col("is_late") == 1) & (F.col("prediction_common") == 1),<br>            False,<br>            "True Positive có probability_late cao nhất",<br>        ),<br>        "B": (<br>            (F.col("is_late") == 0) & (F.col("prediction_common") == 1),` | Chọn order A, B, C, D |

## E. Assertion kiểm tra

| Kiểm tra | Trạng thái | Chi tiết |
| --- | --- | --- |
| Không có feature leakage | PASS | feature leakage=[] |
| Feature nằm trong danh sách được xác nhận | PASS | feature ngoài danh sách=[] |
| Dataset có đủ order_id, label và feature | PASS | cột thiếu=[] |
| order_id không null | PASS | order_id null=0 |
| Label không null | PASS | label null=0 |
| Label chỉ nhận 0 hoặc 1 | PASS | label ngoài 0/1=0 |
| Numeric feature không có NaN hoặc Infinity | PASS | số bất thường theo feature={} |
| Số dòng bằng số order_id khác nhau | PASS | dòng=96470, order_id khác nhau=96470 |
| train_full + test bằng toàn bộ data | PASS | 77379+19091=96470 |
| train_fit + validation bằng train_full | PASS | 62054+15325=77379 |
| train_full và test không giao order_id | PASS | giao nhau=0 |
| train_fit và validation không giao order_id | PASS | giao nhau=0 |
| train_fit có đủ hai class để xác định baseline | PASS | counts=[{'is_late': 0.0, 'count': 57828}, {'is_late': 1.0, 'count': 4226}] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.5, 0.0, 0.06831973898858075] |
| Confusion matrix Baseline majority class bằng số dòng tập đánh giá | PASS | TP+TN+FP+FN=15325, số dòng=15325 |
| TP+FN Baseline majority class bằng số late thật | PASS | TP+FN=1047 |
| TN+FP Baseline majority class bằng số not late thật | PASS | TN+FP=14278 |
| TP+FP Baseline majority class bằng số order được cảnh báo | PASS | TP+FP=0, cảnh báo=0 |
| TN+FN Baseline majority class bằng số order không được cảnh báo | PASS | TN+FN=15325, không cảnh báo=15325 |
| AUC baseline validation bằng 0.5 | PASS | AUC=0.5 |
| Preprocessing train_fit giữ nguyên số dòng | PASS | trước=62054, sau=62054 |
| Metadata có tên cho toàn bộ transformed feature | PASS | num_attrs=141, số tên=141 |
| Validation preprocessing giữ nguyên số dòng | PASS | trước=15325, sau=15325 |
| Hai model có đủ probability trên validation | PASS | join=15325, validation=15325 |
| F1 theo hai công thức khớp nhau | PASS | F1=0.12859518383937921, F1 trực tiếp=0.12859518383937921 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.07673735725938009, 0.06872942725477288, 0.997134670487106, 0.009244992295839754, 0.9907550077041603, 0.1285951838393792, 0.6958512993387037, 0.9911908646003262, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.12790129489372098, F1 trực tiếp=0.12790129489372098 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.06831973898858075, 0.06831973898858075, 1.0, 0.0, 1.0, 0.12790129489372098, 0.6845017608457954, 1.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.132262996941896, F1 trực tiếp=0.13226299694189603 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.11125611745513866, 0.07085807904976449, 0.9914040114613181, 0.04671522622215997, 0.95328477377784, 0.132262996941896, 0.6958512993387037, 0.9558890701468189, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.12790129489372098, F1 trực tiếp=0.12790129489372098 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.06831973898858075, 0.06831973898858075, 1.0, 0.0, 1.0, 0.12790129489372098, 0.6845017608457954, 1.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.13956394945146508, F1 trực tiếp=0.13956394945146508 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.19138662316476346, 0.0752527143391988, 0.9598853868194842, 0.1350329177755988, 0.8649670822244012, 0.13956394945146508, 0.6958512993387037, 0.8714518760195759, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.12790129489372098, F1 trực tiếp=0.12790129489372098 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.06831973898858075, 0.06831973898858075, 1.0, 0.0, 1.0, 0.12790129489372098, 0.6845017608457954, 1.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.15223440825012277, F1 trực tiếp=0.15223440825012277 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.3241109298531811, 0.08325127562438457, 0.8882521489971347, 0.2827426810477658, 0.7172573189522342, 0.15223440825012277, 0.6958512993387037, 0.7289396411092985, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.12790129489372098, F1 trực tiếp=0.12790129489372098 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.06831973898858075, 0.06831973898858075, 1.0, 0.0, 1.0, 0.12790129489372098, 0.6845017608457954, 1.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.16844201095072175, F1 trực tiếp=0.16844201095072175 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.45494290375203916, 0.09402089353189598, 0.8080229226361032, 0.4290516879114722, 0.5709483120885278, 0.16844201095072175, 0.6958512993387037, 0.5871451876019576, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.12860038076521527, F1 trực tiếp=0.12860038076521527 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.07412724306688417, 0.06871882383827776, 1.0, 0.006233366017649531, 0.9937666339823504, 0.12860038076521527, 0.6845017608457954, 0.9941924959216966, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.18714321171506576, F1 trực tiếp=0.18714321171506576 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.5725938009787929, 0.10754528597917558, 0.720152817574021, 0.5617733576131111, 0.4382266423868889, 0.18714321171506576, 0.6958512993387037, 0.45748776508972266, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.1563763817740631, F1 trực tiếp=0.1563763817740631 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.38747145187601956, 0.08630952380952381, 0.830945558739255, 0.35495167390390814, 0.6450483260960919, 0.1563763817740631, 0.6845017608457954, 0.6577487765089722, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.20355361596009977, F1 trực tiếp=0.20355361596009974 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.666557911908646, 0.12162413857329112, 0.623686723973257, 0.6697016388849979, 0.3302983611150021, 0.20355361596009977, 0.6958512993387037, 0.3503425774877651, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.19780219780219777, F1 trực tiếp=0.19780219780219779 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.666557911908646, 0.11835431147848957, 0.6017191977077364, 0.6713125087547276, 0.32868749124527247, 0.19780219780219777, 0.6845017608457954, 0.34734094616639477, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21117061973986226, F1 trực tiếp=0.21117061973986229 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7308972267536705, 0.13202583114087538, 0.5272206303724928, 0.7458327496848298, 0.2541672503151702, 0.21117061973986226, 0.6958512993387037, 0.27282218597063623, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22852799594628828, F1 trực tiếp=0.22852799594628831 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.801305057096248, 0.15551724137931033, 0.43075453677172876, 0.8284773777840033, 0.17152262221599665, 0.22852799594628828, 0.6845017608457954, 0.18923327895595432, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22069910897875256, F1 trực tiếp=0.22069910897875258 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7774225122349103, 0.14504504504504503, 0.46131805157593125, 0.800602325255638, 0.19939767474436196, 0.22069910897875256, 0.6958512993387037, 0.2172920065252855, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.24279069767441863, F1 trực tiếp=0.2427906976744186 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.893768352365416, 0.23662737987307345, 0.2492836676217765, 0.9410281552038101, 0.05897184479618994, 0.24279069767441863, 0.6845017608457954, 0.07197389885807504, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22477923468022479, F1 trực tiếp=0.22477923468022479 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8109624796084829, 0.15613382899628253, 0.40114613180515757, 0.8410141476397255, 0.15898585236027454, 0.2247792346802248, 0.6958512993387037, 0.17553017944535074, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.16979819067501739, F1 trực tiếp=0.16979819067501739 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9221533442088091, 0.3128205128205128, 0.11652340019102196, 0.9812298641266284, 0.018770135873371622, 0.1697981906750174, 0.6845017608457954, 0.025448613376835235, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22795031055900619, F1 trực tiếp=0.22795031055900622 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8377814029363785, 0.1688909341923608, 0.35052531041069723, 0.8735116963160107, 0.12648830368398936, 0.2279503105590062, 0.6958512993387037, 0.14179445350734093, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.05272407732864675, F1 trực tiếp=0.05272407732864675 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9296574225122349, 0.32967032967032966, 0.02865329512893983, 0.9957276929541953, 0.0042723070458047345, 0.05272407732864675, 0.6845017608457954, 0.005938009787928222, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22728892055575345, F1 trực tiếp=0.22728892055575348 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8584665579119086, 0.18125, 0.3046800382043935, 0.899075500770416, 0.10092449922958398, 0.22728892055575345, 0.6958512993387037, 0.11484502446982056, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.022201665124884366, F1 trực tiếp=0.022201665124884366 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9310277324632953, 0.35294117647058826, 0.011461318051575931, 0.9984591679506933, 0.0015408320493066256, 0.022201665124884366, 0.6845017608457954, 0.0022185970636215335, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.2271062271062271, F1 trực tiếp=0.2271062271062271 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8760848287112561, 0.19787234042553192, 0.2664756446991404, 0.9207872251015549, 0.07921277489844517, 0.2271062271062271, 0.6958512993387037, 0.09200652528548124, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.018726591760299626, F1 trực tiếp=0.018726591760299626 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.47619047619047616, 0.009551098376313277, 0.9992295839753467, 0.0007704160246533128, 0.018726591760299626, 0.6845017608457954, 0.0013703099510603588, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21801385681293303, F1 trực tiếp=0.21801385681293303 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8895269168026101, 0.2110912343470483, 0.22540592168099333, 0.9382266423868889, 0.06177335761311108, 0.21801385681293303, 0.6958512993387037, 0.07295269168026101, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0075685903500473037, F1 trực tiếp=0.0075685903500473037 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9315497553017944, 0.4, 0.0038204393505253103, 0.9995797730774618, 0.00042022692253817064, 0.007568590350047304, 0.6845017608457954, 0.0006525285481239804, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.2028227914270779, F1 trực tiếp=0.20282279142707788 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9004893964110929, 0.22401847575057737, 0.18529130850047756, 0.9529345846757249, 0.047065415324275106, 0.2028227914270779, 0.6958512993387037, 0.056508972267536706, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9313539967373573, 0.0, 0.0, 0.9996498108978848, 0.0003501891021151422, 0.0, 0.6845017608457954, 0.0003262642740619902, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.18826263800116211, F1 trực tiếp=0.18826263800116211 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.90884176182708, 0.2403560830860534, 0.15472779369627507, 0.9641406359434095, 0.035859364056590556, 0.1882626380011621, 0.6958512993387037, 0.04398042414355628, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.0, 0.0, 0.999929962179577, 7.003782042302843e-05, 0.0, 0.6845017608457954, 6.525285481239805e-05, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.17182564750473783, F1 trực tiếp=0.17182564750473783 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9144535073409462, 0.2537313432835821, 0.12989493791786055, 0.9719848718307886, 0.028015128169211374, 0.17182564750473783, 0.6958512993387037, 0.03497553017944535, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.0, 0.0, 0.999929962179577, 7.003782042302843e-05, 0.0, 0.6845017608457954, 6.525285481239805e-05, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.15003441156228492, F1 trực tiếp=0.15003441156228492 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9194127243066884, 0.2684729064039409, 0.1041069723018147, 0.9791987673343605, 0.020801232665639446, 0.15003441156228492, 0.6958512993387037, 0.026492659053833606, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.0, 0.0, 0.999929962179577, 7.003782042302843e-05, 0.0, 0.6845017608457954, 6.525285481239805e-05, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.11521418020679468, F1 trực tiếp=0.11521418020679468 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9218270799347471, 0.254071661237785, 0.07449856733524356, 0.9839613391231264, 0.01603866087687351, 0.11521418020679468, 0.6958512993387037, 0.0200326264274062, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.0, 0.0, 0.999929962179577, 7.003782042302843e-05, 0.0, 0.6845017608457954, 6.525285481239805e-05, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.090695856137607514, F1 trực tiếp=0.0906958561376075 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9241109298531811, 0.25, 0.055396370582617004, 0.9878134192463931, 0.012186580753606948, 0.09069585613760751, 0.6958512993387037, 0.015138662316476346, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.0, 0.0, 0.999929962179577, 7.003782042302843e-05, 0.0, 0.6845017608457954, 6.525285481239805e-05, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.066938775510204093, F1 trực tiếp=0.066938775510204079 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9254159869494291, 0.2303370786516854, 0.039159503342884434, 0.9904048186020451, 0.009595181397954896, 0.0669387755102041, 0.6958512993387037, 0.011615008156606852, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.0, 0.0, 0.999929962179577, 7.003782042302843e-05, 0.0, 0.6845017608457954, 6.525285481239805e-05, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.052276559865092755, F1 trực tiếp=0.052276559865092748 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9266557911908646, 0.22302158273381295, 0.029608404966571154, 0.992435915394313, 0.007564084605687071, 0.052276559865092755, 0.6958512993387037, 0.009070146818923328, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.0, 0.0, 0.999929962179577, 7.003782042302843e-05, 0.0, 0.6845017608457954, 6.525285481239805e-05, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.041594454072790291, F1 trực tiếp=0.041594454072790298 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9278303425774878, 0.22429906542056074, 0.022922636103151862, 0.9941868609048886, 0.00581313909511136, 0.04159445407279029, 0.6958512993387037, 0.00698205546492659, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.0, 0.0, 0.999929962179577, 7.003782042302843e-05, 0.0, 0.6845017608457954, 6.525285481239805e-05, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.031718061674008813, F1 trực tiếp=0.031718061674008813 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9282871125611746, 0.20454545454545456, 0.017191977077363897, 0.995097352570388, 0.0049026474296119905, 0.03171806167400881, 0.6958512993387037, 0.005742251223491028, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.0, 0.0, 0.999929962179577, 7.003782042302843e-05, 0.0, 0.6845017608457954, 6.525285481239805e-05, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.021582733812949638, F1 trực tiếp=0.021582733812949641 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9290048939641109, 0.18461538461538463, 0.011461318051575931, 0.9962879955175795, 0.0037120044824205073, 0.021582733812949638, 0.6958512993387037, 0.004241435562805873, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.019981834695731154, F1 trực tiếp=0.019981834695731154 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9295921696574225, 0.2037037037037037, 0.010506208213944603, 0.9969883737218098, 0.003011626278190223, 0.019981834695731154, 0.6958512993387037, 0.003523654159869494, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.014719411223551058, F1 trực tiếp=0.014719411223551058 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9301141924959216, 0.2, 0.007640878701050621, 0.997758789746463, 0.0022412102535369098, 0.014719411223551058, 0.6958512993387037, 0.0026101141924959217, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0092592592592592587, F1 trực tiếp=0.0092592592592592587 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.930179445350734, 0.15151515151515152, 0.004775549188156638, 0.9980389410281552, 0.0019610589718447964, 0.009259259259259259, 0.6958512993387037, 0.0021533442088091355, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.007462686567164179, F1 trực tiếp=0.007462686567164179 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9305709624796085, 0.16, 0.0038204393505253103, 0.9985292057711164, 0.001470794228883597, 0.007462686567164179, 0.6958512993387037, 0.0016313213703099511, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0074836295603367634, F1 trực tiếp=0.0074836295603367634 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9307667210440457, 0.18181818181818182, 0.0038204393505253103, 0.9987393192323855, 0.0012606807676145118, 0.007483629560336763, 0.6958512993387037, 0.001435562805872757, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0056338028169014088, F1 trực tiếp=0.0056338028169014088 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9308972267536705, 0.16666666666666666, 0.0028653295128939827, 0.9989494326936545, 0.0010505673063454265, 0.005633802816901409, 0.6958512993387037, 0.0011745513866231647, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0056497175141242929, F1 trực tiếp=0.0056497175141242938 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9310929853181077, 0.2, 0.0028653295128939827, 0.9991595461549236, 0.0008404538450763413, 0.005649717514124293, 0.6958512993387037, 0.0009787928221859706, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0056657223796033997, F1 trực tiếp=0.0056657223796033997 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9312887438825449, 0.25, 0.0028653295128939827, 0.9993696596161927, 0.0006303403838072559, 0.0056657223796034, 0.6958512993387037, 0.0007830342577487765, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0037950664136622387, F1 trực tiếp=0.0037950664136622392 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9314845024469821, 0.2857142857142857, 0.0019102196752626551, 0.9996498108978848, 0.0003501891021151422, 0.0037950664136622387, 0.6958512993387037, 0.0004567699836867863, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0038022813688212923, F1 trực tiếp=0.0038022813688212928 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.4, 0.0019102196752626551, 0.9997898865387309, 0.00021011346126908532, 0.0038022813688212923, 0.6958512993387037, 0.0003262642740619902, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0019029495718363464, F1 trực tiếp=0.0019029495718363464 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9315497553017944, 0.25, 0.0009551098376313276, 0.9997898865387309, 0.00021011346126908532, 0.0019029495718363464, 0.6958512993387037, 0.0002610114192495922, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0019047619047619048, F1 trực tiếp=0.0019047619047619048 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.3333333333333333, 0.0009551098376313276, 0.9998599243591539, 0.00014007564084605686, 0.0019047619047619048, 0.6958512993387037, 0.00019575856443719412, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0019047619047619048, F1 trực tiếp=0.0019047619047619048 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.3333333333333333, 0.0009551098376313276, 0.9998599243591539, 0.00014007564084605686, 0.0019047619047619048, 0.6958512993387037, 0.00019575856443719412, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0019047619047619048, F1 trực tiếp=0.0019047619047619048 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.3333333333333333, 0.0009551098376313276, 0.9998599243591539, 0.00014007564084605686, 0.0019047619047619048, 0.6958512993387037, 0.00019575856443719412, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0019047619047619048, F1 trực tiếp=0.0019047619047619048 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.3333333333333333, 0.0009551098376313276, 0.9998599243591539, 0.00014007564084605686, 0.0019047619047619048, 0.6958512993387037, 0.00019575856443719412, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0019047619047619048, F1 trực tiếp=0.0019047619047619048 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.3333333333333333, 0.0009551098376313276, 0.9998599243591539, 0.00014007564084605686, 0.0019047619047619048, 0.6958512993387037, 0.00019575856443719412, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0019047619047619048, F1 trực tiếp=0.0019047619047619048 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.3333333333333333, 0.0009551098376313276, 0.9998599243591539, 0.00014007564084605686, 0.0019047619047619048, 0.6958512993387037, 0.00019575856443719412, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0019047619047619048, F1 trực tiếp=0.0019047619047619048 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.3333333333333333, 0.0009551098376313276, 0.9998599243591539, 0.00014007564084605686, 0.0019047619047619048, 0.6958512993387037, 0.00019575856443719412, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0019065776930409911, F1 trực tiếp=0.0019065776930409914 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.5, 0.0009551098376313276, 0.999929962179577, 7.003782042302843e-05, 0.0019065776930409911, 0.6958512993387037, 0.0001305057096247961, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0019065776930409911, F1 trực tiếp=0.0019065776930409914 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.5, 0.0009551098376313276, 0.999929962179577, 7.003782042302843e-05, 0.0019065776930409911, 0.6958512993387037, 0.0001305057096247961, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0019065776930409911, F1 trực tiếp=0.0019065776930409914 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.5, 0.0009551098376313276, 0.999929962179577, 7.003782042302843e-05, 0.0019065776930409911, 0.6958512993387037, 0.0001305057096247961, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0019065776930409911, F1 trực tiếp=0.0019065776930409914 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.5, 0.0009551098376313276, 0.999929962179577, 7.003782042302843e-05, 0.0019065776930409911, 0.6958512993387037, 0.0001305057096247961, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0019065776930409911, F1 trực tiếp=0.0019065776930409914 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.5, 0.0009551098376313276, 0.999929962179577, 7.003782042302843e-05, 0.0019065776930409911, 0.6958512993387037, 0.0001305057096247961, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.0019065776930409911, F1 trực tiếp=0.0019065776930409914 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.5, 0.0009551098376313276, 0.999929962179577, 7.003782042302843e-05, 0.0019065776930409911, 0.6958512993387037, 0.0001305057096247961, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316802610114192, 0.0, 0.0, 1.0, 0.0, 0.0, 0.6845017608457954, 0.0, 0.06831973898858075] |
| Có common threshold thỏa alert rate của cả hai model | PASS | số candidate hợp lệ=40 |
| F1 theo hai công thức khớp nhau | PASS | F1=0.20355361596009977, F1 trực tiếp=0.20355361596009974 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.666557911908646, 0.12162413857329112, 0.623686723973257, 0.6697016388849979, 0.3302983611150021, 0.20355361596009977, 0.6958512993387037, 0.3503425774877651, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.19780219780219777, F1 trực tiếp=0.19780219780219779 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.666557911908646, 0.11835431147848957, 0.6017191977077364, 0.6713125087547276, 0.32868749124527247, 0.19780219780219777, 0.6845017608457954, 0.34734094616639477, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.20476947535771065, F1 trực tiếp=0.20476947535771065 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.673605220228385, 0.12283044058744993, 0.6150907354345749, 0.6778960638744922, 0.32210393612550775, 0.20476947535771065, 0.6958512993387037, 0.34212071778140296, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.20327421555252387, F1 trực tiếp=0.20327421555252387 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.6951386623164764, 0.12372846169815238, 0.5692454632282713, 0.704370359994397, 0.29562964000560304, 0.20327421555252387, 0.6845017608457954, 0.3143230016313214, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.20557717250324253, F1 trực tiếp=0.20557717250324253 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.6802610114192496, 0.12380394454208163, 0.6055396370582617, 0.6857402997618715, 0.3142597002381286, 0.20557717250324253, 0.6958512993387037, 0.3341598694942904, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.20481273747059892, F1 trực tiếp=0.20481273747059889 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7132137030995106, 0.1263392857142857, 0.5405921680993314, 0.7258719708642667, 0.2741280291357333, 0.20481273747059892, 0.6845017608457954, 0.2923327895595432, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.20667885030736002, F1 trực tiếp=0.20667885030736002 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.6884176182707994, 0.1251005631536605, 0.5940783190066857, 0.6953354811598264, 0.3046645188401737, 0.20667885030736002, 0.6958512993387037, 0.3244371941272431, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.20653398422831395, F1 trực tiếp=0.20653398422831393 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7242414355628058, 0.12853470437017994, 0.5253104106972302, 0.7388289676425269, 0.261171032357473, 0.20653398422831395, 0.6845017608457954, 0.27921696574225124, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.20695504664970316, F1 trực tiếp=0.20695504664970313 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.6949429037520392, 0.12582508250825084, 0.5826170009551098, 0.7031797170472055, 0.2968202829527945, 0.20695504664970316, 0.6958512993387037, 0.3163458401305057, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21042319749216298, F1 trực tiếp=0.21042319749216301 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7370309951060359, 0.1323638156273108, 0.5128939828080229, 0.7534668721109399, 0.2465331278890601, 0.21042319749216298, 0.6845017608457954, 0.26473083197389885, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.20694420452582479, F1 trực tiếp=0.20694420452582485 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7004241435562806, 0.1263180092787853, 0.5721107927411653, 0.7098333099873932, 0.2901666900126068, 0.2069442045258248, 0.6958512993387037, 0.3094290375203915, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21189364724984777, F1 trực tiếp=0.21189364724984777 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7466231647634584, 0.1345360824742268, 0.498567335243553, 0.7648129990194705, 0.2351870009805295, 0.21189364724984777, 0.6845017608457954, 0.2531810766721044, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.20776014109347446, F1 trực tiếp=0.20776014109347443 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.706884176182708, 0.127406446030716, 0.562559694364852, 0.7174674324135033, 0.2825325675864967, 0.20776014109347446, 0.6958512993387037, 0.30166394779771616, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21192893401015231, F1 trực tiếp=0.21192893401015228 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7568678629690049, 0.1361043194784026, 0.4785100286532951, 0.7772797310547696, 0.22272026894523042, 0.2119289340101523, 0.6845017608457954, 0.2401957585644372, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21026102610261027, F1 trực tiếp=0.21026102610261027 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7137357259380098, 0.129547471162378, 0.5577841451766953, 0.7251715926600364, 0.27482840733996355, 0.21026102610261027, 0.6958512993387037, 0.29415986949429035, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21599820748375531, F1 trực tiếp=0.21599820748375531 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7716802610114193, 0.1411007025761124, 0.4603629417382999, 0.7945090348788346, 0.20549096512116544, 0.2159982074837553, 0.6845017608457954, 0.22290375203915172, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21006243114212267, F1 trực tiếp=0.21006243114212267 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7192822185970636, 0.13002955217094794, 0.5463228271251194, 0.7319652612410702, 0.2680347387589298, 0.21006243114212267, 0.6958512993387037, 0.287047308319739, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.2227488151658768, F1 trực tiếp=0.22274881516587677 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7859706362153345, 0.14812480302552788, 0.448901623686724, 0.8106877713965541, 0.18931222860344585, 0.2227488151658768, 0.6845017608457954, 0.207047308319739, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21092137361606306, F1 trực tiếp=0.21092137361606306 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7256117455138662, 0.13124708080336292, 0.5367717287488061, 0.7394593080263342, 0.2605406919736658, 0.21092137361606306, 0.6958512993387037, 0.2794127243066884, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22695738354806738, F1 trực tiếp=0.22695738354806738 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7964110929853181, 0.15322850451656073, 0.437440305635148, 0.8227342765093151, 0.17726572349068498, 0.22695738354806738, 0.6845017608457954, 0.19504078303425776, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21117061973986226, F1 trực tiếp=0.21117061973986229 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7308972267536705, 0.13202583114087538, 0.5272206303724928, 0.7458327496848298, 0.2541672503151702, 0.21117061973986226, 0.6958512993387037, 0.27282218597063623, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22852799594628828, F1 trực tiếp=0.22852799594628831 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.801305057096248, 0.15551724137931033, 0.43075453677172876, 0.8284773777840033, 0.17152262221599665, 0.22852799594628828, 0.6845017608457954, 0.18923327895595432, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21308920919361121, F1 trực tiếp=0.21308920919361121 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.736378466557912, 0.13383900171274773, 0.5224450811843362, 0.7520661157024794, 0.24793388429752067, 0.2130892091936112, 0.6958512993387037, 0.2666884176182708, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22864864864864867, F1 trực tiếp=0.22864864864864864 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.813768352365416, 0.159442140972484, 0.4040114613180516, 0.8438156604566466, 0.15618433954335342, 0.22864864864864867, 0.6845017608457954, 0.173115823817292, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21311150722915428, F1 trực tiếp=0.21311150722915428 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7407504078303426, 0.1344327836081959, 0.5138490926456543, 0.7573889900546295, 0.2426110099453705, 0.21311150722915428, 0.6958512993387037, 0.26114192495921695, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22137404580152673, F1 trực tiếp=0.22137404580152673 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8269494290375204, 0.15981348028825773, 0.3600764087870105, 0.8611850399215576, 0.13881496007844235, 0.22137404580152673, 0.6845017608457954, 0.15393148450244698, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21461646869337625, F1 trực tiếp=0.21461646869337628 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7454486133768352, 0.13596938775510203, 0.5090735434574976, 0.7627819022272027, 0.23721809777279732, 0.21461646869337625, 0.6958512993387037, 0.25579119086460034, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22692425636307884, F1 trực tiếp=0.22692425636307881 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8354975530179445, 0.16711833785004518, 0.3533906399235912, 0.8708502591399355, 0.12914974086006442, 0.22692425636307884, 0.6845017608457954, 0.14446982055464927, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.2153972153972154, F1 trực tiếp=0.2153972153972154 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7499510603588907, 0.13708626531144122, 0.5023877745940784, 0.7681047765793528, 0.23189522342064714, 0.2153972153972154, 0.6958512993387037, 0.25037520391517126, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.23459244532803181, F1 trực tiếp=0.23459244532803181 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8492659053833606, 0.1796042617960426, 0.33810888252148996, 0.886748844375963, 0.11325115562403698, 0.2345924453280318, 0.6845017608457954, 0.12861337683523655, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.2151660747858784, F1 trực tiếp=0.21516607478587843 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7548450244698206, 0.13770053475935828, 0.4918815663801337, 0.7741280291357333, 0.2258719708642667, 0.2151660747858784, 0.6958512993387037, 0.24404567699836868, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.23901098901098902, F1 trực tiếp=0.23901098901098902 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8553996737357259, 0.1865951742627346, 0.332378223495702, 0.8937526264182659, 0.10624737358173414, 0.23901098901098902, 0.6845017608457954, 0.12169657422512235, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21574468085106382, F1 trực tiếp=0.21574468085106382 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7594779771615008, 0.1387900355871886, 0.48424068767908307, 0.7796610169491526, 0.22033898305084745, 0.21574468085106382, 0.6958512993387037, 0.23836867862969005, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.24078803356439257, F1 trực tiếp=0.24078803356439255 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8642088091353997, 0.19480519480519481, 0.3151862464183381, 0.9044684129429892, 0.09553158705701079, 0.24078803356439257, 0.6845017608457954, 0.11053833605220229, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21581798483206932, F1 trực tiếp=0.21581798483206935 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7638499184339315, 0.13957399103139012, 0.47564469914040114, 0.7849838913013027, 0.2150161086986973, 0.21581798483206932, 0.6958512993387037, 0.23282218597063623, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.24347158218125961, F1 trực tiếp=0.24347158218125961 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8714518760195759, 0.2035966602440591, 0.3027698185291309, 0.9131531026754447, 0.08684689732455526, 0.2434715821812596, 0.6845017608457954, 0.10159869494290376, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21718061674008809, F1 trực tiếp=0.21718061674008809 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7680913539967373, 0.14113942170054394, 0.4708691499522445, 0.7898865387309147, 0.2101134612690853, 0.2171806167400881, 0.6958512993387037, 0.22792822185970638, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.24989858012170382, F1 trực tiếp=0.24989858012170385 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.879347471451876, 0.21720733427362482, 0.2941738299904489, 0.9222580193304384, 0.07774198066956156, 0.24989858012170382, 0.6845017608457954, 0.09252854812398043, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21907968574635242, F1 trực tiếp=0.21907968574635242 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7729853181076672, 0.1431924882629108, 0.46609360076408785, 0.7954895643647569, 0.20451043563524304, 0.21907968574635242, 0.6958512993387037, 0.22238172920065252, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.24850042844901457, F1 trực tiếp=0.24850042844901457 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8855464926590538, 0.22533022533022534, 0.276981852913085, 0.9301722930382407, 0.06982770696175936, 0.24850042844901457, 0.6845017608457954, 0.08398042414355628, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22069910897875256, F1 trực tiếp=0.22069910897875258 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7774225122349103, 0.14504504504504503, 0.46131805157593125, 0.800602325255638, 0.19939767474436196, 0.22069910897875256, 0.6958512993387037, 0.2172920065252855, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.24279069767441863, F1 trực tiếp=0.2427906976744186 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.893768352365416, 0.23662737987307345, 0.2492836676217765, 0.9410281552038101, 0.05897184479618994, 0.24279069767441863, 0.6845017608457954, 0.07197389885807504, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22232536551404039, F1 trực tiếp=0.22232536551404039 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7813376835236542, 0.1468424279583078, 0.4574976122254059, 0.8050847457627118, 0.19491525423728814, 0.2223253655140404, 0.6958512993387037, 0.2128548123980424, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.24206930209858465, F1 trực tiếp=0.24206930209858468 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8986623164763459, 0.24750499001996007, 0.23686723973256923, 0.9471914834010365, 0.05280851659896344, 0.24206930209858465, 0.6845017608457954, 0.06538336052202284, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22295701464336323, F1 trực tiếp=0.22295701464336326 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7853181076672104, 0.14810166300596173, 0.45081184336198665, 0.8098473175514778, 0.1901526824485222, 0.22295701464336323, 0.6958512993387037, 0.20796084828711256, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.2317198764160659, F1 trực tiếp=0.23171987641606592 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9026427406199021, 0.25139664804469275, 0.2148997134670487, 0.953074660316571, 0.04692533968342905, 0.2317198764160659, 0.6845017608457954, 0.058401305057096245, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22291616622627913, F1 trực tiếp=0.22291616622627913 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7889070146818923, 0.14890885750962773, 0.44317096466093603, 0.8142597002381285, 0.1857402997618714, 0.22291616622627913, 0.6958512993387037, 0.2033278955954323, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22734761120263589, F1 trực tiếp=0.22734761120263591 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.908189233278956, 0.26744186046511625, 0.1977077363896848, 0.9602885558201428, 0.03971144417985712, 0.22734761120263589, 0.6845017608457954, 0.050505709624796086, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22390613541921292, F1 trực tiếp=0.22390613541921289 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7928221859706363, 0.1504599211563732, 0.437440305635148, 0.8188821963860484, 0.18111780361395152, 0.22390613541921292, 0.6958512993387037, 0.19862969004893963, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22005730659025791, F1 trực tiếp=0.22005730659025788 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9111908646003263, 0.27507163323782235, 0.1833810888252149, 0.9645608628659477, 0.03543913713405239, 0.2200573065902579, 0.6845017608457954, 0.04554649265905383, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22420193021529328, F1 trực tiếp=0.22420193021529325 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7954323001631322, 0.15130260521042085, 0.4326647564469914, 0.8220338983050848, 0.17796610169491525, 0.22420193021529328, 0.6958512993387037, 0.19536704730831975, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.20818291215403129, F1 trực tiếp=0.20818291215403129 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9141272430668842, 0.2813008130081301, 0.16523400191021967, 0.9690432833730215, 0.03095671662697857, 0.20818291215403129, 0.6845017608457954, 0.04013050570962479, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22445898339204831, F1 trực tiếp=0.22445898339204831 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7988907014681892, 0.15237444482405194, 0.4259789875835721, 0.8262361675304665, 0.17376383246953356, 0.2244589833920483, 0.6958512993387037, 0.19099510603588907, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.19899874843554446, F1 trực tiếp=0.19899874843554444 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9164763458401305, 0.2885662431941924, 0.1518624641833811, 0.9725451743941729, 0.027454825605827148, 0.19899874843554446, 0.6845017608457954, 0.03595432300163132, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22415557830092123, F1 trực tiếp=0.2241555783009212 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8021533442088091, 0.15309332401258302, 0.4183381088825215, 0.8302983611150021, 0.1697016388849979, 0.22415557830092123, 0.6958512993387037, 0.1866884176182708, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.1940874035989717, F1 trực tiếp=0.19408740359897173 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9181729200652529, 0.2966601178781925, 0.14422158548233047, 0.9749264602885558, 0.02507353971144418, 0.1940874035989717, 0.6845017608457954, 0.033213703099510605, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22424557752341312, F1 trực tiếp=0.22424557752341312 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8054159869494291, 0.15409367179120487, 0.41165234001910217, 0.8342905168791147, 0.16570948312088526, 0.22424557752341312, 0.6958512993387037, 0.18251223491027732, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.19291338582677164, F1 trực tiếp=0.19291338582677164 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9197389885807504, 0.3081761006289308, 0.14040114613180515, 0.9768875192604006, 0.023112480739599383, 0.19291338582677164, 0.6845017608457954, 0.031125611745513867, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22433359725521246, F1 trực tiếp=0.22433359725521246 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8082218597063622, 0.1549963530269876, 0.4059216809933142, 0.8377223700798431, 0.16227762992015687, 0.22433359725521246, 0.6958512993387037, 0.17892332789559542, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.17922606924643583, F1 trực tiếp=0.17922606924643583 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9211092985318108, 0.30985915492957744, 0.12607449856733524, 0.9794088807956296, 0.02059111920437036, 0.17922606924643583, 0.6845017608457954, 0.027797716150081565, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22477923468022479, F1 trực tiếp=0.22477923468022479 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8109624796084829, 0.15613382899628253, 0.40114613180515757, 0.8410141476397255, 0.15898585236027454, 0.2247792346802248, 0.6958512993387037, 0.17553017944535074, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.16979819067501739, F1 trực tiếp=0.16979819067501739 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9221533442088091, 0.3128205128205128, 0.11652340019102196, 0.9812298641266284, 0.018770135873371622, 0.1697981906750174, 0.6845017608457954, 0.025448613376835235, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22488429077048733, F1 trực tiếp=0.22488429077048733 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8142251223491028, 0.15727341964965727, 0.3944603629417383, 0.8450063034038381, 0.15499369659616194, 0.22488429077048733, 0.6958512993387037, 0.17135399673735727, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.16005767844268204, F1 trực tiếp=0.16005767844268204 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9239804241435563, 0.3264705882352941, 0.10601719197707736, 0.9839613391231264, 0.01603866087687351, 0.16005767844268204, 0.6845017608457954, 0.022185970636215333, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22542232068679038, F1 trực tiếp=0.22542232068679036 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8174877650897227, 0.15873634945397816, 0.38872970391595035, 0.8489284213475277, 0.15107157865247234, 0.22542232068679038, 0.6958512993387037, 0.16730831973898858, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.15158204562178068, F1 trực tiếp=0.15158204562178071 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.924763458401305, 0.3301282051282051, 0.09837631327602674, 0.985362095531587, 0.014637904468412942, 0.15158204562178068, 0.6845017608457954, 0.02035889070146819, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.2247191011235955, F1 trực tiếp=0.2247191011235955 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8199021207177815, 0.15917230401910068, 0.38204393505253104, 0.8520100854461409, 0.1479899145538591, 0.2247191011235955, 0.6958512993387037, 0.1639804241435563, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.14681647940074907, F1 trực tiếp=0.14681647940074907 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9256769983686787, 0.3402777777777778, 0.0936007640878701, 0.9866928141196246, 0.013307185880375402, 0.14681647940074907, 0.6845017608457954, 0.018792822185970635, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.2260176487332764, F1 trực tiếp=0.2260176487332764 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8225774877650898, 0.16098945660989455, 0.37917860553963706, 0.8550917495447542, 0.14490825045524583, 0.2260176487332764, 0.6958512993387037, 0.16091353996737356, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.14078041315990819, F1 trực tiếp=0.14078041315990819 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.926721044045677, 0.35384615384615387, 0.08787010506208214, 0.9882336461689312, 0.011766353831068777, 0.1407804131599082, 0.6845017608457954, 0.016965742251223492, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22588099364529174, F1 trực tiếp=0.22588099364529174 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8251223491027733, 0.1619047619047619, 0.3734479465138491, 0.8582434514637904, 0.14175654853620956, 0.22588099364529174, 0.6958512993387037, 0.15758564437194128, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.1192368839427663, F1 trực tiếp=0.1192368839427663 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.927699836867863, 0.35545023696682465, 0.07163323782234957, 0.9904748564224681, 0.009525143577531868, 0.1192368839427663, 0.6845017608457954, 0.013768352365415987, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22736595370641666, F1 trực tiếp=0.22736595370641663 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8279282218597064, 0.16398985629754861, 0.37058261700095513, 0.8614651912032497, 0.13853480879675026, 0.22736595370641666, 0.6958512993387037, 0.15438825448613377, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.10705596107055962, F1 trực tiếp=0.1070559610705596 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9281566068515498, 0.3548387096774194, 0.06303724928366762, 0.9915954615492366, 0.008404538450763412, 0.10705596107055962, 0.6845017608457954, 0.012137030995106036, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22711058263971462, F1 trực tiếp=0.22711058263971462 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8303425774877651, 0.16486836426413465, 0.3648519579751671, 0.86447681748144, 0.13552318251856002, 0.22711058263971462, 0.6958512993387037, 0.15119086460032627, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.10024650780608052, F1 trực tiếp=0.10024650780608052 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9285481239804242, 0.3588235294117647, 0.05826170009551098, 0.9923658775738899, 0.0076341224261101, 0.10024650780608052, 0.6845017608457954, 0.011092985318107667, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.2273276904474002, F1 trực tiếp=0.22732769044740025 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8332137030995106, 0.16629809818664307, 0.35912129894937916, 0.8679787085025914, 0.1320212914974086, 0.2273276904474002, 0.6958512993387037, 0.14753670473083197, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.074829931972789115, F1 trực tiếp=0.074829931972789115 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9290048939641109, 0.34108527131782945, 0.04202483285577841, 0.9940467852640426, 0.005953214735957417, 0.07482993197278912, 0.6845017608457954, 0.008417618270799348, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22597482345716915, F1 trực tiếp=0.22597482345716918 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8354975530179445, 0.1665158371040724, 0.35148042024832854, 0.8709903347807816, 0.1290096652192184, 0.22597482345716915, 0.6958512993387037, 0.14420880913539968, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.060816681146828845, F1 trực tiếp=0.060816681146828845 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9294616639477977, 0.33653846153846156, 0.033428844317096466, 0.995167390390811, 0.004832609609188962, 0.060816681146828845, 0.6845017608457954, 0.006786296900489396, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22795031055900619, F1 trực tiếp=0.22795031055900622 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8377814029363785, 0.1688909341923608, 0.35052531041069723, 0.8735116963160107, 0.12648830368398936, 0.2279503105590062, 0.6958512993387037, 0.14179445350734093, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.05272407732864675, F1 trực tiếp=0.05272407732864675 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9296574225122349, 0.32967032967032966, 0.02865329512893983, 0.9957276929541953, 0.0042723070458047345, 0.05272407732864675, 0.6845017608457954, 0.005938009787928222, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22867972204674672, F1 trực tiếp=0.22867972204674669 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.840652528548124, 0.1708352996696555, 0.3457497612225406, 0.876943549516739, 0.12305645048326096, 0.22867972204674672, 0.6958512993387037, 0.13827079934747144, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.046181172291296618, F1 trực tiếp=0.046181172291296625 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9299184339314845, 0.3291139240506329, 0.024832855778414518, 0.9962879955175795, 0.0037120044824205073, 0.04618117229129662, 0.6845017608457954, 0.0051549755301794455, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22970805261469365, F1 trực tiếp=0.22970805261469363 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8433278955954323, 0.17294685990338166, 0.3419293218720153, 0.8800952514357753, 0.11990474856422469, 0.22970805261469365, 0.6958512993387037, 0.13507340946166393, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.037803780378037805, F1 trực tiếp=0.037803780378037805 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9302446982055464, 0.328125, 0.02005730659025788, 0.9969883737218098, 0.003011626278190223, 0.037803780378037805, 0.6845017608457954, 0.004176182707993475, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22946544980443284, F1 trực tiếp=0.22946544980443284 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.845742251223491, 0.17417120237506184, 0.3361986628462273, 0.8831068777139656, 0.11689312228603446, 0.22946544980443284, 0.6958512993387037, 0.13187601957585646, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.029143897996357013, F1 trực tiếp=0.029143897996357013 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9304404567699837, 0.3137254901960784, 0.015281757402101241, 0.9975486762851941, 0.0024513237148059953, 0.029143897996357013, 0.6845017608457954, 0.0033278955954323002, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.23043908880818753, F1 trực tiếp=0.23043908880818753 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8478955954323002, 0.17608476286579214, 0.3333333333333333, 0.8856282392491945, 0.11437176075080543, 0.23043908880818753, 0.6958512993387037, 0.12933115823817293, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.027447392497712716, F1 trực tiếp=0.027447392497712716 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9306362153344209, 0.32608695652173914, 0.014326647564469915, 0.9978288275668861, 0.0021711724331138814, 0.027447392497712716, 0.6845017608457954, 0.00300163132137031, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22960725075528698, F1 trực tiếp=0.22960725075528701 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8502446982055465, 0.17701863354037267, 0.32664756446991405, 0.8886398655273848, 0.11136013447261521, 0.22960725075528698, 0.6958512993387037, 0.12606851549755302, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.025664527956003668, F1 trực tiếp=0.025664527956003668 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9306362153344209, 0.3181818181818182, 0.013371537726838587, 0.9978988653873091, 0.002101134612690853, 0.025664527956003668, 0.6845017608457954, 0.002871125611745514, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.2294636795655125, F1 trực tiếp=0.22946367956551256 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8518760195758565, 0.1779884149552396, 0.3228271251193887, 0.8906709623196526, 0.1093290376803474, 0.2294636795655125, 0.6958512993387037, 0.12391517128874388, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.025688073394495414, F1 trực tiếp=0.025688073394495414 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9307014681892333, 0.32558139534883723, 0.013371537726838587, 0.9979689032077321, 0.0020310967922678247, 0.025688073394495414, 0.6845017608457954, 0.0028058727569331156, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.2274914089347079, F1 trực tiếp=0.2274914089347079 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8533115823817292, 0.17767042404723565, 0.31614135625596945, 0.8927020591119205, 0.10729794088807956, 0.2274914089347079, 0.6958512993387037, 0.12156606851549755, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.023919043238270467, F1 trực tiếp=0.02391904323827047 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9307667210440457, 0.325, 0.012416427889207259, 0.9981089788485782, 0.0018910211514217678, 0.023919043238270467, 0.6845017608457954, 0.0026101141924959217, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22592978797358357, F1 trực tiếp=0.2259297879735836 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8546818923327896, 0.17759562841530055, 0.3104106972301815, 0.8945930802633422, 0.10540691973665779, 0.22592978797358357, 0.6958512993387037, 0.11941272430668842, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.023963133640552997, F1 trực tiếp=0.023963133640552997 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9308972267536705, 0.34210526315789475, 0.012416427889207259, 0.9982490544894242, 0.001750945510575711, 0.023963133640552997, 0.6845017608457954, 0.002479608482871126, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22629538244624603, F1 trực tiếp=0.22629538244624603 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8567699836867863, 0.1793296089385475, 0.30659025787965616, 0.8971144417985712, 0.10288555820142878, 0.22629538244624603, 0.6958512993387037, 0.11680261011419249, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.024029574861367836, F1 trực tiếp=0.024029574861367836 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9310929853181077, 0.37142857142857144, 0.012416427889207259, 0.9984591679506933, 0.0015408320493066256, 0.024029574861367836, 0.6845017608457954, 0.0022838499184339315, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22728892055575345, F1 trực tiếp=0.22728892055575348 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8584665579119086, 0.18125, 0.3046800382043935, 0.899075500770416, 0.10092449922958398, 0.22728892055575345, 0.6958512993387037, 0.11484502446982056, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.022201665124884366, F1 trực tiếp=0.022201665124884366 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9310277324632953, 0.35294117647058826, 0.011461318051575931, 0.9984591679506933, 0.0015408320493066256, 0.022201665124884366, 0.6845017608457954, 0.0022185970636215335, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22776572668112802, F1 trực tiếp=0.22776572668112799 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8606199021207178, 0.18324607329842932, 0.3008595988538682, 0.9016669001260681, 0.09833309987393192, 0.22776572668112802, 0.6958512993387037, 0.11216965742251224, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.022222222222222223, F1 trực tiếp=0.022222222222222223 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9310929853181077, 0.36363636363636365, 0.011461318051575931, 0.9985292057711164, 0.001470794228883597, 0.022222222222222223, 0.6845017608457954, 0.0021533442088091355, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22800586510263932, F1 trực tiếp=0.22800586510263929 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8625774877650897, 0.18500892325996432, 0.29703915950334286, 0.904048186020451, 0.09595181397954895, 0.22800586510263932, 0.6958512993387037, 0.10969004893964111, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.022222222222222223, F1 trực tiếp=0.022222222222222223 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9310929853181077, 0.36363636363636365, 0.011461318051575931, 0.9985292057711164, 0.001470794228883597, 0.022222222222222223, 0.6845017608457954, 0.0021533442088091355, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22759390107846783, F1 trực tiếp=0.22759390107846783 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8644698205546493, 0.18635809987819732, 0.2922636103151863, 0.906429471914834, 0.09357052808516599, 0.22759390107846783, 0.6958512993387037, 0.10714518760195758, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.022242817423540315, F1 trực tiếp=0.022242817423540315 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9311582381729201, 0.375, 0.011461318051575931, 0.9985992435915394, 0.0014007564084605687, 0.022242817423540315, 0.6845017608457954, 0.0020880913539967376, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22773393461104846, F1 trực tiếp=0.22773393461104849 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.865905383360522, 0.18773234200743494, 0.28939828080229224, 0.9081804174254097, 0.09181958257459027, 0.22773393461104846, 0.6958512993387037, 0.10531810766721043, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.022242817423540315, F1 trực tiếp=0.022242817423540315 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9311582381729201, 0.375, 0.011461318051575931, 0.9985992435915394, 0.0014007564084605687, 0.022242817423540315, 0.6845017608457954, 0.0020880913539967376, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22892025944296066, F1 trực tiếp=0.22892025944296071 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8681239804241435, 0.1905972045743329, 0.28653295128939826, 0.9107718167810618, 0.08922818321893823, 0.22892025944296066, 0.6958512993387037, 0.10270799347471452, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.020446096654275089, F1 trực tiếp=0.020446096654275093 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9312234910277325, 0.3793103448275862, 0.010506208213944603, 0.9987393192323855, 0.0012606807676145118, 0.02044609665427509, 0.6845017608457954, 0.0018923327895595432, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22505800464037123, F1 trực tiếp=0.22505800464037123 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8692332789559544, 0.18908382066276802, 0.27793696275071633, 0.9125928001120606, 0.08740719988793949, 0.22505800464037123, 0.6958512993387037, 0.10042414355628058, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.020503261882572225, F1 trực tiếp=0.020503261882572229 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9314192495921697, 0.4230769230769231, 0.010506208213944603, 0.9989494326936545, 0.0010505673063454265, 0.020503261882572225, 0.6845017608457954, 0.001696574225122349, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.2277886497064579, F1 trực tiếp=0.22778864970645793 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8712561174551386, 0.1929708222811671, 0.27793696275071633, 0.9147639725451744, 0.0852360274548256, 0.2277886497064579, 0.6958512993387037, 0.09840130505709625, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.018709073900841908, F1 trực tiếp=0.018709073900841908 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9315497553017944, 0.45454545454545453, 0.009551098376313277, 0.9991595461549236, 0.0008404538450763413, 0.018709073900841908, 0.6845017608457954, 0.001435562805872757, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22601110229976212, F1 trực tiếp=0.2260111022997621 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.872626427406199, 0.19322033898305085, 0.2722063037249284, 0.9166549936965962, 0.08334500630340384, 0.22601110229976212, 0.6958512993387037, 0.09624796084828711, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.018709073900841908, F1 trực tiếp=0.018709073900841908 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9315497553017944, 0.45454545454545453, 0.009551098376313277, 0.9991595461549236, 0.0008404538450763413, 0.018709073900841908, 0.6845017608457954, 0.001435562805872757, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22641509433962262, F1 trực tiếp=0.22641509433962265 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.874257748776509, 0.19529085872576177, 0.2693409742120344, 0.9186160526684409, 0.08138394733155904, 0.22641509433962262, 0.6958512993387037, 0.09422512234910277, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.018726591760299626, F1 trực tiếp=0.018726591760299626 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.47619047619047616, 0.009551098376313277, 0.9992295839753467, 0.0007704160246533128, 0.018726591760299626, 0.6845017608457954, 0.0013703099510603588, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.2271062271062271, F1 trực tiếp=0.2271062271062271 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8760848287112561, 0.19787234042553192, 0.2664756446991404, 0.9207872251015549, 0.07921277489844517, 0.2271062271062271, 0.6958512993387037, 0.09200652528548124, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.018726591760299626, F1 trực tiếp=0.018726591760299626 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9316150081566068, 0.47619047619047616, 0.009551098376313277, 0.9992295839753467, 0.0007704160246533128, 0.018726591760299626, 0.6845017608457954, 0.0013703099510603588, 0.06831973898858075] |
| Có common threshold thỏa alert rate của cả hai model | PASS | số candidate hợp lệ=37 |
| Lần chạy official tự động chọn common threshold bằng F1 trên validation | PASS | run_tag=official, threshold_mode=AUTO_VALIDATION |
| Threshold thử thủ công của validation nằm trong [0, 1] | PASS | thresholds=[0.09, 0.094, 0.1] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22069910897875256, F1 trực tiếp=0.22069910897875258 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7774225122349103, 0.14504504504504503, 0.46131805157593125, 0.800602325255638, 0.19939767474436196, 0.22069910897875256, 0.6958512993387037, 0.2172920065252855, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.24279069767441863, F1 trực tiếp=0.2427906976744186 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.893768352365416, 0.23662737987307345, 0.2492836676217765, 0.9410281552038101, 0.05897184479618994, 0.24279069767441863, 0.6845017608457954, 0.07197389885807504, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22390613541921292, F1 trực tiếp=0.22390613541921289 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7928221859706363, 0.1504599211563732, 0.437440305635148, 0.8188821963860484, 0.18111780361395152, 0.22390613541921292, 0.6958512993387037, 0.19862969004893963, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22005730659025791, F1 trực tiếp=0.22005730659025788 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9111908646003263, 0.27507163323782235, 0.1833810888252149, 0.9645608628659477, 0.03543913713405239, 0.2200573065902579, 0.6845017608457954, 0.04554649265905383, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22477923468022479, F1 trực tiếp=0.22477923468022479 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.8109624796084829, 0.15613382899628253, 0.40114613180515757, 0.8410141476397255, 0.15898585236027454, 0.2247792346802248, 0.6958512993387037, 0.17553017944535074, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.16979819067501739, F1 trực tiếp=0.16979819067501739 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9221533442088091, 0.3128205128205128, 0.11652340019102196, 0.9812298641266284, 0.018770135873371622, 0.1697981906750174, 0.6845017608457954, 0.025448613376835235, 0.06831973898858075] |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22390613541921292, F1 trực tiếp=0.22390613541921289 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7928221859706363, 0.1504599211563732, 0.437440305635148, 0.8188821963860484, 0.18111780361395152, 0.22390613541921292, 0.6958512993387037, 0.19862969004893963, 0.06831973898858075] |
| Confusion matrix Logistic Regression bằng số dòng tập đánh giá | PASS | TP+TN+FP+FN=15325, số dòng=15325 |
| TP+FN Logistic Regression bằng số late thật | PASS | TP+FN=1047 |
| TN+FP Logistic Regression bằng số not late thật | PASS | TN+FP=14278 |
| TP+FP Logistic Regression bằng số order được cảnh báo | PASS | TP+FP=3044, cảnh báo=3044 |
| TN+FN Logistic Regression bằng số order không được cảnh báo | PASS | TN+FN=12281, không cảnh báo=12281 |
| F1 theo hai công thức khớp nhau | PASS | F1=0.22005730659025791, F1 trực tiếp=0.22005730659025788 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9111908646003263, 0.27507163323782235, 0.1833810888252149, 0.9645608628659477, 0.03543913713405239, 0.2200573065902579, 0.6845017608457954, 0.04554649265905383, 0.06831973898858075] |
| Confusion matrix Random Forest bằng số dòng tập đánh giá | PASS | TP+TN+FP+FN=15325, số dòng=15325 |
| TP+FN Random Forest bằng số late thật | PASS | TP+FN=1047 |
| TN+FP Random Forest bằng số not late thật | PASS | TN+FP=14278 |
| TP+FP Random Forest bằng số order được cảnh báo | PASS | TP+FP=698, cảnh báo=698 |
| TN+FN Random Forest bằng số order không được cảnh báo | PASS | TN+FN=14627, không cảnh báo=14627 |
| Validation Logistic Regression khớp bảng candidate được chọn | PASS | model=Logistic Regression |
| Validation Random Forest khớp bảng candidate được chọn | PASS | model=Random Forest |
| Common threshold chỉ được chọn từ validation | PASS | threshold=0.094, mode=AUTO_VALIDATION |
| Model demo chỉ được chọn từ metrics validation | PASS | model=Logistic Regression, tie-break=F1/Recall/Precision/AUC/tên cố định |
| train_full có đủ hai class để xác định baseline | PASS | counts=[{'is_late': 0.0, 'count': 72106}, {'is_late': 1.0, 'count': 5273}] |
| F1 theo hai công thức khớp nhau | PASS | F1=0, F1 trực tiếp=0 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9339479335812687, 0.0, 0.0, 1.0, 0.0, 0.0, 0.5, 0.0, 0.06605206641873133] |
| Confusion matrix Baseline majority class bằng số dòng tập đánh giá | PASS | TP+TN+FP+FN=19091, số dòng=19091 |
| TP+FN Baseline majority class bằng số late thật | PASS | TP+FN=1261 |
| TN+FP Baseline majority class bằng số not late thật | PASS | TN+FP=17830 |
| TP+FP Baseline majority class bằng số order được cảnh báo | PASS | TP+FP=0, cảnh báo=0 |
| TN+FN Baseline majority class bằng số order không được cảnh báo | PASS | TN+FN=19091, không cảnh báo=19091 |
| AUC baseline test bằng 0.5 | PASS | AUC=0.5 |
| Preprocessing train_full giữ nguyên số dòng | PASS | trước=77379, sau=77379 |
| Metadata có tên cho toàn bộ transformed feature | PASS | num_attrs=141, số tên=141 |
| Test preprocessing giữ nguyên số dòng | PASS | trước=19091, sau=19091 |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21388512860181785, F1 trực tiếp=0.21388512860181783 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.7870724425121786, 0.14143222506393863, 0.43854084060269627, 0.8117218171620864, 0.18827818283791362, 0.21388512860181785, 0.7018961350991807, 0.20480854853072128, 0.06605206641873133] |
| Confusion matrix Logistic Regression bằng số dòng tập đánh giá | PASS | TP+TN+FP+FN=19091, số dòng=19091 |
| TP+FN Logistic Regression bằng số late thật | PASS | TP+FN=1261 |
| TN+FP Logistic Regression bằng số not late thật | PASS | TN+FP=17830 |
| TP+FP Logistic Regression bằng số order được cảnh báo | PASS | TP+FP=3910, cảnh báo=3910 |
| TN+FN Logistic Regression bằng số order không được cảnh báo | PASS | TN+FN=15181, không cảnh báo=15181 |
| F1 theo hai công thức khớp nhau | PASS | F1=0.21186813186813186, F1 trực tiếp=0.21186813186813186 |
| Toàn bộ metrics hữu hạn | PASS | metrics=[0.9060813996123828, 0.23767258382642997, 0.19111816019032513, 0.9566461020751542, 0.043353897924845765, 0.21186813186813186, 0.6872840595579981, 0.053114032790320044, 0.06605206641873133] |
| Confusion matrix Random Forest bằng số dòng tập đánh giá | PASS | TP+TN+FP+FN=19091, số dòng=19091 |
| TP+FN Random Forest bằng số late thật | PASS | TP+FN=1261 |
| TN+FP Random Forest bằng số not late thật | PASS | TN+FP=17830 |
| TP+FP Random Forest bằng số order được cảnh báo | PASS | TP+FP=1014, cảnh báo=1014 |
| TN+FN Random Forest bằng số order không được cảnh báo | PASS | TN+FN=18077, không cảnh báo=18077 |
| Hai model có đủ probability trên test | PASS | join=19091, test=19091 |
| Logistic Regression có cả hai label để tạo ROC curve | PASS | positive=1261, negative=17830 |
| AUC_manual Logistic Regression khớp AUC_Spark | PASS | AUC_manual=0.70189613509918103, AUC_Spark=0.7018961350991807, độ lệch=3.3306690738754696e-16 |
| Random Forest có cả hai label để tạo ROC curve | PASS | positive=1261, negative=17830 |
| AUC_manual Random Forest khớp AUC_Spark | PASS | AUC_manual=0.68728405955799843, AUC_Spark=0.68728405955799809, độ lệch=3.3306690738754696e-16 |
| Baseline majority class có cả hai label để tạo ROC curve | PASS | positive=1261, negative=17830 |
| AUC_manual Baseline majority class khớp AUC_Spark | PASS | AUC_manual=0.5, AUC_Spark=0.5, độ lệch=0 |
| Chọn được order A | PASS | quy tắc=True Positive có probability_late cao nhất |
| Chọn được order B | PASS | quy tắc=False Positive có probability_late cao nhất |
| Chọn được order C | PASS | quy tắc=False Negative gần common threshold nhất ở phía dưới |
| Chọn được order D | PASS | quy tắc=True Negative có probability_late thấp nhất |
| Thu được đúng bốn order demo khác nhau | PASS | order=[('A', 'f46b842d9b4dfd29acf5eec998837ede'), ('B', '686c0ba20be3837a5041edbc39d3f9ae'), ('C', '9b1d71b20edcf15ab15e0bb4a932f23f'), ('D', 'c2bb89b5c1dd978d507284be78a04cb2')] |
| Số Logistic Regression coefficients bằng features vector | PASS | coefficients=141, features=141 |
| Logistic Regression probability order A khớp Spark | PASS | manual=0.39798931891486933, Spark=0.39798931891486933, độ lệch=0 |
| Logistic Regression probability order B khớp Spark | PASS | manual=0.73944765651953459, Spark=0.73944765651954547, độ lệch=1.0880185641326534e-14 |
| Logistic Regression probability order C khớp Spark | PASS | manual=0.093995718010235374, Spark=0.093995718010240203, độ lệch=4.829470157119431e-15 |
| Logistic Regression probability order D khớp Spark | PASS | manual=0.0001448519905773873, Spark=0.00014485199057734377, độ lệch=4.3530717225293003e-17 |
| Số Random Forest feature importance bằng features vector | PASS | importances=141, features=141 |
| Random Forest có đúng số decision tree đã cấu hình | PASS | trees=30, weights=30 |
| Random Forest probability order A khớp Spark | PASS | manual=0.13350572426609683, Spark=0.13350572426609683, độ lệch=0 |
| Order A có leaf index của mọi decision tree | PASS | leaf indices=30, trees=30 |
| Leaf index tree 0 order A khớp leafCol | PASS | predictLeaf=9.0, leafCol=9.0 |
| Leaf index tree 1 order A khớp leafCol | PASS | predictLeaf=0.0, leafCol=0.0 |
| Leaf index tree 2 order A khớp leafCol | PASS | predictLeaf=11.0, leafCol=11.0 |
| Leaf index tree 3 order A khớp leafCol | PASS | predictLeaf=13.0, leafCol=13.0 |
| Leaf index tree 4 order A khớp leafCol | PASS | predictLeaf=12.0, leafCol=12.0 |
| Leaf index tree 5 order A khớp leafCol | PASS | predictLeaf=20.0, leafCol=20.0 |
| Leaf index tree 6 order A khớp leafCol | PASS | predictLeaf=24.0, leafCol=24.0 |
| Leaf index tree 7 order A khớp leafCol | PASS | predictLeaf=12.0, leafCol=12.0 |
| Leaf index tree 8 order A khớp leafCol | PASS | predictLeaf=26.0, leafCol=26.0 |
| Leaf index tree 9 order A khớp leafCol | PASS | predictLeaf=4.0, leafCol=4.0 |
| Leaf index tree 10 order A khớp leafCol | PASS | predictLeaf=15.0, leafCol=15.0 |
| Leaf index tree 11 order A khớp leafCol | PASS | predictLeaf=12.0, leafCol=12.0 |
| Leaf index tree 12 order A khớp leafCol | PASS | predictLeaf=7.0, leafCol=7.0 |
| Leaf index tree 13 order A khớp leafCol | PASS | predictLeaf=15.0, leafCol=15.0 |
| Leaf index tree 14 order A khớp leafCol | PASS | predictLeaf=20.0, leafCol=20.0 |
| Leaf index tree 15 order A khớp leafCol | PASS | predictLeaf=6.0, leafCol=6.0 |
| Leaf index tree 16 order A khớp leafCol | PASS | predictLeaf=23.0, leafCol=23.0 |
| Leaf index tree 17 order A khớp leafCol | PASS | predictLeaf=3.0, leafCol=3.0 |
| Leaf index tree 18 order A khớp leafCol | PASS | predictLeaf=20.0, leafCol=20.0 |
| Leaf index tree 19 order A khớp leafCol | PASS | predictLeaf=3.0, leafCol=3.0 |
| Leaf index tree 20 order A khớp leafCol | PASS | predictLeaf=13.0, leafCol=13.0 |
| Leaf index tree 21 order A khớp leafCol | PASS | predictLeaf=21.0, leafCol=21.0 |
| Leaf index tree 22 order A khớp leafCol | PASS | predictLeaf=18.0, leafCol=18.0 |
| Leaf index tree 23 order A khớp leafCol | PASS | predictLeaf=2.0, leafCol=2.0 |
| Leaf index tree 24 order A khớp leafCol | PASS | predictLeaf=6.0, leafCol=6.0 |
| Leaf index tree 25 order A khớp leafCol | PASS | predictLeaf=5.0, leafCol=5.0 |
| Leaf index tree 26 order A khớp leafCol | PASS | predictLeaf=14.0, leafCol=14.0 |
| Leaf index tree 27 order A khớp leafCol | PASS | predictLeaf=21.0, leafCol=21.0 |
| Leaf index tree 28 order A khớp leafCol | PASS | predictLeaf=8.0, leafCol=8.0 |
| Leaf index tree 29 order A khớp leafCol | PASS | predictLeaf=2.0, leafCol=2.0 |
| Tổng treeWeights contribution order A khớp rawPrediction | PASS | tổng tree=[25.994828272017095, 4.0051717279829049], model raw=[25.994828272017099, 4.0051717279829058] |
| Random Forest probability order B khớp Spark | PASS | manual=0.12883516751028987, Spark=0.12883516751028987, độ lệch=0 |
| Order B có leaf index của mọi decision tree | PASS | leaf indices=30, trees=30 |
| Leaf index tree 0 order B khớp leafCol | PASS | predictLeaf=9.0, leafCol=9.0 |
| Leaf index tree 1 order B khớp leafCol | PASS | predictLeaf=0.0, leafCol=0.0 |
| Leaf index tree 2 order B khớp leafCol | PASS | predictLeaf=11.0, leafCol=11.0 |
| Leaf index tree 3 order B khớp leafCol | PASS | predictLeaf=13.0, leafCol=13.0 |
| Leaf index tree 4 order B khớp leafCol | PASS | predictLeaf=16.0, leafCol=16.0 |
| Leaf index tree 5 order B khớp leafCol | PASS | predictLeaf=20.0, leafCol=20.0 |
| Leaf index tree 6 order B khớp leafCol | PASS | predictLeaf=16.0, leafCol=16.0 |
| Leaf index tree 7 order B khớp leafCol | PASS | predictLeaf=15.0, leafCol=15.0 |
| Leaf index tree 8 order B khớp leafCol | PASS | predictLeaf=26.0, leafCol=26.0 |
| Leaf index tree 9 order B khớp leafCol | PASS | predictLeaf=4.0, leafCol=4.0 |
| Leaf index tree 10 order B khớp leafCol | PASS | predictLeaf=11.0, leafCol=11.0 |
| Leaf index tree 11 order B khớp leafCol | PASS | predictLeaf=9.0, leafCol=9.0 |
| Leaf index tree 12 order B khớp leafCol | PASS | predictLeaf=7.0, leafCol=7.0 |
| Leaf index tree 13 order B khớp leafCol | PASS | predictLeaf=15.0, leafCol=15.0 |
| Leaf index tree 14 order B khớp leafCol | PASS | predictLeaf=20.0, leafCol=20.0 |
| Leaf index tree 15 order B khớp leafCol | PASS | predictLeaf=0.0, leafCol=0.0 |
| Leaf index tree 16 order B khớp leafCol | PASS | predictLeaf=15.0, leafCol=15.0 |
| Leaf index tree 17 order B khớp leafCol | PASS | predictLeaf=8.0, leafCol=8.0 |
| Leaf index tree 18 order B khớp leafCol | PASS | predictLeaf=16.0, leafCol=16.0 |
| Leaf index tree 19 order B khớp leafCol | PASS | predictLeaf=17.0, leafCol=17.0 |
| Leaf index tree 20 order B khớp leafCol | PASS | predictLeaf=9.0, leafCol=9.0 |
| Leaf index tree 21 order B khớp leafCol | PASS | predictLeaf=19.0, leafCol=19.0 |
| Leaf index tree 22 order B khớp leafCol | PASS | predictLeaf=11.0, leafCol=11.0 |
| Leaf index tree 23 order B khớp leafCol | PASS | predictLeaf=11.0, leafCol=11.0 |
| Leaf index tree 24 order B khớp leafCol | PASS | predictLeaf=14.0, leafCol=14.0 |
| Leaf index tree 25 order B khớp leafCol | PASS | predictLeaf=5.0, leafCol=5.0 |
| Leaf index tree 26 order B khớp leafCol | PASS | predictLeaf=4.0, leafCol=4.0 |
| Leaf index tree 27 order B khớp leafCol | PASS | predictLeaf=18.0, leafCol=18.0 |
| Leaf index tree 28 order B khớp leafCol | PASS | predictLeaf=5.0, leafCol=5.0 |
| Leaf index tree 29 order B khớp leafCol | PASS | predictLeaf=16.0, leafCol=16.0 |
| Tổng treeWeights contribution order B khớp rawPrediction | PASS | tổng tree=[26.134944974691305, 3.8650550253086968], model raw=[26.134944974691305, 3.8650550253086964] |
| Random Forest probability order C khớp Spark | PASS | manual=0.062254984512976501, Spark=0.062254984512976501, độ lệch=0 |
| Order C có leaf index của mọi decision tree | PASS | leaf indices=30, trees=30 |
| Leaf index tree 0 order C khớp leafCol | PASS | predictLeaf=9.0, leafCol=9.0 |
| Leaf index tree 1 order C khớp leafCol | PASS | predictLeaf=2.0, leafCol=2.0 |
| Leaf index tree 2 order C khớp leafCol | PASS | predictLeaf=11.0, leafCol=11.0 |
| Leaf index tree 3 order C khớp leafCol | PASS | predictLeaf=13.0, leafCol=13.0 |
| Leaf index tree 4 order C khớp leafCol | PASS | predictLeaf=20.0, leafCol=20.0 |
| Leaf index tree 5 order C khớp leafCol | PASS | predictLeaf=20.0, leafCol=20.0 |
| Leaf index tree 6 order C khớp leafCol | PASS | predictLeaf=12.0, leafCol=12.0 |
| Leaf index tree 7 order C khớp leafCol | PASS | predictLeaf=20.0, leafCol=20.0 |
| Leaf index tree 8 order C khớp leafCol | PASS | predictLeaf=26.0, leafCol=26.0 |
| Leaf index tree 9 order C khớp leafCol | PASS | predictLeaf=4.0, leafCol=4.0 |
| Leaf index tree 10 order C khớp leafCol | PASS | predictLeaf=15.0, leafCol=15.0 |
| Leaf index tree 11 order C khớp leafCol | PASS | predictLeaf=9.0, leafCol=9.0 |
| Leaf index tree 12 order C khớp leafCol | PASS | predictLeaf=7.0, leafCol=7.0 |
| Leaf index tree 13 order C khớp leafCol | PASS | predictLeaf=15.0, leafCol=15.0 |
| Leaf index tree 14 order C khớp leafCol | PASS | predictLeaf=20.0, leafCol=20.0 |
| Leaf index tree 15 order C khớp leafCol | PASS | predictLeaf=0.0, leafCol=0.0 |
| Leaf index tree 16 order C khớp leafCol | PASS | predictLeaf=23.0, leafCol=23.0 |
| Leaf index tree 17 order C khớp leafCol | PASS | predictLeaf=21.0, leafCol=21.0 |
| Leaf index tree 18 order C khớp leafCol | PASS | predictLeaf=23.0, leafCol=23.0 |
| Leaf index tree 19 order C khớp leafCol | PASS | predictLeaf=17.0, leafCol=17.0 |
| Leaf index tree 20 order C khớp leafCol | PASS | predictLeaf=13.0, leafCol=13.0 |
| Leaf index tree 21 order C khớp leafCol | PASS | predictLeaf=20.0, leafCol=20.0 |
| Leaf index tree 22 order C khớp leafCol | PASS | predictLeaf=18.0, leafCol=18.0 |
| Leaf index tree 23 order C khớp leafCol | PASS | predictLeaf=12.0, leafCol=12.0 |
| Leaf index tree 24 order C khớp leafCol | PASS | predictLeaf=14.0, leafCol=14.0 |
| Leaf index tree 25 order C khớp leafCol | PASS | predictLeaf=5.0, leafCol=5.0 |
| Leaf index tree 26 order C khớp leafCol | PASS | predictLeaf=14.0, leafCol=14.0 |
| Leaf index tree 27 order C khớp leafCol | PASS | predictLeaf=21.0, leafCol=21.0 |
| Leaf index tree 28 order C khớp leafCol | PASS | predictLeaf=12.0, leafCol=12.0 |
| Leaf index tree 29 order C khớp leafCol | PASS | predictLeaf=16.0, leafCol=16.0 |
| Tổng treeWeights contribution order C khớp rawPrediction | PASS | tổng tree=[28.132350464610706, 1.8676495353892952], model raw=[28.132350464610706, 1.867649535389295] |
| Random Forest probability order D khớp Spark | PASS | manual=0.054934261389883575, Spark=0.054934261389883575, độ lệch=0 |
| Order D có leaf index của mọi decision tree | PASS | leaf indices=30, trees=30 |
| Leaf index tree 0 order D khớp leafCol | PASS | predictLeaf=9.0, leafCol=9.0 |
| Leaf index tree 1 order D khớp leafCol | PASS | predictLeaf=5.0, leafCol=5.0 |
| Leaf index tree 2 order D khớp leafCol | PASS | predictLeaf=11.0, leafCol=11.0 |
| Leaf index tree 3 order D khớp leafCol | PASS | predictLeaf=13.0, leafCol=13.0 |
| Leaf index tree 4 order D khớp leafCol | PASS | predictLeaf=26.0, leafCol=26.0 |
| Leaf index tree 5 order D khớp leafCol | PASS | predictLeaf=22.0, leafCol=22.0 |
| Leaf index tree 6 order D khớp leafCol | PASS | predictLeaf=17.0, leafCol=17.0 |
| Leaf index tree 7 order D khớp leafCol | PASS | predictLeaf=15.0, leafCol=15.0 |
| Leaf index tree 8 order D khớp leafCol | PASS | predictLeaf=33.0, leafCol=33.0 |
| Leaf index tree 9 order D khớp leafCol | PASS | predictLeaf=4.0, leafCol=4.0 |
| Leaf index tree 10 order D khớp leafCol | PASS | predictLeaf=14.0, leafCol=14.0 |
| Leaf index tree 11 order D khớp leafCol | PASS | predictLeaf=9.0, leafCol=9.0 |
| Leaf index tree 12 order D khớp leafCol | PASS | predictLeaf=3.0, leafCol=3.0 |
| Leaf index tree 13 order D khớp leafCol | PASS | predictLeaf=15.0, leafCol=15.0 |
| Leaf index tree 14 order D khớp leafCol | PASS | predictLeaf=20.0, leafCol=20.0 |
| Leaf index tree 15 order D khớp leafCol | PASS | predictLeaf=17.0, leafCol=17.0 |
| Leaf index tree 16 order D khớp leafCol | PASS | predictLeaf=26.0, leafCol=26.0 |
| Leaf index tree 17 order D khớp leafCol | PASS | predictLeaf=9.0, leafCol=9.0 |
| Leaf index tree 18 order D khớp leafCol | PASS | predictLeaf=10.0, leafCol=10.0 |
| Leaf index tree 19 order D khớp leafCol | PASS | predictLeaf=17.0, leafCol=17.0 |
| Leaf index tree 20 order D khớp leafCol | PASS | predictLeaf=3.0, leafCol=3.0 |
| Leaf index tree 21 order D khớp leafCol | PASS | predictLeaf=20.0, leafCol=20.0 |
| Leaf index tree 22 order D khớp leafCol | PASS | predictLeaf=19.0, leafCol=19.0 |
| Leaf index tree 23 order D khớp leafCol | PASS | predictLeaf=12.0, leafCol=12.0 |
| Leaf index tree 24 order D khớp leafCol | PASS | predictLeaf=14.0, leafCol=14.0 |
| Leaf index tree 25 order D khớp leafCol | PASS | predictLeaf=10.0, leafCol=10.0 |
| Leaf index tree 26 order D khớp leafCol | PASS | predictLeaf=14.0, leafCol=14.0 |
| Leaf index tree 27 order D khớp leafCol | PASS | predictLeaf=17.0, leafCol=17.0 |
| Leaf index tree 28 order D khớp leafCol | PASS | predictLeaf=19.0, leafCol=19.0 |
| Leaf index tree 29 order D khớp leafCol | PASS | predictLeaf=12.0, leafCol=12.0 |
| Tổng treeWeights contribution order D khớp rawPrediction | PASS | tổng tree=[28.351972158303493, 1.6480278416965075], model raw=[28.351972158303493, 1.6480278416965073] |
| Lần chạy lặp có cùng threshold, split, confusion matrix, metrics và A/B/C/D | PASS | previous=ec543f47703be58e1068a1bebb9c44c632f3e9766da82d87ddb134b460c25dbd, current=ec543f47703be58e1068a1bebb9c44c632f3e9766da82d87ddb134b460c25dbd |
| Dataset không bị sửa trong lần chạy | PASS | hash trước=7301d0c09c808be88ef35203991797fcdf23c3b414f33de978c995b9371601e1, hash sau=7301d0c09c808be88ef35203991797fcdf23c3b414f33de978c995b9371601e1 |
| Output tồn tại và không rỗng: 05_thong_tin_chia_du_lieu.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_thong_tin_chia_du_lieu.csv |
| Output tồn tại và không rỗng: 05_preprocessing_details.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_preprocessing_details.csv |
| Output tồn tại và không rỗng: 05_string_indexer_mapping.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_string_indexer_mapping.csv |
| Output tồn tại và không rỗng: 05_feature_vector_metadata.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_feature_vector_metadata.csv |
| Output tồn tại và không rỗng: 05_validation_common_thresholds.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_validation_common_thresholds.csv |
| Output tồn tại và không rỗng: 05_xep_hang_common_threshold_F1.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_xep_hang_common_threshold_F1.csv |
| Output tồn tại và không rỗng: 05_so_sanh_threshold_thu_cong.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_so_sanh_threshold_thu_cong.csv |
| Output tồn tại và không rỗng: 05_common_threshold_duoc_chon.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_common_threshold_duoc_chon.csv |
| Output tồn tại và không rỗng: 05_ket_qua_validation_common_threshold.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_ket_qua_validation_common_threshold.csv |
| Output tồn tại và không rỗng: 05_confusion_matrix_validation.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_confusion_matrix_validation.csv |
| Output tồn tại và không rỗng: 05_ket_qua_test_common_threshold.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_ket_qua_test_common_threshold.csv |
| Output tồn tại và không rỗng: 05_confusion_matrix_test.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_confusion_matrix_test.csv |
| Output tồn tại và không rỗng: 05_ket_qua_baseline.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_ket_qua_baseline.csv |
| Output tồn tại và không rỗng: 05_logistic_coefficients.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_logistic_coefficients.csv |
| Output tồn tại và không rỗng: 05_logistic_score_breakdown_orders.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_logistic_score_breakdown_orders.csv |
| Output tồn tại và không rỗng: 05_random_forest_feature_importances.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_random_forest_feature_importances.csv |
| Output tồn tại và không rỗng: 05_random_forest_score_breakdown_orders.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_random_forest_score_breakdown_orders.csv |
| Output tồn tại và không rỗng: 05_random_forest_tree_details.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_random_forest_tree_details.csv |
| Output tồn tại và không rỗng: 05_roc_points_logistic_regression.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_roc_points_logistic_regression.csv |
| Output tồn tại và không rỗng: 05_roc_points_random_forest.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_roc_points_random_forest.csv |
| Output tồn tại và không rỗng: 05_roc_points_baseline.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_roc_points_baseline.csv |
| Output tồn tại và không rỗng: 05_auc_trapezoids_logistic_regression.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_auc_trapezoids_logistic_regression.csv |
| Output tồn tại và không rỗng: 05_auc_trapezoids_random_forest.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_auc_trapezoids_random_forest.csv |
| Output tồn tại và không rỗng: 05_auc_trapezoids_baseline.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_auc_trapezoids_baseline.csv |
| Output tồn tại và không rỗng: 05_demo_orders_A_B_C_D.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_demo_orders_A_B_C_D.csv |
| Output tồn tại và không rỗng: 05_danh_sach_feature.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_danh_sach_feature.csv |
| Output tồn tại và không rỗng: 05_so_sanh_mo_hinh.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_so_sanh_mo_hinh.csv |
| Output tồn tại và không rỗng: 05_ma_tran_nham_lan.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_ma_tran_nham_lan.csv |
| Output tồn tại và không rỗng: 05_chan_doan_xac_suat.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_chan_doan_xac_suat.csv |
| Output tồn tại và không rỗng: 05_reproducibility_signature.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_reproducibility_signature.csv |
| Output tồn tại và không rỗng: 05_reproducibility_check.csv | PASS | C:\data_python\olist_tieu_luan\outputs\tables\05_reproducibility_check.csv |
| Output tồn tại và không rỗng: 05_common_threshold_validation.png | PASS | C:\data_python\olist_tieu_luan\outputs\charts\05_common_threshold_validation.png |
| Output tồn tại và không rỗng: 05_confusion_matrix_logistic_regression.png | PASS | C:\data_python\olist_tieu_luan\outputs\charts\05_confusion_matrix_logistic_regression.png |
| Output tồn tại và không rỗng: 05_confusion_matrix_random_forest.png | PASS | C:\data_python\olist_tieu_luan\outputs\charts\05_confusion_matrix_random_forest.png |
| Output tồn tại và không rỗng: 05_confusion_matrix_baseline.png | PASS | C:\data_python\olist_tieu_luan\outputs\charts\05_confusion_matrix_baseline.png |
| Output tồn tại và không rỗng: 05_roc_curve_test.png | PASS | C:\data_python\olist_tieu_luan\outputs\charts\05_roc_curve_test.png |
| Output tồn tại và không rỗng: 05_probability_distribution_test.png | PASS | C:\data_python\olist_tieu_luan\outputs\charts\05_probability_distribution_test.png |
| Output tồn tại và không rỗng: 05_so_sanh_mo_hinh.png | PASS | C:\data_python\olist_tieu_luan\outputs\charts\05_so_sanh_mo_hinh.png |

## F. Kiểm tra chạy lặp

Trạng thái: **MATCH**. Previous signature: `ec543f47703be58e1068a1bebb9c44c632f3e9766da82d87ddb134b460c25dbd`. Current signature: `ec543f47703be58e1068a1bebb9c44c632f3e9766da82d87ddb134b460c25dbd`. Signature bao gồm common threshold, data split, validation/test confusion matrix và metrics, cùng order A/B/C/D.

## G. Danh sách output

- `outputs\tables\05_thong_tin_chia_du_lieu.csv`
- `outputs\tables\05_preprocessing_details.csv`
- `outputs\tables\05_string_indexer_mapping.csv`
- `outputs\tables\05_feature_vector_metadata.csv`
- `outputs\tables\05_validation_common_thresholds.csv`
- `outputs\tables\05_xep_hang_common_threshold_F1.csv`
- `outputs\tables\05_so_sanh_threshold_thu_cong.csv`
- `outputs\tables\05_common_threshold_duoc_chon.csv`
- `outputs\tables\05_ket_qua_validation_common_threshold.csv`
- `outputs\tables\05_confusion_matrix_validation.csv`
- `outputs\tables\05_ket_qua_test_common_threshold.csv`
- `outputs\tables\05_confusion_matrix_test.csv`
- `outputs\tables\05_ket_qua_baseline.csv`
- `outputs\tables\05_logistic_coefficients.csv`
- `outputs\tables\05_logistic_score_breakdown_orders.csv`
- `outputs\tables\05_random_forest_feature_importances.csv`
- `outputs\tables\05_random_forest_score_breakdown_orders.csv`
- `outputs\tables\05_random_forest_tree_details.csv`
- `outputs\tables\05_roc_points_logistic_regression.csv`
- `outputs\tables\05_roc_points_random_forest.csv`
- `outputs\tables\05_roc_points_baseline.csv`
- `outputs\tables\05_auc_trapezoids_logistic_regression.csv`
- `outputs\tables\05_auc_trapezoids_random_forest.csv`
- `outputs\tables\05_auc_trapezoids_baseline.csv`
- `outputs\tables\05_demo_orders_A_B_C_D.csv`
- `outputs\tables\05_danh_sach_feature.csv`
- `outputs\tables\05_so_sanh_mo_hinh.csv`
- `outputs\tables\05_ma_tran_nham_lan.csv`
- `outputs\tables\05_chan_doan_xac_suat.csv`
- `outputs\tables\05_code_line_reference.csv`
- `outputs\tables\05_assertion_checks.csv`
- `outputs\tables\05_run_metadata.csv`
- `outputs\tables\05_reproducibility_signature.csv`
- `outputs\tables\05_reproducibility_check.csv`
- `outputs\charts\05_common_threshold_validation.png`
- `outputs\charts\05_confusion_matrix_logistic_regression.png`
- `outputs\charts\05_confusion_matrix_random_forest.png`
- `outputs\charts\05_confusion_matrix_baseline.png`
- `outputs\charts\05_roc_curve_test.png`
- `outputs\charts\05_probability_distribution_test.png`
- `outputs\charts\05_so_sanh_mo_hinh.png`
- `bao_cao_kiem_chung_05_F1.md`
