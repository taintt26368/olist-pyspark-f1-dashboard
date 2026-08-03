# -*- coding: utf-8 -*-

"""Huấn luyện, chọn common threshold và kiểm chứng hai model Olist.

File này là quy trình độc lập của bước 05. Mọi số liệu trong các bảng, biểu đồ
và báo cáo Markdown đều được tính lại từ dataset và model ở lần chạy hiện tại.
"""

from pathlib import Path
from datetime import datetime
import csv
import hashlib
import json
import math
import os
import platform
import sys
import time

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ``matplotlib`` là dependency bắt buộc cho các biểu đồ bằng chứng. Khi chạy
# trong Spyder, thông báo này chỉ ra đúng interpreter cần cài package.
try:
    import matplotlib
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "Thiếu package matplotlib trong Python đang chạy: "
        f"{sys.executable}. Hãy cài bằng lệnh: "
        f'"{sys.executable}" -m pip install matplotlib'
    ) from exc

# Backend Agg ghi PNG mà không yêu cầu cửa sổ đồ họa.
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.functions import vector_to_array


# ================================================================
# CẤU HÌNH THỬ NGHIỆM
# ================================================================
# RUN_TAG = "official": ghi vào thư mục outputs như phiên bản chính thức.
# RUN_TAG khác "official": ghi riêng vào experiments/<RUN_TAG> để không
# ghi đè kết quả chính thức và không gây MISMATCH với chữ ký cũ.
RUN_TAG = "official"

# None: chương trình tự chọn common threshold trên validation.
# Gán số từ 0 đến 1, ví dụ 0.09 hoặc 0.10, để ép toàn bộ
# validation/test, confusion matrix, biểu đồ và báo cáo dùng threshold đó.
MANUAL_COMMON_THRESHOLD = None

# Danh sách threshold được xuất ra CSV để so sánh thủ công chỉ trên validation.
# Common threshold được chọn sẽ tự động được thêm vào danh
# sách này; không đưa threshold mặc định cũ vào bảng nghiên cứu.
THRESHOLD_TEST_THU_CONG = [0.09, 0.10]

# Cấu hình được xác nhận từ project hiện tại.
PROJECT_DIR = Path(__file__).resolve().parent
DATA_FILE = PROJECT_DIR / "data" / "output" / "orders_enriched.csv"

if RUN_TAG.strip().lower() == "official":
    OUTPUT_BASE_DIR = PROJECT_DIR / "outputs"
    TABLE_DIR = OUTPUT_BASE_DIR / "tables"
    CHART_DIR = OUTPUT_BASE_DIR / "charts"
    WAREHOUSE_DIR = PROJECT_DIR / "spark_warehouse"
    REPORT_FILE = (
        PROJECT_DIR
        / "bao_cao_kiem_chung_05_F1.md"
    )
    PROMPT_FILE = (
        PROJECT_DIR
        / "140. prompt_cap_nhat_slide_va_tieu_luan_olist.md"
    )
else:
    OUTPUT_BASE_DIR = PROJECT_DIR / "experiments" / RUN_TAG
    TABLE_DIR = OUTPUT_BASE_DIR / "tables"
    CHART_DIR = OUTPUT_BASE_DIR / "charts"
    WAREHOUSE_DIR = OUTPUT_BASE_DIR / "spark_warehouse"
    REPORT_FILE = OUTPUT_BASE_DIR / "bao_cao_kiem_chung_buoc_05.md"
    PROMPT_FILE = OUTPUT_BASE_DIR / "prompt_cap_nhat_slide_va_tieu_luan.md"

for folder in (TABLE_DIR, CHART_DIR, WAREHOUSE_DIR):
    folder.mkdir(parents=True, exist_ok=True)

SEED = 42
MAX_ALERT_RATE = 0.20
LR_MAX_ITER = 50
LR_REG_PARAM = 0.01
RF_NUM_TREES = 30
RF_MAX_DEPTH = 6

COT_PHAN_LOAI = [
    "customer_state",
    "main_seller_state",
    "main_category",
]

COT_SO = [
    "item_count",
    "product_count",
    "seller_count",
    "total_price",
    "total_freight",
    "average_item_price",
    "total_weight_g",
    "total_volume_cm3",
    "freight_ratio",
    "purchase_year",
    "purchase_month",
    "purchase_day_of_week",
    "purchase_hour",
    "estimated_delivery_days",
    "customer_seller_same_state",
]

# Các cột không được đưa vào feature tại thời điểm đặt hàng.
COT_LEAKAGE_CAM = {
    "order_delivered_customer_date",
    "delivery_difference_days",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "main_payment_type",
    "payment_record_count",
    "payment_type_count",
    "max_installments",
    "payment_value_total",
}

BASELINE_NAME = "Baseline majority class"
TEN_MODEL = ["Logistic Regression", "Random Forest"]
TEN_PHUONG_PHAP = [BASELINE_NAME, *TEN_MODEL]
SIGNATURE_VERSION = "05_f1_official_v1"
CAC_ASSERTION = []

TABLE_FILES = [
    TABLE_DIR / "05_thong_tin_chia_du_lieu.csv",
    TABLE_DIR / "05_preprocessing_details.csv",
    TABLE_DIR / "05_string_indexer_mapping.csv",
    TABLE_DIR / "05_feature_vector_metadata.csv",
    TABLE_DIR / "05_validation_common_thresholds.csv",
    TABLE_DIR / "05_xep_hang_common_threshold_F1.csv",
    TABLE_DIR / "05_so_sanh_threshold_thu_cong.csv",
    TABLE_DIR / "05_common_threshold_duoc_chon.csv",
    TABLE_DIR / "05_ket_qua_validation_common_threshold.csv",
    TABLE_DIR / "05_confusion_matrix_validation.csv",
    TABLE_DIR / "05_ket_qua_test_common_threshold.csv",
    TABLE_DIR / "05_confusion_matrix_test.csv",
    TABLE_DIR / "05_ket_qua_baseline.csv",
    TABLE_DIR / "05_logistic_coefficients.csv",
    TABLE_DIR / "05_logistic_score_breakdown_orders.csv",
    TABLE_DIR / "05_random_forest_feature_importances.csv",
    TABLE_DIR / "05_random_forest_score_breakdown_orders.csv",
    TABLE_DIR / "05_random_forest_tree_details.csv",
    TABLE_DIR / "05_roc_points_logistic_regression.csv",
    TABLE_DIR / "05_roc_points_random_forest.csv",
    TABLE_DIR / "05_roc_points_baseline.csv",
    TABLE_DIR / "05_auc_trapezoids_logistic_regression.csv",
    TABLE_DIR / "05_auc_trapezoids_random_forest.csv",
    TABLE_DIR / "05_auc_trapezoids_baseline.csv",
    TABLE_DIR / "05_demo_orders_A_B_C_D.csv",
    TABLE_DIR / "05_danh_sach_feature.csv",
    TABLE_DIR / "05_so_sanh_mo_hinh.csv",
    TABLE_DIR / "05_ma_tran_nham_lan.csv",
    TABLE_DIR / "05_chan_doan_xac_suat.csv",
    TABLE_DIR / "05_code_line_reference.csv",
    TABLE_DIR / "05_assertion_checks.csv",
    TABLE_DIR / "05_run_metadata.csv",
    TABLE_DIR / "05_reproducibility_signature.csv",
    TABLE_DIR / "05_reproducibility_check.csv",
]

CHART_FILES = [
    CHART_DIR / "05_common_threshold_validation.png",
    CHART_DIR / "05_confusion_matrix_logistic_regression.png",
    CHART_DIR / "05_confusion_matrix_random_forest.png",
    CHART_DIR / "05_confusion_matrix_baseline.png",
    CHART_DIR / "05_roc_curve_test.png",
    CHART_DIR / "05_probability_distribution_test.png",
    CHART_DIR / "05_so_sanh_mo_hinh.png",
]


def ghi_csv(duong_dan, du_lieu, danh_sach_cot):
    """Ghi một CSV UTF-8 có header, kể cả khi danh sách dữ liệu rỗng."""
    with duong_dan.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=danh_sach_cot,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(du_lieu)


def doc_mot_dong_csv(duong_dan):
    """Đọc dòng dữ liệu đầu tiên của CSV, hoặc trả về None nếu file trống."""
    if not duong_dan.is_file() or duong_dan.stat().st_size == 0:
        return None
    with duong_dan.open("r", encoding="utf-8-sig", newline="") as f:
        return next(csv.DictReader(f), None)


def bam_sha256(duong_dan):
    """Tính SHA-256 theo từng khối để chứng minh dataset gốc không bị sửa."""
    bo_bam = hashlib.sha256()
    with duong_dan.open("rb") as f:
        while True:
            khoi = f.read(1024 * 1024)
            if not khoi:
                break
            bo_bam.update(khoi)
    return bo_bam.hexdigest()


def chia_an_toan(tu_so, mau_so):
    """Thực hiện phép chia và trả 0 khi mẫu số bằng 0."""
    return float(tu_so / mau_so) if mau_so else 0.0


def la_huu_han(gia_tri):
    """Kiểm tra một giá trị có phải số hữu hạn, không phải NaN/Infinity."""
    return math.isfinite(float(gia_tri))


def xac_nhan(ten_kiem_tra, dieu_kien, chi_tiet):
    """Ghi PASS/FAIL cho một assertion và dừng ngay nếu điều kiện sai.

    Mọi kiểm tra được lưu trong ``CAC_ASSERTION`` để đưa vào CSV và phụ lục báo
    cáo, nên chương trình không thể báo thành công khi một kiểm tra đã thất bại.
    """
    trang_thai = "PASS" if bool(dieu_kien) else "FAIL"
    CAC_ASSERTION.append({
        "kiem_tra": ten_kiem_tra,
        "trang_thai": trang_thai,
        "chi_tiet": str(chi_tiet),
    })
    if not dieu_kien:
        raise RuntimeError(f"{ten_kiem_tra}: {chi_tiet}")


def kiem_tra_feature(danh_sach_feature, cot_duoc_phep):
    """Chặn feature leakage và feature nằm ngoài danh sách đã xác nhận.

    Hàm so sánh tập feature đầu vào với ``COT_LEAKAGE_CAM`` và danh sách cột
    cho phép; kết quả sai sẽ dừng trước khi preprocessing/model được fit.
    """
    # CODE_REF: FEATURE_LEAKAGE
    feature_leakage = sorted(
        set(danh_sach_feature).intersection(COT_LEAKAGE_CAM)
    )
    feature_ngoai_danh_sach = sorted(
        set(danh_sach_feature) - set(cot_duoc_phep)
    )
    xac_nhan(
        "Không có feature leakage",
        not feature_leakage,
        f"feature leakage={feature_leakage}",
    )
    xac_nhan(
        "Feature nằm trong danh sách được xác nhận",
        not feature_ngoai_danh_sach,
        f"feature ngoài danh sách={feature_ngoai_danh_sach}",
    )


def kiem_tra_nan_vo_cuc(df, danh_sach_cot):
    """Đếm và chặn NaN/Infinity trong toàn bộ numeric feature đầu vào."""
    # CODE_REF: NAN_INFINITY
    ket_qua = df.agg(*[
        F.sum(
            F.when(
                F.isnan(F.col(ten_cot))
                | (F.col(ten_cot) == float("inf"))
                | (F.col(ten_cot) == float("-inf")),
                1,
            ).otherwise(0)
        ).alias(ten_cot)
        for ten_cot in danh_sach_cot
    ]).first().asDict()
    loi = {
        ten_cot: int(so_dong or 0)
        for ten_cot, so_dong in ket_qua.items()
        if int(so_dong or 0) > 0
    }
    xac_nhan(
        "Numeric feature không có NaN hoặc Infinity",
        not loi,
        f"số bất thường theo feature={loi}",
    )
    return ket_qua


def thong_ke_tap(ten_tap, df):
    """Tính row count, order distinct, số late/not late và prevalence một tập."""
    dong = df.agg(
        F.count("*").alias("so_dong"),
        F.countDistinct("order_id").alias("so_order_id_khac_nhau"),
        F.sum(F.when(F.col("is_late") == 1, 1).otherwise(0)).alias(
            "so_don_late"
        ),
        F.sum(F.when(F.col("is_late") == 0, 1).otherwise(0)).alias(
            "so_don_not_late"
        ),
    ).first().asDict()
    so_dong = int(dong["so_dong"] or 0)
    late = int(dong["so_don_late"] or 0)
    not_late = int(dong["so_don_not_late"] or 0)
    return {
        "tap_du_lieu": ten_tap,
        "so_dong": so_dong,
        "so_order_id_khac_nhau": int(
            dong["so_order_id_khac_nhau"] or 0
        ),
        "so_don_late": late,
        "so_don_not_late": not_late,
        "late_rate": chia_an_toan(late, so_dong),
    }


def tao_pipeline_tien_xu_ly():
    """Tạo Pipeline preprocessing dùng chung cho hai model.

    Pipeline gồm StringIndexer, OneHotEncoder, Imputer(strategy='median') và
    VectorAssembler. Hàm chỉ định nghĩa các stage; nơi gọi quyết định tập được
    dùng để fit nhằm ngăn validation/test leakage.
    """
    cot_chi_so = [f"{cot}_index" for cot in COT_PHAN_LOAI]
    cot_ma_hoa = [f"{cot}_ohe" for cot in COT_PHAN_LOAI]
    cot_so_da_dien = [f"{cot}_filled" for cot in COT_SO]

    # CODE_REF: STRING_INDEXER
    indexers = [
        StringIndexer(
            inputCol=cot,
            outputCol=f"{cot}_index",
            handleInvalid="keep",
        )
        for cot in COT_PHAN_LOAI
    ]
    # CODE_REF: ONE_HOT_ENCODER
    encoder = OneHotEncoder(
        inputCols=cot_chi_so,
        outputCols=cot_ma_hoa,
        handleInvalid="keep",
    )
    # CODE_REF: IMPUTER
    imputer = Imputer(
        inputCols=COT_SO,
        outputCols=cot_so_da_dien,
        strategy="median",
    )
    # CODE_REF: VECTOR_ASSEMBLER
    assembler = VectorAssembler(
        inputCols=cot_so_da_dien + cot_ma_hoa,
        outputCol="features",
        handleInvalid="keep",
    )
    return Pipeline(stages=indexers + [encoder, imputer, assembler])


def ten_feature_tu_metadata(df_da_bien_doi):
    """Lấy đúng tên transformed feature từ metadata của Spark."""
    metadata = df_da_bien_doi.schema["features"].metadata
    ml_attr = metadata.get("ml_attr", {})
    so_feature = int(ml_attr.get("num_attrs", 0))
    attrs = ml_attr.get("attrs", {})
    theo_index = {}
    loai_theo_index = {}
    for loai_attr, danh_sach in attrs.items():
        for attr in danh_sach:
            chi_so = int(attr["idx"])
            theo_index[chi_so] = attr.get("name")
            loai_theo_index[chi_so] = loai_attr
    xac_nhan(
        "Metadata có tên cho toàn bộ transformed feature",
        so_feature > 0
        and len(theo_index) == so_feature
        and sorted(theo_index) == list(range(so_feature))
        and all(theo_index.values()),
        f"num_attrs={so_feature}, số tên={len(theo_index)}",
    )
    return [theo_index[i] for i in range(so_feature)], [
        loai_theo_index[i] for i in range(so_feature)
    ]


def feature_goc_cua_transformed(ten):
    """Ánh xạ tên transformed feature về numeric/categorical feature gốc."""
    for cot in COT_SO:
        if ten == f"{cot}_filled" or ten.startswith(f"{cot}_filled_"):
            return cot
    for cot in COT_PHAN_LOAI:
        if ten.startswith(f"{cot}_ohe"):
            return cot
    return "không xác định"


def chi_tiet_preprocessing(
    pham_vi,
    pipeline_model,
    df_nguon,
    df_da_bien_doi,
    so_dong_truoc,
):
    """Trích xuất bằng chứng đầy đủ từ fitted preprocessing Pipeline.

    Kết quả gồm median của Imputer, mapping của StringIndexer, kích thước OHE,
    metadata của features vector và kiểm tra row count trước/sau transform.
    ``pham_vi`` cho biết Pipeline được fit trên train_fit hay train_full.
    """
    so_dong_sau = df_da_bien_doi.count()
    xac_nhan(
        f"Preprocessing {pham_vi} giữ nguyên số dòng",
        so_dong_truoc == so_dong_sau,
        f"trước={so_dong_truoc}, sau={so_dong_sau}",
    )

    so_cat = df_nguon.agg(*[
        F.countDistinct(cot).alias(cot) for cot in COT_PHAN_LOAI
    ]).first().asDict()
    indexer_models = pipeline_model.stages[:len(COT_PHAN_LOAI)]
    encoder_model = pipeline_model.stages[len(COT_PHAN_LOAI)]
    imputer_model = pipeline_model.stages[len(COT_PHAN_LOAI) + 1]
    surrogate = imputer_model.surrogateDF.first().asDict()

    ten_vector, loai_vector = ten_feature_tu_metadata(df_da_bien_doi)
    preprocessing_rows = [
        {
            "pham_vi_fit": pham_vi,
            "component": "Pipeline",
            "feature_goc": "tất cả",
            "chi_tiet": "số dòng trước preprocessing",
            "gia_tri": so_dong_truoc,
        },
        {
            "pham_vi_fit": pham_vi,
            "component": "Pipeline",
            "feature_goc": "tất cả",
            "chi_tiet": "số dòng sau preprocessing",
            "gia_tri": so_dong_sau,
        },
        {
            "pham_vi_fit": pham_vi,
            "component": "VectorAssembler",
            "feature_goc": "features",
            "chi_tiet": "tổng chiều dài features vector",
            "gia_tri": len(ten_vector),
        },
    ]

    for cot in COT_SO:
        preprocessing_rows.append({
            "pham_vi_fit": pham_vi,
            "component": "Imputer",
            "feature_goc": cot,
            "chi_tiet": "median",
            "gia_tri": surrogate[cot],
        })

    mapping_rows = []
    for vi_tri, (cot, model) in enumerate(
        zip(COT_PHAN_LOAI, indexer_models)
    ):
        labels = list(model.labels)
        for index, category in enumerate(labels):
            mapping_rows.append({
                "pham_vi_fit": pham_vi,
                "feature_goc": cot,
                "string_index": index,
                "category": category,
                "la_nhom_invalid": 0,
            })
        mapping_rows.append({
            "pham_vi_fit": pham_vi,
            "feature_goc": cot,
            "string_index": len(labels),
            "category": "__unknown_or_unseen__",
            "la_nhom_invalid": 1,
        })

        output_col = f"{cot}_ohe"
        ohe_metadata = df_da_bien_doi.schema[output_col].metadata
        ohe_size = int(
            ohe_metadata.get("ml_attr", {}).get("num_attrs", 0)
        )
        preprocessing_rows.extend([
            {
                "pham_vi_fit": pham_vi,
                "component": "StringIndexer",
                "feature_goc": cot,
                "chi_tiet": "số category thực tế",
                "gia_tri": int(so_cat[cot]),
            },
            {
                "pham_vi_fit": pham_vi,
                "component": "StringIndexer",
                "feature_goc": cot,
                "chi_tiet": "số label trong mapping",
                "gia_tri": len(labels),
            },
            {
                "pham_vi_fit": pham_vi,
                "component": "OneHotEncoder",
                "feature_goc": cot,
                "chi_tiet": "categorySizes",
                "gia_tri": int(encoder_model.categorySizes[vi_tri]),
            },
            {
                "pham_vi_fit": pham_vi,
                "component": "OneHotEncoder",
                "feature_goc": cot,
                "chi_tiet": "kích thước output vector",
                "gia_tri": ohe_size,
            },
        ])

    metadata_rows = [
        {
            "pham_vi_fit": pham_vi,
            "feature_index": index,
            "transformed_feature_name": ten,
            "feature_goc": feature_goc_cua_transformed(ten),
            "metadata_attribute_type": loai_vector[index],
        }
        for index, ten in enumerate(ten_vector)
    ]
    return preprocessing_rows, mapping_rows, metadata_rows, ten_vector


def fit_hai_model(df_ready):
    """Fit Logistic Regression và Random Forest trên cùng DataFrame đã xử lý.

    Hyperparameter lấy từ hằng số cấu hình; hàm không thay đổi feature, không
    thêm class weight và không thực hiện Cross Validation/Grid Search.
    """
    logistic = LogisticRegression(
        featuresCol="features",
        labelCol="is_late",
        predictionCol="prediction_spark_unused",
        maxIter=LR_MAX_ITER,
        regParam=LR_REG_PARAM,
    )
    random_forest = RandomForestClassifier(
        featuresCol="features",
        labelCol="is_late",
        predictionCol="prediction_spark_unused",
        numTrees=RF_NUM_TREES,
        maxDepth=RF_MAX_DEPTH,
        seed=SEED,
        leafCol="leaf_indices",
    )
    # CODE_REF: FIT_LOGISTIC
    model_logistic = logistic.fit(df_ready)
    # CODE_REF: FIT_RANDOM_FOREST
    model_random_forest = random_forest.fit(df_ready)
    return model_logistic, model_random_forest


def tao_probability(model, df_ready):
    """Transform dữ liệu và lấy probability lớp late từ phần tử vector thứ hai."""
    # CODE_REF: PROBABILITY_LATE
    return (
        model.transform(df_ready)
        .withColumn(
            "probability_late",
            vector_to_array("probability")[1],
        )
    )


def tinh_metrics_tu_counts(tp, tn, fp, fn, auc):
    """Tính toàn bộ metrics từ TP, TN, FP, FN và AUC.

    Hàm tính F1 theo cả công thức Precision–Recall lẫn công thức trực tiếp từ
    confusion matrix, sau đó assertion hai cách tính phải khớp nhau.
    """
    n = tp + tn + fp + fn
    # CODE_REF: METRIC_ACCURACY
    accuracy = chia_an_toan(tp + tn, n)
    # CODE_REF: METRIC_PRECISION
    precision = chia_an_toan(tp, tp + fp)
    # CODE_REF: METRIC_RECALL
    recall = chia_an_toan(tp, tp + fn)
    # CODE_REF: METRIC_SPECIFICITY
    specificity = chia_an_toan(tn, tn + fp)
    # CODE_REF: METRIC_FPR
    fpr = chia_an_toan(fp, fp + tn)
    # CODE_REF: METRIC_F1
    f1 = chia_an_toan(2 * precision * recall, precision + recall)
    f1_truc_tiep = chia_an_toan(2 * tp, 2 * tp + fp + fn)
    # CODE_REF: METRIC_ALERT_RATE
    alert_rate = chia_an_toan(tp + fp, n)
    prevalence = chia_an_toan(tp + fn, n)
    xac_nhan(
        "F1 theo hai công thức khớp nhau",
        abs(f1 - f1_truc_tiep) <= 1e-12,
        f"F1={f1:.17g}, F1 trực tiếp={f1_truc_tiep:.17g}",
    )
    cac_chi_so = [
        accuracy,
        precision,
        recall,
        specificity,
        fpr,
        f1,
        auc,
        alert_rate,
        prevalence,
    ]
    xac_nhan(
        "Toàn bộ metrics hữu hạn",
        all(la_huu_han(gia_tri) for gia_tri in cac_chi_so),
        f"metrics={cac_chi_so}",
    )
    return {
        "tp": int(tp),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
        "n": int(n),
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "specificity": specificity,
        "fpr": fpr,
        "f1": f1,
        "f1_direct": f1_truc_tiep,
        "auc": float(auc),
        "alert_rate": alert_rate,
        "prevalence": prevalence,
    }


def auc_spark(df_probability):
    """Tính AUC ROC bằng BinaryClassificationEvaluator của Spark."""
    evaluator = BinaryClassificationEvaluator(
        labelCol="is_late",
        rawPredictionCol="probability_late",
        metricName="areaUnderROC",
        numBins=0,
    )
    return float(evaluator.evaluate(df_probability))


def xac_dinh_label_da_so(df_train, ten_tap_train):
    """Xác định majority class chỉ từ train, tuyệt đối không nhìn validation/test.

    Nếu hai class có cùng số dòng, label nhỏ hơn được dùng làm tie-break để kết
    quả tái lập. Hàm trả cả label và số lượng từng class làm bằng chứng.
    """
    # CODE_REF: BASELINE_MAJORITY
    counts = (
        df_train
        .groupBy("is_late")
        .count()
        .orderBy(F.col("count").desc(), F.col("is_late").asc())
        .collect()
    )
    xac_nhan(
        f"{ten_tap_train} có đủ hai class để xác định baseline",
        len(counts) == 2,
        f"counts={[row.asDict() for row in counts]}",
    )
    majority_label = int(counts[0]["is_late"])
    class_counts = {
        int(row["is_late"]): int(row["count"])
        for row in counts
    }
    return majority_label, class_counts


def tao_du_doan_baseline(df, majority_label):
    """Tạo baseline luôn dự đoán majority class, không fit model hay threshold.

    ``probability_late`` là score hằng 0 hoặc 1 nên AUC kỳ vọng bằng 0.5 khi tập
    đánh giá có đủ hai class. ``prediction_common`` chỉ là cột tương thích với
    hàm tính confusion matrix; baseline không sử dụng common threshold.
    """
    probability_late = 1.0 if int(majority_label) == 1 else 0.0
    return (
        df.select("order_id", "is_late")
        .withColumn("probability_late", F.lit(probability_late))
        .withColumn("prediction_common", F.lit(float(majority_label)))
    )


def them_prediction_common(df_probability, common_threshold):
    """Tạo prediction_common bằng cách so probability_late với threshold khóa."""
    # CODE_REF: PREDICTION_COMMON
    return df_probability.withColumn(
        "prediction_common",
        F.when(
            F.col("probability_late") >= F.lit(common_threshold),
            F.lit(1.0),
        ).otherwise(F.lit(0.0)),
    )


def danh_gia_dataframe(ten_model, df_prediction, so_dong, auc):
    """Tính confusion matrix bằng Spark rồi kiểm tra mọi tổng hàng/cột.

    Kết quả được chuyển sang ``tinh_metrics_tu_counts`` để nhận Accuracy,
    Precision, Recall, Specificity, FPR, F1, alert rate và prevalence.
    """
    # CODE_REF: CONFUSION_MATRIX
    dong = df_prediction.agg(
        F.sum(
            F.when(
                (F.col("is_late") == 1)
                & (F.col("prediction_common") == 1),
                1,
            ).otherwise(0)
        ).alias("tp"),
        F.sum(
            F.when(
                (F.col("is_late") == 0)
                & (F.col("prediction_common") == 0),
                1,
            ).otherwise(0)
        ).alias("tn"),
        F.sum(
            F.when(
                (F.col("is_late") == 0)
                & (F.col("prediction_common") == 1),
                1,
            ).otherwise(0)
        ).alias("fp"),
        F.sum(
            F.when(
                (F.col("is_late") == 1)
                & (F.col("prediction_common") == 0),
                1,
            ).otherwise(0)
        ).alias("fn"),
    ).first().asDict()
    ket_qua = tinh_metrics_tu_counts(
        int(dong["tp"] or 0),
        int(dong["tn"] or 0),
        int(dong["fp"] or 0),
        int(dong["fn"] or 0),
        auc,
    )
    ket_qua["model"] = ten_model
    xac_nhan(
        f"Confusion matrix {ten_model} bằng số dòng tập đánh giá",
        ket_qua["n"] == so_dong,
        f"TP+TN+FP+FN={ket_qua['n']}, số dòng={so_dong}",
    )
    xac_nhan(
        f"TP+FN {ten_model} bằng số late thật",
        ket_qua["tp"] + ket_qua["fn"]
        == int(df_prediction.filter(F.col("is_late") == 1).count()),
        f"TP+FN={ket_qua['tp'] + ket_qua['fn']}",
    )
    xac_nhan(
        f"TN+FP {ten_model} bằng số not late thật",
        ket_qua["tn"] + ket_qua["fp"]
        == int(df_prediction.filter(F.col("is_late") == 0).count()),
        f"TN+FP={ket_qua['tn'] + ket_qua['fp']}",
    )
    so_canh_bao = int(
        df_prediction.filter(F.col("prediction_common") == 1).count()
    )
    so_khong_canh_bao = int(
        df_prediction.filter(F.col("prediction_common") == 0).count()
    )
    xac_nhan(
        f"TP+FP {ten_model} bằng số order được cảnh báo",
        ket_qua["tp"] + ket_qua["fp"] == so_canh_bao,
        f"TP+FP={ket_qua['tp'] + ket_qua['fp']}, cảnh báo={so_canh_bao}",
    )
    xac_nhan(
        f"TN+FN {ten_model} bằng số order không được cảnh báo",
        ket_qua["tn"] + ket_qua["fn"] == so_khong_canh_bao,
        "TN+FN="
        f"{ket_qua['tn'] + ket_qua['fn']}, không cảnh báo={so_khong_canh_bao}",
    )
    return ket_qua


def metrics_python(danh_sach, cot_probability, threshold, auc):
    """Tính confusion matrix cho một threshold từ danh sách validation cục bộ."""
    tp = tn = fp = fn = 0
    for dong in danh_sach:
        label = int(dong["is_late"])
        prediction = 1 if float(dong[cot_probability]) >= threshold else 0
        if label == 1 and prediction == 1:
            tp += 1
        elif label == 0 and prediction == 0:
            tn += 1
        elif label == 0 and prediction == 1:
            fp += 1
        else:
            fn += 1
    return tinh_metrics_tu_counts(tp, tn, fp, fn, auc)


def danh_gia_common_threshold(
    danh_sach_validation,
    threshold,
    giai_do,
    auc_logistic,
    auc_random_forest,
):
    """Đánh giá cùng một threshold cho Logistic Regression và Random Forest.

    Ngoài metrics riêng của từng model, hàm tính average_f1, minimum_f1,
    average_recall và average_alert_rate; candidate chỉ hợp lệ khi alert rate
    của cả hai model không vượt giới hạn cấu hình.
    """
    logistic = metrics_python(
        danh_sach_validation,
        "probability_logistic",
        threshold,
        auc_logistic,
    )
    random_forest = metrics_python(
        danh_sach_validation,
        "probability_random_forest",
        threshold,
        auc_random_forest,
    )
    average_f1 = (logistic["f1"] + random_forest["f1"]) / 2
    minimum_f1 = min(logistic["f1"], random_forest["f1"])
    average_recall = (
        logistic["recall"] + random_forest["recall"]
    ) / 2
    average_alert_rate = (
        logistic["alert_rate"] + random_forest["alert_rate"]
    ) / 2
    hop_le = (
        logistic["alert_rate"] <= MAX_ALERT_RATE
        and random_forest["alert_rate"] <= MAX_ALERT_RATE
    )
    ket_qua = {
        "giai_do": giai_do,
        "common_threshold": threshold,
        "hop_le_alert_rate": int(hop_le),
        "average_f1": average_f1,
        "minimum_f1": minimum_f1,
        "average_recall": average_recall,
        "average_alert_rate": average_alert_rate,
    }
    for tien_to, metrics in [
        ("logistic", logistic),
        ("random_forest", random_forest),
    ]:
        for ten, gia_tri in metrics.items():
            ket_qua[f"{tien_to}_{ten}"] = gia_tri
    return ket_qua


def tao_bang_so_sanh_threshold_thu_cong(
    ten_tap,
    danh_sach_probability,
    danh_sach_threshold,
    auc_logistic,
    auc_random_forest,
):
    """Tính metrics của nhiều threshold mà không huấn luyện lại model."""
    # CODE_REF: MANUAL_THRESHOLD_COMPARISON
    thresholds = sorted({float(x) for x in danh_sach_threshold})
    xac_nhan(
        f"Threshold thử thủ công của {ten_tap} nằm trong [0, 1]",
        bool(thresholds) and all(0.0 <= x <= 1.0 for x in thresholds),
        f"thresholds={thresholds}",
    )
    rows = []
    for threshold in thresholds:
        for ten_model, cot_probability, auc in [
            (
                "Logistic Regression",
                "probability_logistic",
                auc_logistic,
            ),
            (
                "Random Forest",
                "probability_random_forest",
                auc_random_forest,
            ),
        ]:
            metrics = metrics_python(
                danh_sach_probability,
                cot_probability,
                threshold,
                auc,
            )
            rows.append({
                "tap_du_lieu": ten_tap,
                "model": ten_model,
                "threshold": threshold,
                **metrics,
            })
    return rows


def khoa_xep_hang_threshold(dong):
    """Tạo khóa sắp hạng candidate đúng thứ tự ưu tiên nghiên cứu."""
    return (
        dong["average_f1"],
        dong["minimum_f1"],
        dong["average_recall"],
        -dong["average_alert_rate"],
        dong["common_threshold"],
    )


def chon_common_threshold(candidates):
    """Chọn common threshold tốt nhất trong các candidate hợp lệ."""
    # CODE_REF: SELECT_COMMON_THRESHOLD
    hop_le = [dong for dong in candidates if dong["hop_le_alert_rate"]]
    xac_nhan(
        "Có common threshold thỏa alert rate của cả hai model",
        bool(hop_le),
        f"số candidate hợp lệ={len(hop_le)}",
    )
    return max(hop_le, key=khoa_xep_hang_threshold)


def tim_common_threshold(
    danh_sach_validation,
    auc_logistic,
    auc_random_forest,
):
    """Tìm common threshold bằng coarse search rồi refine quanh điểm tốt nhất.

    Giai đoạn coarse thử 0.01–0.49 với bước 0.01; refine thử vùng ±0.03 với
    bước 0.001. Toàn bộ dữ liệu đầu vào chỉ đến từ validation.
    """
    coarse_thresholds = [i / 100 for i in range(1, 50)]
    # CODE_REF: THRESHOLD_SEARCH
    coarse_rows = [
        danh_gia_common_threshold(
            danh_sach_validation,
            threshold,
            "coarse",
            auc_logistic,
            auc_random_forest,
        )
        for threshold in coarse_thresholds
    ]
    best_coarse = chon_common_threshold(coarse_rows)
    tam = int(round(best_coarse["common_threshold"] * 1000))
    bat_dau = max(1, tam - 30)
    ket_thuc = min(499, tam + 30)
    refine_thresholds = [i / 1000 for i in range(bat_dau, ket_thuc + 1)]
    refine_rows = [
        danh_gia_common_threshold(
            danh_sach_validation,
            threshold,
            "refine",
            auc_logistic,
            auc_random_forest,
        )
        for threshold in refine_thresholds
    ]
    best_refine = chon_common_threshold(refine_rows)
    return coarse_rows, refine_rows, best_coarse, best_refine


def tinh_roc_auc_thu_cong(ten_model, df_probability, auc_cua_spark):
    """Tạo đầy đủ ROC points và tính AUC thủ công bằng hình thang.

    Score được sắp giảm dần và các order có score bằng nhau được xử lý cùng một
    ngưỡng. Hàm xuất diện tích từng trapezoid và assertion độ lệch với AUC Spark
    không vượt 1e-6.
    """
    # CODE_REF: ROC_POINTS
    scores = [
        (
            float(dong["probability_late"]),
            int(dong["is_late"]),
            dong["order_id"],
        )
        for dong in df_probability.select(
            "order_id", "is_late", "probability_late"
        ).collect()
    ]
    scores.sort(key=lambda x: (-x[0], x[2]))
    positives = sum(label for _, label, _ in scores)
    negatives = len(scores) - positives
    xac_nhan(
        f"{ten_model} có cả hai label để tạo ROC curve",
        positives > 0 and negatives > 0,
        f"positive={positives}, negative={negatives}",
    )

    roc_points = [{
        "model": ten_model,
        "point_index": 0,
        "threshold_score": "above_max_probability",
        "tp_cumulative": 0,
        "fp_cumulative": 0,
        "tpr": 0.0,
        "fpr": 0.0,
        "total_positive": positives,
        "total_negative": negatives,
    }]
    tp_cum = fp_cum = 0
    vi_tri = 0
    while vi_tri < len(scores):
        score = scores[vi_tri][0]
        tie_tp = tie_fp = 0
        while vi_tri < len(scores) and scores[vi_tri][0] == score:
            if scores[vi_tri][1] == 1:
                tie_tp += 1
            else:
                tie_fp += 1
            vi_tri += 1
        tp_cum += tie_tp
        fp_cum += tie_fp
        roc_points.append({
            "model": ten_model,
            "point_index": len(roc_points),
            "threshold_score": score,
            "tp_cumulative": tp_cum,
            "fp_cumulative": fp_cum,
            "tpr": chia_an_toan(tp_cum, positives),
            "fpr": chia_an_toan(fp_cum, negatives),
            "total_positive": positives,
            "total_negative": negatives,
        })

    trapezoids = []
    for index in range(len(roc_points) - 1):
        hien_tai = roc_points[index]
        tiep_theo = roc_points[index + 1]
        # CODE_REF: AUC_TRAPEZOID
        dien_tich = (
            (tiep_theo["fpr"] - hien_tai["fpr"])
            * (tiep_theo["tpr"] + hien_tai["tpr"])
            / 2
        )
        trapezoids.append({
            "model": ten_model,
            "trapezoid_index": index,
            "point_i": hien_tai["point_index"],
            "point_i_plus_1": tiep_theo["point_index"],
            "fpr_i": hien_tai["fpr"],
            "tpr_i": hien_tai["tpr"],
            "fpr_i_plus_1": tiep_theo["fpr"],
            "tpr_i_plus_1": tiep_theo["tpr"],
            "trapezoid_area": dien_tich,
        })
    auc_manual = math.fsum(dong["trapezoid_area"] for dong in trapezoids)
    do_lech = abs(auc_manual - auc_cua_spark)
    xac_nhan(
        f"AUC_manual {ten_model} khớp AUC_Spark",
        do_lech <= 0.000001,
        "AUC_manual="
        f"{auc_manual:.17g}, AUC_Spark={auc_cua_spark:.17g}, "
        f"độ lệch={do_lech:.17g}",
    )
    return roc_points, trapezoids, auc_manual, do_lech


def sigmoid_on_dinh(z):
    """Tính sigmoid theo hai nhánh để tránh overflow khi |z| lớn."""
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    exp_z = math.exp(z)
    return exp_z / (1.0 + exp_z)


def loai_ket_qua(label, prediction):
    """Đổi cặp label/prediction thành TP, TN, FP hoặc FN."""
    if int(label) == 1 and int(prediction) == 1:
        return "TP"
    if int(label) == 0 and int(prediction) == 0:
        return "TN"
    if int(label) == 0 and int(prediction) == 1:
        return "FP"
    return "FN"


def lay_mot_order(df, dieu_kien, tang_dan, da_chon):
    """Lấy một order xác định theo điều kiện, score và order_id tie-break."""
    tap = df.filter(dieu_kien)
    if da_chon:
        tap = tap.filter(~F.col("order_id").isin(sorted(da_chon)))
    thu_tu = F.col("probability_late").asc()
    if not tang_dan:
        thu_tu = F.col("probability_late").desc()
    ket_qua = tap.orderBy(thu_tu, F.col("order_id").asc()).limit(1).collect()
    return ket_qua[0] if ket_qua else None


def chon_bon_order(df_model_demo, common_threshold):
    """Chọn bốn order A–D theo các nhóm TP, FP, FN, TN của model demo.

    Việc chọn hoàn toàn xác định, không ngẫu nhiên. Nếu một nhóm rỗng, hàm dùng
    quy tắc thay thế có ghi chú và không chọn trùng order đã dùng trước đó.
    """
    # CODE_REF: SELECT_DEMO_ORDERS
    quy_tac = {
        "A": (
            (F.col("is_late") == 1) & (F.col("prediction_common") == 1),
            False,
            "True Positive có probability_late cao nhất",
        ),
        "B": (
            (F.col("is_late") == 0) & (F.col("prediction_common") == 1),
            False,
            "False Positive có probability_late cao nhất",
        ),
        "C": (
            (F.col("is_late") == 1)
            & (F.col("prediction_common") == 0)
            & (F.col("probability_late") < F.lit(common_threshold)),
            False,
            "False Negative gần common threshold nhất ở phía dưới",
        ),
        "D": (
            (F.col("is_late") == 0) & (F.col("prediction_common") == 0),
            True,
            "True Negative có probability_late thấp nhất",
        ),
    }
    da_chon = set()
    ket_qua = []
    for alias in ["A", "B", "C", "D"]:
        dieu_kien, tang_dan, mo_ta = quy_tac[alias]
        dong = lay_mot_order(df_model_demo, dieu_kien, tang_dan, da_chon)
        thay_the = "không"
        if dong is None:
            if alias in ("A", "C"):
                fallback = F.col("is_late") == 1
            else:
                fallback = F.col("is_late") == 0
            dong = lay_mot_order(
                df_model_demo,
                fallback,
                alias == "D",
                da_chon,
            )
            thay_the = (
                "Nhóm yêu cầu không có order; dùng order cùng label theo "
                "hướng probability gần nhất với quy tắc gốc."
            )
        if dong is None:
            dong = lay_mot_order(
                df_model_demo,
                F.lit(True),
                alias == "D",
                da_chon,
            )
            thay_the = (
                "Không còn order cùng label; dùng order chưa chọn theo hướng "
                "probability của quy tắc gốc."
            )
        xac_nhan(
            f"Chọn được order {alias}",
            dong is not None,
            f"quy tắc={mo_ta}",
        )
        da_chon.add(dong["order_id"])
        ket_qua.append({
            "alias": alias,
            "order_id": dong["order_id"],
            "quy_tac": mo_ta,
            "quy_tac_thay_the": thay_the,
        })
    return ket_qua


def phan_tich_logistic(
    model,
    ten_feature,
    demo_rows,
):
    """Kiểm chứng Logistic Regression score của các order demo.

    Hàm xuất toàn bộ coefficients, liệt kê transformed feature khác 0, tính từng
    contribution, dựng lại z = intercept + Σ(coefficient × value), áp dụng
    sigmoid và kiểm tra probability_manual khớp probability của Spark 1e-10.
    """
    coefficients = [float(x) for x in model.coefficients]
    intercept = float(model.intercept)
    xac_nhan(
        "Số Logistic Regression coefficients bằng features vector",
        len(coefficients) == len(ten_feature),
        f"coefficients={len(coefficients)}, features={len(ten_feature)}",
    )
    coefficient_rows = [
        {
            "feature_index": i,
            "transformed_feature_name": ten_feature[i],
            "coefficient": coefficients[i],
            "intercept": intercept,
        }
        for i in range(len(ten_feature))
    ]
    breakdown_rows = []
    summaries = {}
    for dong in demo_rows:
        vector = [float(x) for x in dong["features_logistic"].toArray()]
        contributions = []
        for index, value in enumerate(vector):
            if value != 0.0:
                contribution = value * coefficients[index]
                contributions.append({
                    "feature_index": index,
                    "transformed_feature_name": ten_feature[index],
                    "feature_value": value,
                    "coefficient": coefficients[index],
                    "contribution": contribution,
                })
        sum_contributions = math.fsum(
            x["contribution"] for x in contributions
        )
        z_manual = intercept + sum_contributions
        # CODE_REF: VERIFY_LOGISTIC_SCORE
        probability_manual = sigmoid_on_dinh(z_manual)
        probability_spark = float(dong["probability_logistic"])
        absolute_difference = abs(probability_manual - probability_spark)
        xac_nhan(
            f"Logistic Regression probability order {dong['alias']} khớp Spark",
            absolute_difference <= 1e-10,
            "manual="
            f"{probability_manual:.17g}, Spark={probability_spark:.17g}, "
            f"độ lệch={absolute_difference:.17g}",
        )
        for contribution in contributions:
            breakdown_rows.append({
                "alias": dong["alias"],
                "order_id": dong["order_id"],
                **contribution,
                "sum_contributions": sum_contributions,
                "intercept": intercept,
                "z_manual": z_manual,
                "probability_manual": probability_manual,
                "probability_spark": probability_spark,
                "absolute_difference": absolute_difference,
            })
        summaries[dong["alias"]] = {
            "contributions": contributions,
            "sum_contributions": sum_contributions,
            "intercept": intercept,
            "z_manual": z_manual,
            "probability_manual": probability_manual,
            "probability_spark": probability_spark,
            "absolute_difference": absolute_difference,
        }
    return coefficient_rows, breakdown_rows, summaries


def phan_tich_random_forest(
    model,
    ten_feature,
    demo_rows,
    common_threshold,
):
    """Kiểm chứng Random Forest score và trích xuất cấu trúc model.

    Hàm chuẩn hóa rawPrediction để tính probability_manual, lấy leaf index nếu
    public API hỗ trợ, xuất feature importance, depth/node count từng decision
    tree và model.toDebugString. Không dùng coefficient hoặc sigmoid.
    """
    importances = [float(x) for x in model.featureImportances]
    xac_nhan(
        "Số Random Forest feature importance bằng features vector",
        len(importances) == len(ten_feature),
        f"importances={len(importances)}, features={len(ten_feature)}",
    )
    importance_rows = [
        {
            "feature_index": i,
            "transformed_feature_name": ten_feature[i],
            "feature_importance": importances[i],
        }
        for i in range(len(ten_feature))
    ]
    trees = list(model.trees)
    weights = [float(x) for x in model.treeWeights]
    xac_nhan(
        "Random Forest có đúng số decision tree đã cấu hình",
        len(trees) == RF_NUM_TREES and len(weights) == RF_NUM_TREES,
        f"trees={len(trees)}, weights={len(weights)}",
    )
    tree_rows = [
        {
            "tree_index": i,
            "tree_weight": weights[i],
            "depth": int(tree.depth),
            "num_nodes": int(tree.numNodes),
            "tree_debug_string": tree.toDebugString,
        }
        for i, tree in enumerate(trees)
    ]
    breakdown_rows = []
    summaries = {}
    for dong in demo_rows:
        vector = dong["features_random_forest"]
        raw_model = [float(x) for x in dong["raw_random_forest"].toArray()]
        raw_total = math.fsum(raw_model)
        # CODE_REF: VERIFY_RANDOM_FOREST_SCORE
        probability_manual = chia_an_toan(raw_model[1], raw_total)
        probability_spark = float(dong["probability_random_forest"])
        probability_difference = abs(probability_manual - probability_spark)
        xac_nhan(
            f"Random Forest probability order {dong['alias']} khớp Spark",
            probability_difference <= 1e-10,
            "manual="
            f"{probability_manual:.17g}, Spark={probability_spark:.17g}, "
            f"độ lệch={probability_difference:.17g}",
        )
        leaves = [float(x) for x in dong["leaf_indices"].toArray()]
        xac_nhan(
            f"Order {dong['alias']} có leaf index của mọi decision tree",
            len(leaves) == len(trees),
            f"leaf indices={len(leaves)}, trees={len(trees)}",
        )
        weighted_0 = []
        weighted_1 = []
        per_tree = []
        for index, tree in enumerate(trees):
            tree_raw = [float(x) for x in tree.predictRaw(vector)]
            tree_probability = [
                float(x) for x in tree.predictProbability(vector)
            ]
            leaf_public = float(tree.predictLeaf(vector))
            xac_nhan(
                f"Leaf index tree {index} order {dong['alias']} khớp leafCol",
                abs(leaf_public - leaves[index]) <= 1e-12,
                f"predictLeaf={leaf_public}, leafCol={leaves[index]}",
            )
            contribution_0 = weights[index] * tree_probability[0]
            contribution_1 = weights[index] * tree_probability[1]
            weighted_0.append(contribution_0)
            weighted_1.append(contribution_1)
            chi_tiet = {
                "tree_index": index,
                "leaf_index": leaves[index],
                "tree_weight": weights[index],
                "tree_raw_prediction_0": tree_raw[0],
                "tree_raw_prediction_1": tree_raw[1],
                "tree_probability_0": tree_probability[0],
                "tree_probability_1": tree_probability[1],
                "weighted_contribution_0": contribution_0,
                "weighted_contribution_1": contribution_1,
            }
            per_tree.append(chi_tiet)
            breakdown_rows.append({
                "alias": dong["alias"],
                "order_id": dong["order_id"],
                **chi_tiet,
                "model_raw_prediction_0": raw_model[0],
                "model_raw_prediction_1": raw_model[1],
                "model_raw_total": raw_total,
                "probability_manual": probability_manual,
                "probability_spark": probability_spark,
                "absolute_difference": probability_difference,
                "prediction_common": int(
                    probability_spark >= common_threshold
                ),
                "common_threshold": common_threshold,
            })
        sum_0 = math.fsum(weighted_0)
        sum_1 = math.fsum(weighted_1)
        xac_nhan(
            f"Tổng treeWeights contribution order {dong['alias']} khớp rawPrediction",
            abs(sum_0 - raw_model[0]) <= 1e-10
            and abs(sum_1 - raw_model[1]) <= 1e-10,
            "tổng tree="
            f"[{sum_0:.17g}, {sum_1:.17g}], model raw="
            f"[{raw_model[0]:.17g}, {raw_model[1]:.17g}]",
        )
        summaries[dong["alias"]] = {
            "raw_prediction_0": raw_model[0],
            "raw_prediction_1": raw_model[1],
            "raw_total": raw_total,
            "probability_manual": probability_manual,
            "probability_spark": probability_spark,
            "absolute_difference": probability_difference,
            "leaf_indices": leaves,
            "per_tree": per_tree,
            "weighted_sum_0": sum_0,
            "weighted_sum_1": sum_1,
        }
    return importance_rows, tree_rows, breakdown_rows, summaries


def metrics_ra_dong(ten_model, metrics, common_threshold):
    """Đóng gói metrics của một model thành một dòng để ghi CSV."""
    return {
        "model": ten_model,
        "common_threshold": common_threshold,
        **metrics,
    }


def confusion_ra_dong(ten_model, metrics, common_threshold, tap_du_lieu):
    """Tạo một dòng confusion matrix kèm các tổng kiểm chứng hàng/cột."""
    return {
        "tap_du_lieu": tap_du_lieu,
        "model": ten_model,
        "common_threshold": common_threshold,
        "true_positive": metrics["tp"],
        "true_negative": metrics["tn"],
        "false_positive": metrics["fp"],
        "false_negative": metrics["fn"],
        "actual_late_total": metrics["tp"] + metrics["fn"],
        "actual_not_late_total": metrics["tn"] + metrics["fp"],
        "alert_total": metrics["tp"] + metrics["fp"],
        "no_alert_total": metrics["tn"] + metrics["fn"],
        "total": metrics["n"],
    }


def ve_confusion_matrix(duong_dan, ten_model, metrics):
    """Vẽ confusion matrix 2×2 và ghi số TN, FP, FN, TP trực tiếp lên ô."""
    matrix = [
        [metrics["tn"], metrics["fp"]],
        [metrics["fn"], metrics["tp"]],
    ]
    fig, ax = plt.subplots(figsize=(6.5, 5.5))
    anh = ax.imshow(matrix, cmap="Blues")
    nguong_mau = max(max(row) for row in matrix) / 2
    for i in range(2):
        for j in range(2):
            ax.text(
                j,
                i,
                f"{matrix[i][j]:,}",
                ha="center",
                va="center",
                color="white" if matrix[i][j] > nguong_mau else "black",
            )
    ax.set_xticks([0, 1], ["Predicted 0", "Predicted 1"])
    ax.set_yticks([0, 1], ["Actual 0", "Actual 1"])
    ax.set_title(f"Confusion matrix - {ten_model}")
    fig.colorbar(anh, ax=ax)
    fig.tight_layout()
    fig.savefig(duong_dan, dpi=200)
    plt.close(fig)


def ve_bieu_do_threshold(refine_rows, common_threshold):
    """Vẽ F1 và alert rate của hai model trên toàn bộ vùng refine."""
    thresholds = [x["common_threshold"] for x in refine_rows]
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.plot(
        thresholds,
        [x["logistic_f1"] for x in refine_rows],
        label="Logistic Regression F1",
    )
    ax.plot(
        thresholds,
        [x["random_forest_f1"] for x in refine_rows],
        label="Random Forest F1",
    )
    ax.plot(
        thresholds,
        [x["average_f1"] for x in refine_rows],
        label="average_f1",
        linewidth=2.5,
    )
    ax.axvline(
        common_threshold,
        color="black",
        linestyle="--",
        label=f"common threshold = {common_threshold:.3f}",
    )
    ax.set_xlabel("common threshold")
    ax.set_ylabel("F1")
    ax.set_title("Validation common threshold selection by F1")
    ax2 = ax.twinx()
    ax2.plot(
        thresholds,
        [x["logistic_alert_rate"] for x in refine_rows],
        linestyle=":",
        alpha=0.65,
        label="Logistic Regression alert rate",
    )
    ax2.plot(
        thresholds,
        [x["random_forest_alert_rate"] for x in refine_rows],
        linestyle=":",
        alpha=0.65,
        label="Random Forest alert rate",
    )
    ax2.axhline(MAX_ALERT_RATE, color="red", linestyle="--", label="alert rate limit = 20%")
    ax2.set_ylabel("alert rate")
    handles_1, labels_1 = ax.get_legend_handles_labels()
    handles_2, labels_2 = ax2.get_legend_handles_labels()
    ax.legend(handles_1 + handles_2, labels_1 + labels_2, loc="best")
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(CHART_DIR / "05_common_threshold_validation.png", dpi=200)
    plt.close(fig)


def ve_roc(roc_logistic, roc_random_forest, auc_lr, auc_rf):
    """Vẽ hai ROC curve và baseline majority class có AUC bằng 0.5."""
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.plot(
        [x["fpr"] for x in roc_logistic],
        [x["tpr"] for x in roc_logistic],
        label=f"Logistic Regression AUC={auc_lr:.6f}",
    )
    ax.plot(
        [x["fpr"] for x in roc_random_forest],
        [x["tpr"] for x in roc_random_forest],
        label=f"Random Forest AUC={auc_rf:.6f}",
    )
    ax.plot(
        [0, 1],
        [0, 1],
        color="gray",
        linestyle="--",
        label="Baseline majority class AUC=0.500000",
    )
    ax.set_xlabel("FPR")
    ax.set_ylabel("TPR")
    ax.set_title("ROC curve trên test")
    ax.legend()
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(CHART_DIR / "05_roc_curve_test.png", dpi=200)
    plt.close(fig)


def ve_probability_distribution(scores_lr, scores_rf, common_threshold):
    """Vẽ phân phối probability_late của hai model và common threshold."""
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(scores_lr, bins=50, alpha=0.55, label="Logistic Regression")
    ax.hist(scores_rf, bins=50, alpha=0.55, label="Random Forest")
    ax.axvline(
        common_threshold,
        color="black",
        linestyle="--",
        label=f"common threshold = {common_threshold:.3f}",
    )
    ax.set_xlabel("probability_late")
    ax.set_ylabel("Số order")
    ax.set_title("Probability distribution trên test")
    ax.legend()
    fig.tight_layout()
    fig.savefig(CHART_DIR / "05_probability_distribution_test.png", dpi=200)
    plt.close(fig)


def ve_so_sanh_model(comparison_results):
    """Vẽ các metrics test chính thức của baseline và hai model."""
    metrics = ["accuracy", "precision", "recall", "f1", "auc", "alert_rate"]
    x = list(range(len(metrics)))
    width = 0.25
    offsets = [-width, 0.0, width]
    fig, ax = plt.subplots(figsize=(12, 6.5))
    for ten_phuong_phap, offset in zip(TEN_PHUONG_PHAP, offsets):
        bars = ax.bar(
            [i + offset for i in x],
            [comparison_results[ten_phuong_phap][m] for m in metrics],
            width,
            label=ten_phuong_phap,
        )
        for bar, metric in zip(bars, metrics):
            value = comparison_results[ten_phuong_phap][metric]
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                max(bar.get_height(), 0.005) + 0.012,
                f"{value * 100:.2f}%",
                ha="center",
                va="bottom",
                fontsize=7,
                rotation=90,
            )
    ax.set_xticks(x, [m.upper() if m == "auc" else m.title() for m in metrics])
    ax.set_ylim(0, 1)
    ax.set_ylabel("Metric value")
    ax.set_title("Baseline và hai model trên cùng test split")
    ax.legend()
    fig.tight_layout()
    fig.savefig(CHART_DIR / "05_so_sanh_mo_hinh.png", dpi=200)
    plt.close(fig)


def md_escape(gia_tri):
    """Escape ký tự có ý nghĩa đặc biệt trước khi đưa giá trị vào Markdown."""
    return (
        str(gia_tri)
        .replace("|", "\\|")
        .replace("\r\n", "<br>")
        .replace("\n", "<br>")
    )


def md_table(headers, rows):
    """Tạo bảng Markdown từ header và các dòng dạng list hoặc dictionary."""
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        if isinstance(row, dict):
            values = [row.get(h, "") for h in headers]
        else:
            values = row
        lines.append("| " + " | ".join(md_escape(x) for x in values) + " |")
    return "\n".join(lines)


def fmt(gia_tri, so_chu_so=12):
    """Định dạng số nguyên có phân cách hoặc số thực với độ chính xác yêu cầu."""
    if isinstance(gia_tri, int):
        return f"{gia_tri:,}"
    return f"{float(gia_tri):.{so_chu_so}f}"


def pct(gia_tri):
    """Định dạng tỷ lệ 0–1 thành phần trăm với sáu chữ số thập phân."""
    return f"{float(gia_tri) * 100:.6f}%"


def tim_code_ref(marker, so_dong_toi_da=8):
    """Tìm CODE_REF trong chính file này để báo cáo dùng line number hiện tại.

    Hàm đọc source sau khi sửa, tìm marker và lấy đoạn code ngay sau marker;
    nhờ đó bảng “Dòng code – Tính năng” không dùng line number cũ/hard-code.
    """
    lines = Path(__file__).read_text(encoding="utf-8").splitlines()
    target = f"# CODE_REF: {marker}"
    for vi_tri, line in enumerate(lines):
        if line.strip() == target:
            bat_dau = vi_tri + 1
            while bat_dau < len(lines) and not lines[bat_dau].strip():
                bat_dau += 1
            ket_thuc = bat_dau
            da_co_code = False
            while ket_thuc < len(lines) and ket_thuc < bat_dau + so_dong_toi_da:
                stripped = lines[ket_thuc].strip()
                if stripped.startswith("# CODE_REF:"):
                    break
                if not stripped and da_co_code:
                    break
                if stripped:
                    da_co_code = True
                ket_thuc += 1
            code = "\n".join(lines[bat_dau:ket_thuc]).strip()
            return {
                "line": (
                    str(bat_dau + 1)
                    if ket_thuc == bat_dau + 1
                    else f"{bat_dau + 1}-{ket_thuc}"
                ),
                "code": code,
            }
    raise RuntimeError(f"Không tìm thấy code marker {marker}")


def mo_ta_ly_do_neighbor(neighbor, chosen):
    """Giải thích bằng dữ liệu vì sao candidate lân cận không được chọn."""
    if neighbor is None:
        return "Không có candidate liền kề trong vùng refine."
    if not neighbor["hop_le_alert_rate"]:
        return (
            "Không hợp lệ vì alert rate của ít nhất một model vượt 20%: "
            f"Logistic Regression={pct(neighbor['logistic_alert_rate'])}, "
            f"Random Forest={pct(neighbor['random_forest_alert_rate'])}."
        )
    tieu_chi = [
        ("average_f1", True),
        ("minimum_f1", True),
        ("average_recall", True),
        ("average_alert_rate", False),
        ("common_threshold", True),
    ]
    for ten, lon_hon_tot_hon in tieu_chi:
        a = neighbor[ten]
        b = chosen[ten]
        if a != b:
            huong = "thấp hơn" if lon_hon_tot_hon else "cao hơn"
            return (
                f"Bị xếp sau tại tiêu chí `{ten}`: {fmt(a)} {huong} "
                f"giá trị được chọn {fmt(b)}."
            )
    return "Các tiêu chí bằng nhau; quy tắc cuối cùng giữ candidate đã chọn."


def tao_code_reference(context):
    """Tạo các dòng đối chiếu code, công thức, số liệu thật và cách kiểm tra."""
    split = {x["tap_du_lieu"]: x for x in context["split_rows"]}
    test_lr = context["test_results"]["Logistic Regression"]
    test_baseline = context["baseline_test"]
    markers = [
        ("READ_DATASET", "Đọc dataset", "spark.read.csv", f"{split['toan_bo']['so_dong']} dòng"),
        ("FEATURE_LEAKAGE", "Kiểm tra feature leakage", "selected features ∩ leakage columns = ∅", "PASS"),
        ("NAN_INFINITY", "Kiểm tra NaN/Infinity", "isnan(x) hoặc x=±Infinity", "PASS"),
        ("LABEL_INPUT", "Tạo label input", "label = cast(is_late as double)", f"late={split['toan_bo']['so_don_late']}"),
        ("SPLIT_TRAIN_TEST", "Chia train_full/test", "80%/20%, seed=42", f"{split['train_full']['so_dong']}/{split['test']['so_dong']}"),
        ("SPLIT_TRAIN_VALIDATION", "Chia train_fit/validation", "80%/20% của train_full, seed=42", f"{split['train_fit']['so_dong']}/{split['validation']['so_dong']}"),
        ("STRING_INDEXER", "StringIndexer", "category → index", f"{len(COT_PHAN_LOAI)} categorical feature"),
        ("ONE_HOT_ENCODER", "OneHotEncoder", "index → sparse vector", f"vector={len(context['feature_names_final'])}"),
        ("IMPUTER", "Imputer", "median theo train scope", f"{len(COT_SO)} numeric feature"),
        ("VECTOR_ASSEMBLER", "VectorAssembler", "filled numeric + encoded categorical", f"dimension={len(context['feature_names_final'])}"),
        ("BASELINE_MAJORITY", "Tạo baseline", "majority class chỉ xác định từ train_full; áp dụng cố định lên test", f"prediction={context['baseline_label_test']}, AUC={fmt(test_baseline['auc'], 6)}"),
        ("FIT_LOGISTIC", "Fit Logistic Regression", "maxIter=50, regParam=0.01", f"intercept={fmt(context['intercept'])}"),
        ("FIT_RANDOM_FOREST", "Fit Random Forest", "numTrees=30, maxDepth=6, seed=42", f"trees={len(context['tree_rows'])}"),
        ("PROBABILITY_LATE", "Tạo probability_late", "vector_to_array(probability)[1]", "hai model"),
        ("THRESHOLD_SEARCH", "Tìm common threshold", "coarse 0.01; refine 0.001", f"{context['candidate_count']} candidate"),
        ("SELECT_COMMON_THRESHOLD", "Chọn common threshold", "max average_f1 → minimum_f1 → average_recall → min average_alert_rate → max threshold", fmt(context['common_threshold'], 3)),
        ("MANUAL_THRESHOLD_COMPARISON", "So sánh threshold thủ công", "tính lại confusion matrix và metrics tại danh sách threshold cấu hình", f"{len(context['manual_threshold_rows'])} dòng"),
        ("PREDICTION_COMMON", "Tạo prediction_common", "1 nếu probability_late ≥ common threshold", fmt(context['common_threshold'], 3)),
        ("CONFUSION_MATRIX", "Tính confusion matrix", "đếm TP, TN, FP, FN", f"TP={test_lr['tp']}, TN={test_lr['tn']}, FP={test_lr['fp']}, FN={test_lr['fn']}"),
        ("METRIC_ACCURACY", "Tính Accuracy", "(TP+TN)/N", fmt(test_lr["accuracy"])),
        ("METRIC_PRECISION", "Tính Precision", "TP/(TP+FP)", fmt(test_lr["precision"])),
        ("METRIC_RECALL", "Tính Recall", "TP/(TP+FN)", fmt(test_lr["recall"])),
        ("METRIC_SPECIFICITY", "Tính Specificity", "TN/(TN+FP)", fmt(test_lr["specificity"])),
        ("METRIC_FPR", "Tính FPR", "FP/(FP+TN)", fmt(test_lr["fpr"])),
        ("METRIC_F1", "Tính F1", "2×Precision×Recall/(Precision+Recall)", fmt(test_lr["f1"])),
        ("METRIC_ALERT_RATE", "Tính alert rate", "(TP+FP)/N", fmt(test_lr["alert_rate"])),
        ("ROC_POINTS", "Tính ROC points", "sắp probability giảm dần; TPR=TP/P, FPR=FP/N", f"LR={len(context['roc_lr'])} points"),
        ("AUC_TRAPEZOID", "Tính AUC bằng trapezoid", "ΔFPR×(TPR_i+TPR_i+1)/2", fmt(context['auc_manual_lr'])),
        ("VERIFY_LOGISTIC_SCORE", "Kiểm chứng Logistic Regression probability", "sigmoid(intercept+Σ coefficient×value)", "PASS cho A, B, C, D"),
        ("VERIFY_RANDOM_FOREST_SCORE", "Kiểm chứng Random Forest probability", "rawPrediction[1]/ΣrawPrediction", "PASS cho A, B, C, D"),
        ("SELECT_DEMO_ORDERS", "Chọn order A, B, C, D", "quy tắc xác định trên test của model demo", ", ".join(f"{x['alias']}={x['order_id']}" for x in context['demo_rows'])),
    ]
    rows = []
    for marker, feature, formula, actual in markers:
        ref = tim_code_ref(marker)
        rows.append({
            "marker": marker,
            "dong_code_thuc_te": ref["line"],
            "code_thuc_te": ref["code"],
            "tinh_nang": feature,
            "cong_thuc": formula,
            "so_lieu_that": actual,
            "cach_kiem_tra": "Assertion/CSV/report từ lần chạy hiện tại",
        })
    return rows


def tao_bao_cao(context):
    """Sinh báo cáo Markdown duy nhất từ context của lần chạy hiện tại.

    Hàm không chứa số kết quả nghiên cứu hard-code; mọi row count, threshold,
    confusion matrix, metrics, coefficients, tree details và order demo đều được
    truyền từ model/dataset vừa chạy.
    """
    split = {x["tap_du_lieu"]: x for x in context["split_rows"]}
    validation = context["validation_results"]
    test = context["test_results"]
    baseline_validation = context["baseline_validation"]
    baseline_test = context["baseline_test"]
    comparison_results = context["comparison_results"]
    chosen = context["chosen_candidate"]
    demo = context["demo_rows"]
    demo_by_alias = {x["alias"]: x for x in demo}
    lines = []
    add = lines.append

    add("# BÁO CÁO KIỂM CHỨNG BƯỚC 05 — LẦN CHẠY CHÍNH THỨC DÙNG F1")
    add("")
    add("## PHẦN 1. THÔNG TIN LẦN CHẠY")
    add("")
    add(f"- Thời gian bắt đầu: `{context['start_time']}`")
    add(f"- Thời gian kết thúc: `{context['end_time']}`")
    add(f"- Thời lượng: `{context['duration_seconds']:.3f}` giây")
    add(f"- Đường dẫn project: `{PROJECT_DIR}`")
    add(f"- Đường dẫn dataset: `{DATA_FILE}`")
    add(f"- Python: `{context['python_version']}`")
    add(f"- PySpark: `{context['pyspark_version']}`")
    add(f"- Java: `{context['java_version']}`")
    add(f"- Run tag: `{RUN_TAG}`")
    add(f"- Seed: `{SEED}`")
    add(f"- Threshold mode: `{context['threshold_mode']}`")
    add(f"- MANUAL_COMMON_THRESHOLD: `{MANUAL_COMMON_THRESHOLD}`")
    add(f"- Threshold thử thủ công: `{context['thresholds_so_sanh_thu_cong']}`")
    add(f"- Số dòng dataset: `{split['toan_bo']['so_dong']:,}`")
    add(f"- SHA-256 dataset trước và sau chạy: `{context['dataset_hash_after']}`")
    add("- Dataset gốc không bị sửa: **PASS** (SHA-256, kích thước và thời điểm sửa file không đổi).")
    add("- Danh sách file output:")
    add("")
    for path in context["all_outputs"]:
        add(f"  - `{path.relative_to(PROJECT_DIR)}`")

    add("")
    add("## PHẦN 2. SƠ ĐỒ QUY TRÌNH")
    add("")
    add("```text")
    add("dataset")
    add("  → train_full / test")
    add("  → train_fit / validation (chỉ chia tiếp train_full)")
    add("  → preprocessing fit trên train_fit")
    add("  → baseline majority class xác định chỉ từ train")
    add("  → Logistic Regression và Random Forest")
    add("  → common threshold selection trên validation")
    add("  → khóa common threshold và model demo")
    add("  → retrain preprocessing và hai model trên train_full")
    add("  → đánh giá baseline và hai model trên cùng test split")
    add("  → kiểm chứng score, confusion matrix, AUC và công thức")
    add("```")

    add("")
    add("## PHẦN 3. BẢNG “DÒNG CODE – TÍNH NĂNG – CÔNG THỨC – SỐ LIỆU THẬT”")
    add("")
    code_headers = [
        "Dòng code thực tế",
        "Code thực tế",
        "Tính năng",
        "Công thức",
        "Số liệu thật",
        "Cách kiểm tra",
    ]
    code_rows = [[
        x["dong_code_thuc_te"],
        f"`{x['code_thuc_te']}`",
        x["tinh_nang"],
        x["cong_thuc"],
        x["so_lieu_that"],
        x["cach_kiem_tra"],
    ] for x in context["code_rows"]]
    add(md_table(code_headers, code_rows))

    add("")
    add("## PHẦN 4. DATASET VÀ DATA SPLIT")
    add("")
    for ten_tap in ["toan_bo", "train_full", "train_fit", "validation", "test"]:
        row = split[ten_tap]
        add(
            f"- `{ten_tap}` late rate = {row['so_don_late']:,} / "
            f"{row['so_dong']:,} = {fmt(row['late_rate'])} = "
            f"{pct(row['late_rate'])}."
        )
    add("")
    add(
        f"Kiểm tra tổng: {split['train_full']['so_dong']:,} + "
        f"{split['test']['so_dong']:,} = {split['toan_bo']['so_dong']:,}; "
        f"{split['train_fit']['so_dong']:,} + "
        f"{split['validation']['so_dong']:,} = "
        f"{split['train_full']['so_dong']:,}."
    )
    add("")
    add(md_table(
        ["Tập", "Dòng", "order_id khác nhau", "late", "not late", "late rate", "Giao nhau"],
        [[
            x["tap_du_lieu"], x["so_dong"], x["so_order_id_khac_nhau"],
            x["so_don_late"], x["so_don_not_late"], pct(x["late_rate"]),
            x["kiem_tra_giao_nhau"],
        ] for x in context["split_rows"]],
    ))

    add("")
    add("## PHẦN 5. PREPROCESSING")
    add("")
    add("Pipeline lựa chọn common threshold chỉ fit trên `train_fit`; Pipeline final chỉ fit lại trên `train_full`. Không fit trên validation hoặc test.")
    add("")
    add("### Median và kích thước")
    add("")
    add(md_table(
        ["Phạm vi fit", "Component", "Feature gốc", "Chi tiết", "Giá trị"],
        [[x["pham_vi_fit"], x["component"], x["feature_goc"], x["chi_tiet"], x["gia_tri"]] for x in context["preprocessing_rows"]],
    ))
    add("")
    add("### StringIndexer mapping")
    add("")
    add(md_table(
        ["Phạm vi fit", "Feature gốc", "String index", "Category", "Invalid"],
        [[x["pham_vi_fit"], x["feature_goc"], x["string_index"], x["category"], x["la_nhom_invalid"]] for x in context["mapping_rows"]],
    ))
    add("")
    add("### Vị trí transformed feature trong features vector")
    add("")
    add(md_table(
        ["Phạm vi fit", "Vị trí", "Transformed feature name", "Feature gốc", "Metadata type"],
        [[x["pham_vi_fit"], x["feature_index"], x["transformed_feature_name"], x["feature_goc"], x["metadata_attribute_type"]] for x in context["metadata_rows"]],
    ))

    add("")
    add("### Baseline majority class")
    add("")
    add(
        "Baseline không fit model và không dùng common threshold. Majority class "
        "được xác định từ `train_fit` khi quan sát validation và từ `train_full` "
        "khi đánh giá test; validation/test không tham gia quyết định label baseline."
    )
    add("")
    add(md_table(
        ["Phạm vi", "Nguồn xác định majority class", "Prediction cố định", "Class counts nguồn", "AUC"],
        [
            ["validation", "train_fit", context["baseline_label_validation"], context["baseline_counts_validation"], fmt(baseline_validation["auc"], 6)],
            ["test", "train_full", context["baseline_label_test"], context["baseline_counts_test"], fmt(baseline_test["auc"], 6)],
        ],
    ))
    add("")
    add(
        f"Với dataset hiện tại, majority class dùng cho test là "
        f"`{context['baseline_label_test']}` "
        f"(`{'not late' if context['baseline_label_test'] == 0 else 'late'}`). "
        "Vì baseline "
        "cho cùng một probability score cho mọi order nên AUC bằng 0.5; đây là "
        "mức tham chiếu xếp hạng ngẫu nhiên, không phải common threshold."
    )

    add("")
    add("## PHẦN 6. LOGISTIC REGRESSION")
    add("")
    add(f"Cấu hình: `maxIter={LR_MAX_ITER}`, `regParam={LR_REG_PARAM}`. Intercept final: `{fmt(context['intercept'], 16)}`.")
    add("")
    add("Công thức: `z = intercept + Σ(coefficient_j × transformed_feature_j)` và `probability_manual = 1 / (1 + exp(-z))`.")
    add("")
    add(md_table(
        ["Index", "Transformed feature", "Coefficient", "Intercept"],
        [[x["feature_index"], x["transformed_feature_name"], fmt(x["coefficient"], 16), fmt(x["intercept"], 16)] for x in context["coefficient_rows"]],
    ))
    for alias in ["A", "B", "C", "D"]:
        summary = context["lr_summaries"][alias]
        add("")
        add(f"### Order {alias}: `{demo_by_alias[alias]['order_id']}`")
        add("")
        add(md_table(
            ["Index", "Transformed feature", "Value", "Coefficient", "Contribution"],
            [[x["feature_index"], x["transformed_feature_name"], fmt(x["feature_value"], 16), fmt(x["coefficient"], 16), fmt(x["contribution"], 16)] for x in summary["contributions"]],
        ))
        add("")
        add("Phép cộng đầy đủ:")
        add("")
        add("```text")
        add(f"z = {summary['intercept']:.17g}")
        for x in summary["contributions"]:
            add(
                f"  + ({x['feature_value']:.17g} × {x['coefficient']:.17g}) "
                f"[{x['transformed_feature_name']}] = {x['contribution']:.17g}"
            )
        add(f"  = {summary['z_manual']:.17g}")
        add(
            "probability_manual = 1 / (1 + exp(-z)) = "
            f"{summary['probability_manual']:.17g}"
        )
        add(f"probability_Spark = {summary['probability_spark']:.17g}")
        add(f"absolute_difference = {summary['absolute_difference']:.17g}")
        add("```")

    add("")
    add("## PHẦN 7. RANDOM FOREST")
    add("")
    add(f"Cấu hình final: số decision tree `{len(context['tree_rows'])}`, `maxDepth={RF_MAX_DEPTH}`, `seed={SEED}`.")
    add("")
    add("### Feature importance")
    add("")
    add(md_table(
        ["Index", "Transformed feature", "Feature importance"],
        [[x["feature_index"], x["transformed_feature_name"], fmt(x["feature_importance"], 16)] for x in context["importance_rows"]],
    ))
    add("")
    add("### Tree details")
    add("")
    add(md_table(
        ["Tree", "Tree weight", "Depth", "Num nodes"],
        [[x["tree_index"], fmt(x["tree_weight"]), x["depth"], x["num_nodes"]] for x in context["tree_rows"]],
    ))
    add("")
    add(f"PySpark {context['pyspark_version']} cung cấp API công khai `trees`, `treeWeights`, `predictRaw`, `predictProbability`, `leafCol` và `predictLeaf`. Vì vậy báo cáo dùng `predictProbability` công khai của từng decision tree để kiểm tra đúng phép tổng có trọng số mà Random Forest dùng tạo model `rawPrediction`; đồng thời vẫn xuất `rawPrediction` riêng của từng decision tree.")
    for alias in ["A", "B", "C", "D"]:
        summary = context["rf_summaries"][alias]
        add("")
        add(f"### Order {alias}: `{demo_by_alias[alias]['order_id']}`")
        add("")
        add(
            "`probability_manual = rawPrediction[1] / "
            "(rawPrediction[0] + rawPrediction[1])` = "
            f"{summary['raw_prediction_1']:.17g} / "
            f"({summary['raw_prediction_0']:.17g} + "
            f"{summary['raw_prediction_1']:.17g}) = "
            f"{summary['probability_manual']:.17g}."
        )
        add("")
        add(
            f"probability_Spark = `{summary['probability_spark']:.17g}`; "
            f"absolute_difference = `{summary['absolute_difference']:.17g}`; "
            f"prediction_common = `{demo_by_alias[alias]['prediction_random_forest']}` "
            f"tại common threshold `{context['common_threshold']:.3f}`."
        )
        add("")
        add(md_table(
            ["Tree", "Leaf", "Raw[0]", "Raw[1]", "Tree probability[0]", "Tree probability[1]", "Weight", "Contribution[0]", "Contribution[1]"],
            [[x["tree_index"], x["leaf_index"], fmt(x["tree_raw_prediction_0"], 16), fmt(x["tree_raw_prediction_1"], 16), fmt(x["tree_probability_0"], 16), fmt(x["tree_probability_1"], 16), fmt(x["tree_weight"]), fmt(x["weighted_contribution_0"], 16), fmt(x["weighted_contribution_1"], 16)] for x in summary["per_tree"]],
        ))
        add("")
        add(
            "Tổng contribution theo treeWeights = "
            f"`[{summary['weighted_sum_0']:.17g}, "
            f"{summary['weighted_sum_1']:.17g}]`, khớp model rawPrediction."
        )

    add("")
    add("## PHẦN 8. CÁCH CHỌN COMMON THRESHOLD")
    add("")
    add("Coarse thử 0.01 đến 0.49 với bước 0.01. Refine lấy ±0.03 quanh candidate coarse tốt nhất, bước 0.001, và chặn candidate trong khoảng 0.001 đến 0.499. Cùng một danh sách threshold được áp dụng cho hai model.")
    add("")
    add("Candidate chỉ hợp lệ nếu alert rate của cả Logistic Regression và Random Forest không vượt 20%. Xếp hạng lần lượt: average_f1 cao nhất; minimum_f1 cao hơn; average_recall cao hơn; average_alert_rate thấp hơn; common threshold cao hơn.")
    add("")
    add("Baseline majority class không tham gia chọn common threshold và không tham gia chọn model demo; baseline chỉ là mốc so sánh bắt buộc trên cùng data split.")
    add("")
    add(f"`average_f1 = (F1_Logistic_Regression + F1_Random_Forest) / 2`. Tổng số candidate đã thử: `{context['candidate_count']}` ({len(context['coarse_rows'])} coarse + {len(context['refine_rows'])} refine).")
    add("")
    add("### Top 10 candidate hợp lệ")
    add("")
    add(md_table(
        ["Hạng", "Common threshold", "LR F1", "RF F1", "average_f1", "minimum_f1", "average_recall", "average_alert_rate"],
        [[i + 1, fmt(x["common_threshold"], 3), fmt(x["logistic_f1"]), fmt(x["random_forest_f1"]), fmt(x["average_f1"]), fmt(x["minimum_f1"]), fmt(x["average_recall"]), fmt(x["average_alert_rate"])] for i, x in enumerate(context["top_candidates"])],
    ))
    add("")
    add(f"Common threshold được khóa từ validation: **{context['common_threshold']:.3f}**. Test không tham gia chọn threshold.")
    add("")
    add(f"- Candidate liền trước `{context['previous_threshold_display']}`: {context['previous_reason']}")
    add(f"- Candidate liền sau `{context['next_threshold_display']}`: {context['next_reason']}")
    add("")
    add("### So sánh threshold thủ công")
    add("")
    add(
        "Bảng dưới đây tính lại metrics tại các threshold cấu hình trong "
        "`THRESHOLD_TEST_THU_CONG`. Bảng chỉ dùng validation; test không được "
        "dò lại ở các threshold khác sau khi common threshold đã khóa."
    )
    add("")
    add(md_table(
        ["Tập", "Model", "Threshold", "TP", "TN", "FP", "FN", "Precision", "Recall", "F1", "AUC", "Alert rate"],
        [[
            x["tap_du_lieu"], x["model"], fmt(x["threshold"], 3),
            x["tp"], x["tn"], x["fp"], x["fn"],
            pct(x["precision"]), pct(x["recall"]), pct(x["f1"]),
            fmt(x["auc"], 6), pct(x["alert_rate"]),
        ] for x in context["manual_threshold_rows"]],
    ))

    add("")
    add("## PHẦN 9. CONFUSION MATRIX TRÊN TEST")
    for ten_phuong_phap in TEN_PHUONG_PHAP:
        m = comparison_results[ten_phuong_phap]
        add("")
        add(f"### {ten_phuong_phap}")
        add("")
        add(md_table(
            ["Actual / Prediction", "Không cảnh báo", "Cảnh báo", "Tổng actual"],
            [
                ["not late", m["tn"], m["fp"], m["tn"] + m["fp"]],
                ["late", m["fn"], m["tp"], m["fn"] + m["tp"]],
                ["Tổng prediction", m["tn"] + m["fn"], m["fp"] + m["tp"], m["n"]],
            ],
        ))

    add("")
    add("## PHẦN 10. TOÀN BỘ CÔNG THỨC VÀ THAY SỐ TEST")
    for ten_phuong_phap in TEN_PHUONG_PHAP:
        m = comparison_results[ten_phuong_phap]
        tp, tn, fp, fn, n = m["tp"], m["tn"], m["fp"], m["fn"], m["n"]
        add("")
        add(f"### {ten_phuong_phap}")
        add("")
        formulas = [
            ("Accuracy", "(TP + TN) / N", f"({tp} + {tn}) / {n}", m["accuracy"]),
            ("Precision", "TP / (TP + FP)", f"{tp} / ({tp} + {fp})", m["precision"]),
            ("Recall", "TP / (TP + FN)", f"{tp} / ({tp} + {fn})", m["recall"]),
            ("Specificity", "TN / (TN + FP)", f"{tn} / ({tn} + {fp})", m["specificity"]),
            ("FPR", "FP / (FP + TN)", f"{fp} / ({fp} + {tn})", m["fpr"]),
            ("F1", "2TP / (2TP + FP + FN)", f"(2 × {tp}) / (2 × {tp} + {fp} + {fn})", m["f1"]),
            ("alert rate", "(TP + FP) / N", f"({tp} + {fp}) / {n}", m["alert_rate"]),
            ("prevalence", "(TP + FN) / N", f"({tp} + {fn}) / {n}", m["prevalence"]),
        ]
        for metric, formula, substitution, value in formulas:
            add(
                f"- {metric} = {formula} = {substitution} = "
                f"{fmt(value)} = {pct(value)}."
            )

    add("")
    add("## PHẦN 11. KIỂM CHỨNG AUC")
    add("")
    add("Mọi probability score được sắp giảm dần, các score bằng nhau được gộp thành một ROC point. Diện tích từng bước: `trapezoid_area_i = (FPR_i+1 - FPR_i) × (TPR_i+1 + TPR_i) / 2`. BinaryClassificationEvaluator dùng `numBins=0` để so với đầy đủ ROC points.")
    for ten_model, roc, traps, auc_s, auc_m, diff, csv_name in [
        (BASELINE_NAME, context["roc_baseline"], context["traps_baseline"], baseline_test["auc"], context["auc_manual_baseline"], context["auc_diff_baseline"], "05_auc_trapezoids_baseline.csv"),
        ("Logistic Regression", context["roc_lr"], context["traps_lr"], test["Logistic Regression"]["auc"], context["auc_manual_lr"], context["auc_diff_lr"], "05_auc_trapezoids_logistic_regression.csv"),
        ("Random Forest", context["roc_rf"], context["traps_rf"], test["Random Forest"]["auc"], context["auc_manual_rf"], context["auc_diff_rf"], "05_auc_trapezoids_random_forest.csv"),
    ]:
        add("")
        add(f"### {ten_model}")
        add("")
        add(f"AUC_Spark = `{auc_s:.17g}`; AUC_manual = `{auc_m:.17g}`; độ lệch = `{diff:.17g}`; tổng ROC points = `{len(roc)}`; tổng trapezoid = `{len(traps)}`.")
        add("")
        sample = traps[:10] + (traps[-10:] if len(traps) > 10 else [])
        add(md_table(
            ["Bước", "FPR_i", "TPR_i", "FPR_i+1", "TPR_i+1", "Trapezoid area"],
            [[x["trapezoid_index"], fmt(x["fpr_i"]), fmt(x["tpr_i"]), fmt(x["fpr_i_plus_1"]), fmt(x["tpr_i_plus_1"]), fmt(x["trapezoid_area"], 16)] for x in sample],
        ))
        add("")
        add(f"Toàn bộ bước nằm trong `outputs/tables/{csv_name}`.")

    add("")
    add("## PHẦN 12. SO SÁNH BASELINE VÀ HAI MODEL TRÊN CÙNG TEST SPLIT")
    add("")
    compare_rows = []
    for ten_phuong_phap in TEN_PHUONG_PHAP:
        m = comparison_results[ten_phuong_phap]
        compare_rows.append([
            ten_phuong_phap,
            "Không áp dụng" if ten_phuong_phap == BASELINE_NAME else fmt(context["common_threshold"], 3),
            m["tp"], m["tn"], m["fp"], m["fn"],
            pct(m["accuracy"]), pct(m["precision"]), pct(m["recall"]),
            pct(m["specificity"]), pct(m["fpr"]), pct(m["f1"]),
            fmt(m["auc"], 6), pct(m["alert_rate"]),
        ])
    add(md_table(
        ["Phương pháp", "Threshold", "TP", "TN", "FP", "FN", "Accuracy", "Precision", "Recall", "Specificity", "FPR", "F1", "AUC", "Alert rate"],
        compare_rows,
    ))
    add("")
    add(
        "Baseline có thể đạt Accuracy cao do class imbalance nhưng Recall và F1 "
        "bằng 0 khi không cảnh báo order late nào. Vì vậy không được kết "
        "luận phương pháp tốt hơn chỉ dựa trên Accuracy."
    )

    add("")
    add("## PHẦN 13. BỐN ORDER THẬT A, B, C, D")
    for dong in demo:
        add("")
        add(f"### {dong['alias']} — `{dong['order_id']}`")
        add("")
        add(f"Quy tắc: {dong['quy_tac']}. Thay thế: {dong['quy_tac_thay_the']}.")
        add("")
        add(md_table(
            ["Feature gốc", "Giá trị thật"],
            [[feature, dong[feature]] for feature in COT_PHAN_LOAI + COT_SO],
        ))
        add("")
        add(md_table(
            ["Label", "LR probability", "RF probability", "Common threshold", "LR prediction", "RF prediction", "LR result", "RF result", "Vị trí model demo"],
            [[dong["is_late"], fmt(dong["probability_logistic"], 16), fmt(dong["probability_random_forest"], 16), fmt(context["common_threshold"], 3), dong["prediction_logistic"], dong["prediction_random_forest"], dong["result_logistic"], dong["result_random_forest"], dong["vi_tri_so_voi_threshold"]]],
        ))
        add("")
        add(f"Phép tính Logistic Regression đầy đủ nằm ở PHẦN 6; phép tính Random Forest và 30 leaf index nằm ở PHẦN 7. Các bảng CSV breakdown chứa toàn bộ dòng máy đọc được.")

    add("")
    add("## PHẦN 14. MODEL ĐƯỢC CHỌN CHO DEMO")
    add("")
    add("Quy tắc tại common threshold trên validation: F1 cao hơn; nếu bằng nhau thì Recall cao hơn; tiếp theo Precision cao hơn; tiếp theo AUC cao hơn; cuối cùng dùng thứ tự tên cố định để bảo đảm tái lập.")
    add("")
    add(md_table(
        ["Model", "F1", "Recall", "Precision", "AUC", "Alert rate"],
        [[name, fmt(validation[name]["f1"]), fmt(validation[name]["recall"]), fmt(validation[name]["precision"]), fmt(validation[name]["auc"], 6), pct(validation[name]["alert_rate"])] for name in TEN_MODEL],
    ))
    add("")
    add(f"Model được khóa cho demo là **{context['demo_model']}**, common threshold **{context['common_threshold']:.3f}**. Quyết định này chỉ dùng validation, không dùng test. Demo chỉ đưa order vào nhóm ưu tiên kiểm tra; không khẳng định order chắc chắn giao trễ.")

    add("")
    add("## PHẦN 15. HẠN CHẾ")
    add("")
    add("- Dataset có class imbalance; prevalence của class late chỉ là " + pct(split["toan_bo"]["late_rate"]) + ".")
    add("- Precision có thể thấp khi cân bằng Precision và Recall bằng F1 dưới giới hạn alert rate.")
    add("- Dataset thiếu một số dữ liệu logistics vận hành có thể ảnh hưởng trực tiếp tới giao hàng.")
    add("- Probability score chưa chắc là calibrated probability vì quy trình chưa thực hiện probability calibration.")
    add("- Common threshold là quy ước so sánh chung cho hai model, không phải threshold tối ưu tuyệt đối cho mọi dataset.")
    add("- Kết quả quan sát không chứng minh quan hệ nhân quả.")
    add("- Không dùng model để tự động thông báo khách hàng; score chỉ hỗ trợ ưu tiên kiểm tra nội bộ.")

    add("")
    add("## PHẦN 16. KẾT LUẬN")
    add("")
    add(
        f"Baseline majority class dự đoán cố định label "
        f"`{context['baseline_label_test']}` trên test, đạt Accuracy "
        f"`{baseline_test['accuracy']:.6f}` nhưng Recall "
        f"`{baseline_test['recall']:.6f}`, F1 `{baseline_test['f1']:.6f}` và "
        f"AUC `{baseline_test['auc']:.6f}`. Lần chạy này khóa common threshold "
        f"`{context['common_threshold']:.3f}` từ validation. Logistic Regression "
        f"đạt F1 `{test['Logistic Regression']['f1']:.6f}`, Recall "
        f"`{test['Logistic Regression']['recall']:.6f}`, Precision "
        f"`{test['Logistic Regression']['precision']:.6f}`, AUC "
        f"`{test['Logistic Regression']['auc']:.6f}`; Random Forest đạt F1 "
        f"`{test['Random Forest']['f1']:.6f}`, Recall "
        f"`{test['Random Forest']['recall']:.6f}`, Precision "
        f"`{test['Random Forest']['precision']:.6f}`, AUC "
        f"`{test['Random Forest']['auc']:.6f}`. Hai model có Recall/F1/AUC cao "
        "hơn baseline, dù Accuracy có thể thấp hơn do đã phát sinh cảnh báo. "
        "Các nhận định chỉ áp dụng cho data split, feature, cấu hình và quy tắc "
        "threshold được ghi trong báo cáo này."
    )

    add("")
    add("# PHỤ LỤC")
    add("")
    add("## A. Toàn bộ Logistic Regression coefficients")
    add("")
    add(md_table(
        ["Index", "Transformed feature", "Coefficient"],
        [[x["feature_index"], x["transformed_feature_name"], fmt(x["coefficient"], 16)] for x in context["coefficient_rows"]],
    ))
    add("")
    add("## B. Toàn bộ Random Forest feature importance")
    add("")
    add(md_table(
        ["Index", "Transformed feature", "Feature importance"],
        [[x["feature_index"], x["transformed_feature_name"], fmt(x["feature_importance"], 16)] for x in context["importance_rows"]],
    ))
    add("")
    add("## C. Tree details và model.toDebugString")
    add("")
    add(md_table(
        ["Tree", "Weight", "Depth", "Num nodes"],
        [[x["tree_index"], fmt(x["tree_weight"]), x["depth"], x["num_nodes"]] for x in context["tree_rows"]],
    ))
    add("")
    add("```text")
    add(context["rf_debug_string"])
    add("```")
    add("")
    add("## D. Line number code")
    add("")
    add(md_table(
        ["Marker", "Dòng", "Code", "Tính năng"],
        [[x["marker"], x["dong_code_thuc_te"], f"`{x['code_thuc_te']}`", x["tinh_nang"]] for x in context["code_rows"]],
    ))
    add("")
    add("## E. Assertion kiểm tra")
    add("")
    add(md_table(
        ["Kiểm tra", "Trạng thái", "Chi tiết"],
        [[x["kiem_tra"], x["trang_thai"], x["chi_tiet"]] for x in CAC_ASSERTION],
    ))
    add("")
    add("## F. Kiểm tra chạy lặp")
    add("")
    add(f"Trạng thái: **{context['repro_status']}**. Previous signature: `{context['previous_signature']}`. Current signature: `{context['current_signature']}`. Signature bao gồm common threshold, data split, validation/test confusion matrix và metrics, cùng order A/B/C/D.")
    add("")
    add("## G. Danh sách output")
    add("")
    for path in context["all_outputs"]:
        add(f"- `{path.relative_to(PROJECT_DIR)}`")

    REPORT_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def tao_prompt_cap_nhat_tai_lieu(context):
    """Tạo prompt tự chứa số liệu thật để cập nhật slide và tiểu luận."""
    split = {x["tap_du_lieu"]: x for x in context["split_rows"]}
    comparison = context["comparison_results"]
    validation = context["validation_results"]
    lines = [
        "# PROMPT CẬP NHẬT SLIDE VÀ TIỂU LUẬN OLIST",
        "",
        "## Cách sử dụng",
        "",
        "Đính kèm cho ChatGPT: slide hiện tại, file tiểu luận hiện tại, báo cáo `bao_cao_kiem_chung_05_F1.md` và các CSV bước 05 cần đối chiếu. Sau đó sao chép toàn bộ prompt bên dưới.",
        "",
        "---",
        "",
        "Bạn là biên tập viên học thuật và chuyên gia PySpark/Machine Learning. Hãy đọc toàn bộ slide, tiểu luận và báo cáo kiểm chứng tôi đính kèm trước khi sửa. Nhiệm vụ là cập nhật nội dung để khớp tuyệt đối với code và kết quả chạy thật mới nhất của project Olist.",
        "",
        "## Hai yêu cầu gốc phải thể hiện rõ",
        "",
        "1. Tạo tập dữ liệu học máy hợp lệ để dự đoán `is_late` tại thời điểm đặt hàng.",
        "2. Huấn luyện ít nhất hai mô hình phân lớp đơn giản và so sánh với baseline.",
        "",
        "## Sự thật kỹ thuật bắt buộc dùng",
        "",
        f"- Dataset có `{split['toan_bo']['so_dong']:,}` order và `{split['toan_bo']['so_don_late']:,}` order late; prevalence `{pct(split['toan_bo']['late_rate'])}`.",
        f"- Split: train_full `{split['train_full']['so_dong']:,}`, train_fit `{split['train_fit']['so_dong']:,}`, validation `{split['validation']['so_dong']:,}`, test `{split['test']['so_dong']:,}`; seed `{SEED}`.",
        f"- Feature tại thời điểm đặt hàng: `{len(COT_PHAN_LOAI)}` categorical feature và `{len(COT_SO)}` numeric feature, tổng `{len(COT_PHAN_LOAI) + len(COT_SO)}` feature gốc.",
        f"- Categorical feature: `{', '.join(COT_PHAN_LOAI)}`.",
        f"- Numeric feature: `{', '.join(COT_SO)}`.",
        f"- Các cột bị loại vì feature leakage hoặc chưa chứng minh có sẵn lúc đặt hàng: `{', '.join(sorted(COT_LEAKAGE_CAM))}`.",
        f"- Baseline là majority class xác định chỉ từ train_full và dự đoán cố định label `{context['baseline_label_test']}` trên test. Baseline không dùng common threshold.",
        f"- Logistic Regression: `maxIter={LR_MAX_ITER}`, `regParam={LR_REG_PARAM}`.",
        f"- Random Forest: `numTrees={RF_NUM_TREES}`, `maxDepth={RF_MAX_DEPTH}`, `seed={SEED}`.",
        f"- Common threshold `{context['common_threshold']:.3f}` được tự động chọn từ validation; test không tham gia chọn threshold hoặc model demo.",
        f"- Model demo được chọn từ validation: `{context['demo_model']}`.",
        "- `AUC = 0.5` của baseline là mức tham chiếu của score hằng/khả năng xếp hạng ngẫu nhiên; tuyệt đối không gọi 0.5 là common threshold của hai model.",
        "- Không sử dụng threshold mặc định cũ làm kết quả nghiên cứu và không đưa threshold đó vào bảng/demo.",
        "",
        "## Kết quả validation tại common threshold",
        "",
        "| Model | TP | TN | FP | FN | Precision | Recall | F1 | AUC | Alert rate |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name in TEN_MODEL:
        m = validation[name]
        lines.append(
            f"| {name} | {m['tp']} | {m['tn']} | {m['fp']} | {m['fn']} | "
            f"{m['precision']:.6f} | {m['recall']:.6f} | {m['f1']:.6f} | "
            f"{m['auc']:.6f} | {m['alert_rate']:.6f} |"
        )
    lines.extend([
        "",
        "## So sánh trên cùng test split",
        "",
        "| Phương pháp | Threshold | TP | TN | FP | FN | Accuracy | Precision | Recall | F1 | AUC | Alert rate |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for name in TEN_PHUONG_PHAP:
        m = comparison[name]
        threshold = (
            "Không áp dụng"
            if name == BASELINE_NAME
            else f"{context['common_threshold']:.3f}"
        )
        lines.append(
            f"| {name} | {threshold} | {m['tp']} | {m['tn']} | {m['fp']} | "
            f"{m['fn']} | {m['accuracy']:.6f} | {m['precision']:.6f} | "
            f"{m['recall']:.6f} | {m['f1']:.6f} | "
            f"{m['auc']:.6f} | {m['alert_rate']:.6f} |"
        )
    lines.extend([
        "",
        "## Yêu cầu sửa slide",
        "",
        "1. Lập bảng kiểm kê từng slide: số slide, nội dung cũ cần sửa/xóa, nội dung mới, nguồn số liệu trong báo cáo/CSV.",
        "2. Viết lại nội dung từng slide bằng câu ngắn, đủ lớn để trình chiếu; không dồn toàn bộ báo cáo lên slide.",
        "3. Bắt buộc có các slide: bài toán và thời điểm dự đoán; dữ liệu/label/feature leakage; data split; preprocessing; baseline; hai model; cách chọn common threshold trên validation; so sánh test; giải thích demo; hạn chế và kết luận.",
        "4. Trên slide so sánh, phải có baseline cùng Logistic Regression và Random Forest. Giải thích vì sao baseline có Accuracy cao nhưng Recall/F1 bằng 0; không kết luận bằng Accuracy đơn lẻ.",
        "5. Trên ROC curve, ghi rõ đường AUC 0.5 là baseline tham chiếu, không phải threshold.",
        "6. Mọi sơ đồ dùng nền trắng, chữ đen, khung đen, chữ lớn; mũi tên không cắt ngang ô dữ liệu.",
        "",
        "## Yêu cầu sửa tiểu luận",
        "",
        "1. Lập danh sách chương/mục/đoạn/bảng/hình cần sửa trước khi viết lại.",
        "2. Viết lại phần phương pháp để chứng minh feature chỉ dùng thông tin có thể biết tại thời điểm đặt hàng; label `is_late` là kết quả tương lai dùng cho supervised learning, không phải feature.",
        "3. Mô tả baseline majority class là mốc bắt buộc, không phải model được tối ưu và không dùng common threshold.",
        "4. Mô tả Logistic Regression và Random Forest là hai mô hình phân lớp đơn giản được so sánh trên cùng test split.",
        "5. Trình bày rõ common threshold chỉ được chọn trên validation, sau đó khóa trước khi đánh giá test.",
        "6. Thay toàn bộ số liệu cũ bằng số liệu trong prompt/báo cáo mới. Không giữ bảng hoặc kết luận dùng threshold mặc định cũ.",
        "7. Kết luận phải dựa trên Precision, Recall, F1, AUC, alert rate và confusion matrix; giải thích class imbalance khiến Accuracy dễ gây hiểu lầm.",
        "8. Không tuyên bố probability score đã calibrated nếu chưa probability calibration; không khẳng định quan hệ nhân quả; không dùng model để tự động thông báo khách hàng.",
        "",
        "## Quy tắc thuật ngữ và tính trung thực",
        "",
        "- Giữ nguyên các thuật ngữ: Logistic Regression, Random Forest, baseline, threshold, common threshold, probability, feature, label, Pipeline, train, train_fit, validation, test, confusion matrix, Accuracy, Precision, Recall, Specificity, F1, AUC, ROC curve, alert rate, StringIndexer, OneHotEncoder, Imputer, VectorAssembler.",
        "- Không dùng cụm từ “rừng ngẫu nhiên”.",
        "- Không tự tạo số liệu, không làm tròn thành số khác gây sai kết quả, không lấy số từ bản luận văn hoặc output cũ.",
        "- Nếu nội dung trong slide/tiểu luận mâu thuẫn với báo cáo mới, ưu tiên báo cáo mới và nêu rõ vị trí đã thay thế.",
        "- Nếu thiếu file hoặc không đọc được một bảng/hình, hãy yêu cầu tôi đính kèm thay vì suy đoán.",
        "",
        "## Đầu ra ChatGPT phải trả",
        "",
        "1. Bảng kiểm kê thay đổi slide.",
        "2. Nội dung slide đã viết lại theo từng slide.",
        "3. Bảng kiểm kê thay đổi tiểu luận.",
        "4. Các đoạn/bảng/kết luận đã viết lại để tôi chèn trực tiếp.",
        "5. Checklist cuối xác nhận: feature tại thời điểm đặt hàng; không leakage; có baseline; có hai model; threshold chọn từ validation; test chỉ đánh giá; mọi số liệu khớp báo cáo.",
        "",
        "---",
        "",
        "Prompt này được tạo tự động từ lần chạy bước 05; không thay các số trên bằng dữ liệu ước lượng.",
    ])
    PROMPT_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    """Điều phối toàn bộ bước 05 từ kiểm tra data đến xuất báo cáo.

    Thứ tự thực hiện: đọc/kiểm tra -> split -> fit preprocessing trên train_fit
    -> fit hai model -> chọn common threshold và model demo trên validation ->
    retrain trên train_full -> đánh giá test -> kiểm chứng AUC/score -> xuất CSV,
    PNG, báo cáo và chữ ký reproducibility.
    """
    thoi_gian_bat_dau = datetime.now().astimezone()
    perf_bat_dau = time.perf_counter()
    if not DATA_FILE.is_file():
        raise FileNotFoundError(f"Không tìm thấy dataset: {DATA_FILE}")
    dataset_stat_truoc = DATA_FILE.stat()
    dataset_hash_truoc = bam_sha256(DATA_FILE)
    previous_signature_row = doc_mot_dong_csv(
        TABLE_DIR / "05_reproducibility_signature.csv"
    )
    previous_signature_json = (
        previous_signature_row.get("signature_json")
        if previous_signature_row
        else None
    )
    previous_signature_hash = (
        previous_signature_row.get("signature_sha256")
        if previous_signature_row
        else "không có"
    )
    try:
        previous_signature_data = (
            json.loads(previous_signature_json)
            if previous_signature_json
            else None
        )
    except json.JSONDecodeError:
        previous_signature_data = None
    previous_signature_version = (
        previous_signature_data.get("signature_version")
        if isinstance(previous_signature_data, dict)
        else None
    )

    spark = (
        SparkSession.builder
        .appName("Olist_05_Huan_Luyen_Danh_Gia_Kiem_Chung")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR.as_uri())
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    java_version = str(
        spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
    )

    try:
        # CODE_REF: READ_DATASET
        data = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(DATA_FILE.as_uri())
        ).cache()

        danh_sach_feature = COT_PHAN_LOAI + COT_SO
        kiem_tra_feature(danh_sach_feature, danh_sach_feature)
        cot_can_co = {"order_id", "is_late", *danh_sach_feature}
        cot_thieu = sorted(cot_can_co - set(data.columns))
        xac_nhan(
            "Dataset có đủ order_id, label và feature",
            not cot_thieu,
            f"cột thiếu={cot_thieu}",
        )
        so_order_id_null = data.filter(F.col("order_id").isNull()).count()
        so_label_null = data.filter(F.col("is_late").isNull()).count()
        so_label_khong_hop_le = data.filter(
            ~F.col("is_late").isin([0, 1])
        ).count()
        xac_nhan(
            "order_id không null",
            so_order_id_null == 0,
            f"order_id null={so_order_id_null}",
        )
        xac_nhan(
            "Label không null",
            so_label_null == 0,
            f"label null={so_label_null}",
        )
        xac_nhan(
            "Label chỉ nhận 0 hoặc 1",
            so_label_khong_hop_le == 0,
            f"label ngoài 0/1={so_label_khong_hop_le}",
        )
        kiem_tra_nan_vo_cuc(data, COT_SO)

        # CODE_REF: LABEL_INPUT
        data_model = data.select(
            "order_id",
            *COT_PHAN_LOAI,
            *COT_SO,
            F.col("is_late").cast("double").alias("is_late"),
        )
        for ten_cot in COT_PHAN_LOAI:
            data_model = data_model.withColumn(
                ten_cot,
                F.when(
                    F.col(ten_cot).isNull()
                    | (F.trim(F.col(ten_cot)) == ""),
                    F.lit("khong_xac_dinh"),
                ).otherwise(F.trim(F.col(ten_cot))),
            )
        data_model = data_model.orderBy("order_id").cache()
        so_dong_model = data_model.count()
        so_order_id = data_model.select("order_id").distinct().count()
        xac_nhan(
            "Số dòng bằng số order_id khác nhau",
            so_dong_model == so_order_id,
            f"dòng={so_dong_model}, order_id khác nhau={so_order_id}",
        )

        # CODE_REF: SPLIT_TRAIN_TEST
        train_full, test = data_model.randomSplit([0.8, 0.2], seed=SEED)
        train_full = train_full.orderBy("order_id").cache()
        test = test.orderBy("order_id").cache()
        so_train_full = train_full.count()
        so_test = test.count()
        # CODE_REF: SPLIT_TRAIN_VALIDATION
        train_fit, validation = train_full.randomSplit(
            [0.8, 0.2],
            seed=SEED,
        )
        train_fit = train_fit.orderBy("order_id").cache()
        validation = validation.orderBy("order_id").cache()
        so_train_fit = train_fit.count()
        so_validation = validation.count()
        xac_nhan(
            "train_full + test bằng toàn bộ data",
            so_train_full + so_test == so_dong_model,
            f"{so_train_full}+{so_test}={so_dong_model}",
        )
        xac_nhan(
            "train_fit + validation bằng train_full",
            so_train_fit + so_validation == so_train_full,
            f"{so_train_fit}+{so_validation}={so_train_full}",
        )
        overlap_full_test = (
            train_full.select("order_id")
            .join(test.select("order_id"), "order_id", "inner")
            .count()
        )
        overlap_fit_validation = (
            train_fit.select("order_id")
            .join(validation.select("order_id"), "order_id", "inner")
            .count()
        )
        xac_nhan(
            "train_full và test không giao order_id",
            overlap_full_test == 0,
            f"giao nhau={overlap_full_test}",
        )
        xac_nhan(
            "train_fit và validation không giao order_id",
            overlap_fit_validation == 0,
            f"giao nhau={overlap_fit_validation}",
        )
        split_rows = [
            thong_ke_tap("toan_bo", data_model),
            thong_ke_tap("train_full", train_full),
            thong_ke_tap("train_fit", train_fit),
            thong_ke_tap("validation", validation),
            thong_ke_tap("test", test),
        ]
        for row in split_rows:
            row["kiem_tra_giao_nhau"] = "không áp dụng"
        split_rows[1]["kiem_tra_giao_nhau"] = (
            f"với test: {overlap_full_test}"
        )
        split_rows[2]["kiem_tra_giao_nhau"] = (
            f"với validation: {overlap_fit_validation}"
        )
        split_rows[3]["kiem_tra_giao_nhau"] = (
            f"với train_fit: {overlap_fit_validation}"
        )
        split_rows[4]["kiem_tra_giao_nhau"] = (
            f"với train_full: {overlap_full_test}"
        )

        # Baseline validation chỉ học majority class từ train_fit.
        baseline_label_validation, baseline_counts_validation = (
            xac_dinh_label_da_so(train_fit, "train_fit")
        )
        pred_baseline_validation = tao_du_doan_baseline(
            validation,
            baseline_label_validation,
        ).cache()
        auc_baseline_validation = auc_spark(pred_baseline_validation)
        baseline_validation = danh_gia_dataframe(
            BASELINE_NAME,
            pred_baseline_validation,
            so_validation,
            auc_baseline_validation,
        )
        xac_nhan(
            "AUC baseline validation bằng 0.5",
            abs(auc_baseline_validation - 0.5) <= 1e-12,
            f"AUC={auc_baseline_validation:.17g}",
        )

        # Pipeline phục vụ lựa chọn common threshold chỉ fit trên train_fit.
        pipeline_train_fit = tao_pipeline_tien_xu_ly().fit(train_fit)
        train_fit_transformed = pipeline_train_fit.transform(train_fit).cache()
        validation_transformed = pipeline_train_fit.transform(validation).cache()
        train_fit_ready = train_fit_transformed.select(
            "order_id", "is_late", "features"
        ).cache()
        validation_ready = validation_transformed.select(
            "order_id", "is_late", "features"
        ).cache()
        selection_pre, selection_mapping, selection_metadata, _ = (
            chi_tiet_preprocessing(
                "train_fit",
                pipeline_train_fit,
                train_fit,
                train_fit_transformed,
                so_train_fit,
            )
        )
        xac_nhan(
            "Validation preprocessing giữ nguyên số dòng",
            validation_ready.count() == so_validation,
            f"trước={so_validation}, sau={validation_ready.count()}",
        )

        model_lr_selection, model_rf_selection = fit_hai_model(train_fit_ready)
        pred_lr_validation = tao_probability(
            model_lr_selection,
            validation_ready,
        ).cache()
        pred_rf_validation = tao_probability(
            model_rf_selection,
            validation_ready,
        ).cache()
        auc_lr_validation = auc_spark(pred_lr_validation)
        auc_rf_validation = auc_spark(pred_rf_validation)
        validation_joined = (
            pred_lr_validation.select(
                "order_id",
                "is_late",
                F.col("probability_late").alias("probability_logistic"),
            )
            .join(
                pred_rf_validation.select(
                    "order_id",
                    F.col("probability_late").alias(
                        "probability_random_forest"
                    ),
                ),
                "order_id",
                "inner",
            )
        )
        validation_python = [x.asDict() for x in validation_joined.collect()]
        xac_nhan(
            "Hai model có đủ probability trên validation",
            len(validation_python) == so_validation,
            f"join={len(validation_python)}, validation={so_validation}",
        )

        coarse_rows, refine_rows, best_coarse, best_refine = (
            tim_common_threshold(
                validation_python,
                auc_lr_validation,
                auc_rf_validation,
            )
        )

        if MANUAL_COMMON_THRESHOLD is None:
            chosen_candidate = best_refine
            threshold_mode = "AUTO_VALIDATION"
        else:
            manual_threshold = float(MANUAL_COMMON_THRESHOLD)
            xac_nhan(
                "MANUAL_COMMON_THRESHOLD nằm trong [0, 1]",
                0.0 <= manual_threshold <= 1.0,
                f"MANUAL_COMMON_THRESHOLD={manual_threshold}",
            )
            chosen_candidate = danh_gia_common_threshold(
                validation_python,
                manual_threshold,
                "manual",
                auc_lr_validation,
                auc_rf_validation,
            )
            threshold_mode = "MANUAL_OVERRIDE"

        xac_nhan(
            "Lần chạy official tự động chọn common threshold bằng F1 trên validation",
            RUN_TAG.strip().lower() != "official"
            or (
                MANUAL_COMMON_THRESHOLD is None
                and threshold_mode == "AUTO_VALIDATION"
            ),
            f"run_tag={RUN_TAG}, threshold_mode={threshold_mode}",
        )

        common_threshold = float(chosen_candidate["common_threshold"])
        all_candidate_rows = coarse_rows + refine_rows
        if not any(
            abs(float(x["common_threshold"]) - common_threshold) <= 1e-12
            for x in all_candidate_rows
        ):
            all_candidate_rows.append(chosen_candidate)
        top_candidates = sorted(
            [x for x in refine_rows if x["hop_le_alert_rate"]],
            key=khoa_xep_hang_threshold,
            reverse=True,
        )[:10]
        ranked_candidates = []
        for rank, row in enumerate(
            sorted(
                [x for x in all_candidate_rows if x["hop_le_alert_rate"]],
                key=khoa_xep_hang_threshold,
                reverse=True,
            ),
            start=1,
        ):
            ranked_candidates.append({"xep_hang": rank, **row})
        candidate_by_threshold = {
            x["common_threshold"]: x for x in refine_rows
        }
        previous_candidate = candidate_by_threshold.get(
            round(common_threshold - 0.001, 3)
        )
        next_candidate = candidate_by_threshold.get(
            round(common_threshold + 0.001, 3)
        )

        thresholds_so_sanh_thu_cong = sorted({
            *THRESHOLD_TEST_THU_CONG,
            common_threshold,
        })
        manual_threshold_rows_validation = (
            tao_bang_so_sanh_threshold_thu_cong(
                "validation",
                validation_python,
                thresholds_so_sanh_thu_cong,
                auc_lr_validation,
                auc_rf_validation,
            )
        )

        pred_lr_validation_common = them_prediction_common(
            pred_lr_validation,
            common_threshold,
        ).cache()
        pred_rf_validation_common = them_prediction_common(
            pred_rf_validation,
            common_threshold,
        ).cache()
        validation_results = {
            "Logistic Regression": danh_gia_dataframe(
                "Logistic Regression",
                pred_lr_validation_common,
                so_validation,
                auc_lr_validation,
            ),
            "Random Forest": danh_gia_dataframe(
                "Random Forest",
                pred_rf_validation_common,
                so_validation,
                auc_rf_validation,
            ),
        }
        for name, prefix in [
            ("Logistic Regression", "logistic"),
            ("Random Forest", "random_forest"),
        ]:
            xac_nhan(
                f"Validation {name} khớp bảng candidate được chọn",
                all(
                    abs(
                        float(validation_results[name][metric])
                        - float(chosen_candidate[f"{prefix}_{metric}"])
                    )
                    <= 1e-12
                    for metric in [
                        "tp", "tn", "fp", "fn", "accuracy", "precision",
                        "recall", "specificity", "fpr", "f1",
                        "alert_rate",
                    ]
                ),
                f"model={name}",
            )

        def model_demo_key(name):
            m = validation_results[name]
            return (
                m["f1"],
                m["recall"],
                m["precision"],
                m["auc"],
                -TEN_MODEL.index(name),
            )

        demo_model = max(TEN_MODEL, key=model_demo_key)
        xac_nhan(
            "Common threshold chỉ được chọn từ validation",
            chosen_candidate["giai_do"] in {"coarse", "refine"}
            and threshold_mode == "AUTO_VALIDATION",
            f"threshold={common_threshold}, mode={threshold_mode}",
        )
        xac_nhan(
            "Model demo chỉ được chọn từ metrics validation",
            demo_model in validation_results,
            f"model={demo_model}, tie-break=F1/Recall/Precision/AUC/tên cố định",
        )

        # Baseline test chỉ học majority class từ train_full, không nhìn test.
        baseline_label_test, baseline_counts_test = xac_dinh_label_da_so(
            train_full,
            "train_full",
        )
        pred_baseline_test = tao_du_doan_baseline(
            test,
            baseline_label_test,
        ).cache()
        auc_baseline_test = auc_spark(pred_baseline_test)
        baseline_test = danh_gia_dataframe(
            BASELINE_NAME,
            pred_baseline_test,
            so_test,
            auc_baseline_test,
        )
        xac_nhan(
            "AUC baseline test bằng 0.5",
            abs(auc_baseline_test - 0.5) <= 1e-12,
            f"AUC={auc_baseline_test:.17g}",
        )

        # Retrain final preprocessing và hai model chỉ sau khi đã khóa quyết định.
        pipeline_final = tao_pipeline_tien_xu_ly().fit(train_full)
        train_full_transformed = pipeline_final.transform(train_full).cache()
        test_transformed = pipeline_final.transform(test).cache()
        train_full_ready = train_full_transformed.select(
            "order_id", "is_late", "features"
        ).cache()
        test_ready = test_transformed.select(
            "order_id", "is_late", "features"
        ).cache()
        final_pre, final_mapping, final_metadata, feature_names_final = (
            chi_tiet_preprocessing(
                "train_full",
                pipeline_final,
                train_full,
                train_full_transformed,
                so_train_full,
            )
        )
        xac_nhan(
            "Test preprocessing giữ nguyên số dòng",
            test_ready.count() == so_test,
            f"trước={so_test}, sau={test_ready.count()}",
        )
        model_lr_final, model_rf_final = fit_hai_model(train_full_ready)
        pred_lr_test_probability = tao_probability(
            model_lr_final,
            test_ready,
        ).cache()
        pred_rf_test_probability = tao_probability(
            model_rf_final,
            test_ready,
        ).cache()
        auc_lr_test = auc_spark(pred_lr_test_probability)
        auc_rf_test = auc_spark(pred_rf_test_probability)
        pred_lr_test = them_prediction_common(
            pred_lr_test_probability,
            common_threshold,
        ).cache()
        pred_rf_test = them_prediction_common(
            pred_rf_test_probability,
            common_threshold,
        ).cache()
        test_results = {
            "Logistic Regression": danh_gia_dataframe(
                "Logistic Regression",
                pred_lr_test,
                so_test,
                auc_lr_test,
            ),
            "Random Forest": danh_gia_dataframe(
                "Random Forest",
                pred_rf_test,
                so_test,
                auc_rf_test,
            ),
        }
        comparison_results = {
            BASELINE_NAME: baseline_test,
            **test_results,
        }

        test_joined_probability = (
            pred_lr_test_probability.select(
                "order_id",
                "is_late",
                F.col("probability_late").alias("probability_logistic"),
            )
            .join(
                pred_rf_test_probability.select(
                    "order_id",
                    F.col("probability_late").alias(
                        "probability_random_forest"
                    ),
                ),
                "order_id",
                "inner",
            )
        )
        test_python = [x.asDict() for x in test_joined_probability.collect()]
        xac_nhan(
            "Hai model có đủ probability trên test",
            len(test_python) == so_test,
            f"join={len(test_python)}, test={so_test}",
        )
        manual_threshold_rows = manual_threshold_rows_validation
        ghi_csv(
            TABLE_DIR / "05_so_sanh_threshold_thu_cong.csv",
            manual_threshold_rows,
            [
                "tap_du_lieu", "model", "threshold",
                "tp", "tn", "fp", "fn", "n",
                "accuracy", "precision", "recall", "specificity",
                "fpr", "f1", "f1_direct",
                "auc", "alert_rate", "prevalence",
            ],
        )

        roc_lr, traps_lr, auc_manual_lr, auc_diff_lr = (
            tinh_roc_auc_thu_cong(
                "Logistic Regression",
                pred_lr_test_probability,
                auc_lr_test,
            )
        )
        roc_rf, traps_rf, auc_manual_rf, auc_diff_rf = (
            tinh_roc_auc_thu_cong(
                "Random Forest",
                pred_rf_test_probability,
                auc_rf_test,
            )
        )
        (
            roc_baseline,
            traps_baseline,
            auc_manual_baseline,
            auc_diff_baseline,
        ) = tinh_roc_auc_thu_cong(
            BASELINE_NAME,
            pred_baseline_test,
            auc_baseline_test,
        )

        df_demo_chon = (
            pred_lr_test if demo_model == "Logistic Regression"
            else pred_rf_test
        )
        demo_choices = chon_bon_order(df_demo_chon, common_threshold)
        demo_ids = [x["order_id"] for x in demo_choices]
        choice_by_id = {x["order_id"]: x for x in demo_choices}
        raw_test = test.select(
            "order_id", *COT_PHAN_LOAI, *COT_SO, "is_late"
        )
        demo_joined = (
            raw_test.filter(F.col("order_id").isin(demo_ids))
            .join(
                pred_lr_test.select(
                    "order_id",
                    F.col("features").alias("features_logistic"),
                    F.col("rawPrediction").alias("raw_logistic"),
                    F.col("probability_late").alias(
                        "probability_logistic"
                    ),
                    F.col("prediction_common").alias(
                        "prediction_logistic"
                    ),
                ),
                "order_id",
                "inner",
            )
            .join(
                pred_rf_test.select(
                    "order_id",
                    F.col("features").alias("features_random_forest"),
                    F.col("rawPrediction").alias("raw_random_forest"),
                    F.col("probability_late").alias(
                        "probability_random_forest"
                    ),
                    F.col("prediction_common").alias(
                        "prediction_random_forest"
                    ),
                    "leaf_indices",
                ),
                "order_id",
                "inner",
            )
        )
        demo_rows = []
        for row in demo_joined.collect():
            dong = row.asDict(recursive=True)
            dong.update(choice_by_id[dong["order_id"]])
            dong["is_late"] = int(dong["is_late"])
            dong["prediction_logistic"] = int(dong["prediction_logistic"])
            dong["prediction_random_forest"] = int(
                dong["prediction_random_forest"]
            )
            dong["probability_logistic"] = float(
                dong["probability_logistic"]
            )
            dong["probability_random_forest"] = float(
                dong["probability_random_forest"]
            )
            dong["result_logistic"] = loai_ket_qua(
                dong["is_late"], dong["prediction_logistic"]
            )
            dong["result_random_forest"] = loai_ket_qua(
                dong["is_late"], dong["prediction_random_forest"]
            )
            demo_probability = (
                dong["probability_logistic"]
                if demo_model == "Logistic Regression"
                else dong["probability_random_forest"]
            )
            dong["vi_tri_so_voi_threshold"] = (
                "bằng hoặc cao hơn common threshold"
                if demo_probability >= common_threshold
                else "thấp hơn common threshold"
            )
            demo_rows.append(dong)
        demo_rows.sort(key=lambda x: x["alias"])
        xac_nhan(
            "Thu được đúng bốn order demo khác nhau",
            len(demo_rows) == 4
            and len({x["order_id"] for x in demo_rows}) == 4,
            f"order={[(x['alias'], x['order_id']) for x in demo_rows]}",
        )

        (
            coefficient_rows,
            lr_breakdown_rows,
            lr_summaries,
        ) = phan_tich_logistic(
            model_lr_final,
            feature_names_final,
            demo_rows,
        )
        (
            importance_rows,
            tree_rows,
            rf_breakdown_rows,
            rf_summaries,
        ) = phan_tich_random_forest(
            model_rf_final,
            feature_names_final,
            demo_rows,
            common_threshold,
        )

        preprocessing_rows = selection_pre + final_pre
        mapping_rows = selection_mapping + final_mapping
        metadata_rows = selection_metadata + final_metadata

        # Ghi các bảng bằng chứng từ kết quả vừa tính.
        split_fields = [
            "tap_du_lieu", "so_dong", "so_order_id_khac_nhau",
            "so_don_late", "so_don_not_late", "late_rate",
            "kiem_tra_giao_nhau",
        ]
        ghi_csv(TABLE_DIR / "05_thong_tin_chia_du_lieu.csv", split_rows, split_fields)
        ghi_csv(
            TABLE_DIR / "05_preprocessing_details.csv",
            preprocessing_rows,
            ["pham_vi_fit", "component", "feature_goc", "chi_tiet", "gia_tri"],
        )
        ghi_csv(
            TABLE_DIR / "05_string_indexer_mapping.csv",
            mapping_rows,
            ["pham_vi_fit", "feature_goc", "string_index", "category", "la_nhom_invalid"],
        )
        ghi_csv(
            TABLE_DIR / "05_feature_vector_metadata.csv",
            metadata_rows,
            ["pham_vi_fit", "feature_index", "transformed_feature_name", "feature_goc", "metadata_attribute_type"],
        )
        candidate_fields = [
            "giai_do", "common_threshold", "hop_le_alert_rate",
            "logistic_tp", "logistic_tn", "logistic_fp", "logistic_fn",
            "logistic_accuracy", "logistic_precision", "logistic_recall",
            "logistic_specificity", "logistic_fpr", "logistic_f1",
            "logistic_auc", "logistic_alert_rate",
            "random_forest_tp", "random_forest_tn", "random_forest_fp",
            "random_forest_fn", "random_forest_accuracy",
            "random_forest_precision", "random_forest_recall",
            "random_forest_specificity", "random_forest_fpr",
            "random_forest_f1", "random_forest_auc",
            "random_forest_alert_rate", "average_f1", "minimum_f1",
            "average_recall", "average_alert_rate",
        ]
        ghi_csv(
            TABLE_DIR / "05_validation_common_thresholds.csv",
            all_candidate_rows,
            candidate_fields,
        )
        ghi_csv(
            TABLE_DIR / "05_xep_hang_common_threshold_F1.csv",
            ranked_candidates,
            ["xep_hang", *candidate_fields],
        )
        selected_rows = []
        for name, prefix in [
            ("Logistic Regression", "logistic"),
            ("Random Forest", "random_forest"),
        ]:
            selected_rows.append({
                "model": name,
                "common_threshold": common_threshold,
                "selected_demo_model": demo_model,
                "selection_dataset": "validation",
                "coarse_best_threshold": best_coarse["common_threshold"],
                "candidate_count": len(all_candidate_rows),
                **validation_results[name],
            })
        metric_fields = [
            "model", "common_threshold", "tp", "tn", "fp", "fn", "n",
            "accuracy", "precision", "recall", "specificity", "fpr", "f1",
            "f1_direct", "auc", "alert_rate", "prevalence",
        ]
        ghi_csv(
            TABLE_DIR / "05_common_threshold_duoc_chon.csv",
            selected_rows,
            ["model", "common_threshold", "selected_demo_model", "selection_dataset", "coarse_best_threshold", "candidate_count"] + metric_fields[2:],
        )
        validation_metric_rows = [
            metrics_ra_dong(name, validation_results[name], common_threshold)
            for name in TEN_MODEL
        ]
        test_metric_rows = [
            metrics_ra_dong(name, test_results[name], common_threshold)
            for name in TEN_MODEL
        ]
        baseline_metric_rows = [
            {
                "tap_du_lieu": "validation",
                "nguon_majority_class": "train_fit",
                "majority_label": baseline_label_validation,
                "class_counts_nguon": json.dumps(
                    baseline_counts_validation,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                **metrics_ra_dong(BASELINE_NAME, baseline_validation, None),
            },
            {
                "tap_du_lieu": "test",
                "nguon_majority_class": "train_full",
                "majority_label": baseline_label_test,
                "class_counts_nguon": json.dumps(
                    baseline_counts_test,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                **metrics_ra_dong(BASELINE_NAME, baseline_test, None),
            },
        ]
        comparison_metric_rows = [
            metrics_ra_dong(BASELINE_NAME, baseline_test, None),
            *test_metric_rows,
        ]
        ghi_csv(
            TABLE_DIR / "05_ket_qua_validation_common_threshold.csv",
            validation_metric_rows,
            metric_fields,
        )
        validation_confusion = [
            confusion_ra_dong(
                BASELINE_NAME,
                baseline_validation,
                None,
                "validation",
            ),
            *[
            confusion_ra_dong(name, validation_results[name], common_threshold, "validation")
            for name in TEN_MODEL
            ],
        ]
        test_confusion = [
            confusion_ra_dong(
                BASELINE_NAME,
                baseline_test,
                None,
                "test",
            ),
            *[
            confusion_ra_dong(name, test_results[name], common_threshold, "test")
            for name in TEN_MODEL
            ],
        ]
        confusion_fields = [
            "tap_du_lieu", "model", "common_threshold", "true_positive",
            "true_negative", "false_positive", "false_negative",
            "actual_late_total", "actual_not_late_total", "alert_total",
            "no_alert_total", "total",
        ]
        ghi_csv(
            TABLE_DIR / "05_confusion_matrix_validation.csv",
            validation_confusion,
            confusion_fields,
        )
        ghi_csv(
            TABLE_DIR / "05_ket_qua_test_common_threshold.csv",
            test_metric_rows,
            metric_fields,
        )
        ghi_csv(
            TABLE_DIR / "05_ket_qua_baseline.csv",
            baseline_metric_rows,
            [
                "tap_du_lieu", "nguon_majority_class", "majority_label",
                "class_counts_nguon", *metric_fields,
            ],
        )
        ghi_csv(
            TABLE_DIR / "05_confusion_matrix_test.csv",
            test_confusion,
            confusion_fields,
        )
        ghi_csv(
            TABLE_DIR / "05_logistic_coefficients.csv",
            coefficient_rows,
            ["feature_index", "transformed_feature_name", "coefficient", "intercept"],
        )
        ghi_csv(
            TABLE_DIR / "05_logistic_score_breakdown_orders.csv",
            lr_breakdown_rows,
            ["alias", "order_id", "feature_index", "transformed_feature_name", "feature_value", "coefficient", "contribution", "sum_contributions", "intercept", "z_manual", "probability_manual", "probability_spark", "absolute_difference"],
        )
        ghi_csv(
            TABLE_DIR / "05_random_forest_feature_importances.csv",
            importance_rows,
            ["feature_index", "transformed_feature_name", "feature_importance"],
        )
        ghi_csv(
            TABLE_DIR / "05_random_forest_score_breakdown_orders.csv",
            rf_breakdown_rows,
            ["alias", "order_id", "tree_index", "leaf_index", "tree_weight", "tree_raw_prediction_0", "tree_raw_prediction_1", "tree_probability_0", "tree_probability_1", "weighted_contribution_0", "weighted_contribution_1", "model_raw_prediction_0", "model_raw_prediction_1", "model_raw_total", "probability_manual", "probability_spark", "absolute_difference", "prediction_common", "common_threshold"],
        )
        ghi_csv(
            TABLE_DIR / "05_random_forest_tree_details.csv",
            tree_rows,
            ["tree_index", "tree_weight", "depth", "num_nodes", "tree_debug_string"],
        )
        roc_fields = [
            "model", "point_index", "threshold_score", "tp_cumulative",
            "fp_cumulative", "tpr", "fpr", "total_positive", "total_negative",
        ]
        trap_fields = [
            "model", "trapezoid_index", "point_i", "point_i_plus_1",
            "fpr_i", "tpr_i", "fpr_i_plus_1", "tpr_i_plus_1",
            "trapezoid_area",
        ]
        ghi_csv(TABLE_DIR / "05_roc_points_logistic_regression.csv", roc_lr, roc_fields)
        ghi_csv(TABLE_DIR / "05_roc_points_random_forest.csv", roc_rf, roc_fields)
        ghi_csv(TABLE_DIR / "05_roc_points_baseline.csv", roc_baseline, roc_fields)
        ghi_csv(TABLE_DIR / "05_auc_trapezoids_logistic_regression.csv", traps_lr, trap_fields)
        ghi_csv(TABLE_DIR / "05_auc_trapezoids_random_forest.csv", traps_rf, trap_fields)
        ghi_csv(TABLE_DIR / "05_auc_trapezoids_baseline.csv", traps_baseline, trap_fields)
        demo_csv_rows = []
        for dong in demo_rows:
            demo_csv_rows.append({
                "alias": dong["alias"],
                "order_id": dong["order_id"],
                "quy_tac": dong["quy_tac"],
                "quy_tac_thay_the": dong["quy_tac_thay_the"],
                "is_late": dong["is_late"],
                "probability_logistic": dong["probability_logistic"],
                "probability_random_forest": dong["probability_random_forest"],
                "common_threshold": common_threshold,
                "prediction_logistic": dong["prediction_logistic"],
                "prediction_random_forest": dong["prediction_random_forest"],
                "result_logistic": dong["result_logistic"],
                "result_random_forest": dong["result_random_forest"],
                "vi_tri_so_voi_threshold": dong["vi_tri_so_voi_threshold"],
                **{feature: dong[feature] for feature in COT_PHAN_LOAI + COT_SO},
            })
        demo_fields = [
            "alias", "order_id", "quy_tac", "quy_tac_thay_the", "is_late",
            "probability_logistic", "probability_random_forest",
            "common_threshold", "prediction_logistic",
            "prediction_random_forest", "result_logistic",
            "result_random_forest", "vi_tri_so_voi_threshold",
            *COT_PHAN_LOAI, *COT_SO,
        ]
        ghi_csv(
            TABLE_DIR / "05_demo_orders_A_B_C_D.csv",
            demo_csv_rows,
            demo_fields,
        )

        feature_list_rows = [
            {"ten_cot": x, "loai_du_lieu": "categorical feature", "trang_thai": "giữ", "ly_do": "Có thể biết tại thời điểm đặt hàng"}
            for x in COT_PHAN_LOAI
        ] + [
            {"ten_cot": x, "loai_du_lieu": "numeric feature", "trang_thai": "giữ", "ly_do": "Có thể biết tại thời điểm đặt hàng"}
            for x in COT_SO
        ] + [
            {"ten_cot": x, "loai_du_lieu": "không dùng", "trang_thai": "loại", "ly_do": "Feature leakage hoặc chưa chứng minh có sẵn lúc đặt hàng"}
            for x in sorted(COT_LEAKAGE_CAM)
        ]
        ghi_csv(
            TABLE_DIR / "05_danh_sach_feature.csv",
            feature_list_rows,
            ["ten_cot", "loai_du_lieu", "trang_thai", "ly_do"],
        )
        ghi_csv(
            TABLE_DIR / "05_so_sanh_mo_hinh.csv",
            comparison_metric_rows,
            metric_fields,
        )
        ghi_csv(
            TABLE_DIR / "05_ma_tran_nham_lan.csv",
            test_confusion,
            confusion_fields,
        )
        diagnostic_rows = []
        for name, df_pred in [
            (BASELINE_NAME, pred_baseline_test),
            ("Logistic Regression", pred_lr_test),
            ("Random Forest", pred_rf_test),
        ]:
            row = df_pred.agg(
                F.count("*").alias("so_mau_test"),
                F.sum("is_late").alias("so_don_late_that"),
                F.sum("prediction_common").alias("so_canh_bao"),
                F.min("probability_late").alias("probability_min"),
                F.avg("probability_late").alias("probability_mean"),
                F.max("probability_late").alias("probability_max"),
            ).first().asDict()
            diagnostic_rows.append({
                "model": name,
                "common_threshold": (
                    None if name == BASELINE_NAME else common_threshold
                ),
                **row,
            })
        ghi_csv(
            TABLE_DIR / "05_chan_doan_xac_suat.csv",
            diagnostic_rows,
            ["model", "common_threshold", "so_mau_test", "so_don_late_that", "so_canh_bao", "probability_min", "probability_mean", "probability_max"],
        )

        ve_bieu_do_threshold(refine_rows, common_threshold)
        ve_confusion_matrix(
            CHART_DIR / "05_confusion_matrix_logistic_regression.png",
            "Logistic Regression",
            test_results["Logistic Regression"],
        )
        ve_confusion_matrix(
            CHART_DIR / "05_confusion_matrix_random_forest.png",
            "Random Forest",
            test_results["Random Forest"],
        )
        ve_confusion_matrix(
            CHART_DIR / "05_confusion_matrix_baseline.png",
            BASELINE_NAME,
            baseline_test,
        )
        ve_roc(roc_lr, roc_rf, auc_lr_test, auc_rf_test)
        score_rows = pred_lr_test.select(
            "order_id", F.col("probability_late").alias("lr")
        ).join(
            pred_rf_test.select(
                "order_id", F.col("probability_late").alias("rf")
            ),
            "order_id",
            "inner",
        ).collect()
        ve_probability_distribution(
            [float(x["lr"]) for x in score_rows],
            [float(x["rf"]) for x in score_rows],
            common_threshold,
        )
        ve_so_sanh_model(comparison_results)

        signature_data = {
            "signature_version": SIGNATURE_VERSION,
            "common_threshold": common_threshold,
            "split": [
                {
                    "tap_du_lieu": x["tap_du_lieu"],
                    "so_dong": x["so_dong"],
                    "so_don_late": x["so_don_late"],
                    "so_don_not_late": x["so_don_not_late"],
                }
                for x in split_rows
            ],
            "validation": validation_results,
            "test": test_results,
            "baseline_validation_label": baseline_label_validation,
            "baseline_validation": baseline_validation,
            "baseline_test_label": baseline_label_test,
            "baseline_test": baseline_test,
            "demo_model": demo_model,
            "orders": [
                {
                    "alias": x["alias"],
                    "order_id": x["order_id"],
                    "is_late": x["is_late"],
                    "probability_logistic": x["probability_logistic"],
                    "probability_random_forest": x["probability_random_forest"],
                    "prediction_logistic": x["prediction_logistic"],
                    "prediction_random_forest": x["prediction_random_forest"],
                }
                for x in demo_rows
            ],
        }
        current_signature_json = json.dumps(
            signature_data,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        current_signature_hash = hashlib.sha256(
            current_signature_json.encode("utf-8")
        ).hexdigest()
        if previous_signature_version != SIGNATURE_VERSION:
            repro_status = "SIGNATURE_CREATED"
        else:
            repro_status = (
                "MATCH" if previous_signature_json == current_signature_json
                else "MISMATCH"
            )
            xac_nhan(
                "Lần chạy lặp có cùng threshold, split, confusion matrix, metrics và A/B/C/D",
                repro_status == "MATCH",
                f"previous={previous_signature_hash}, current={current_signature_hash}",
            )
        ghi_csv(
            TABLE_DIR / "05_reproducibility_signature.csv",
            [{
                "signature_sha256": current_signature_hash,
                "signature_json": current_signature_json,
            }],
            ["signature_sha256", "signature_json"],
        )
        ghi_csv(
            TABLE_DIR / "05_reproducibility_check.csv",
            [{
                "trang_thai": repro_status,
                "previous_signature_sha256": previous_signature_hash,
                "current_signature_sha256": current_signature_hash,
                "noi_dung_so_sanh": "common threshold; data split; validation/test confusion matrix và metrics; order A/B/C/D",
            }],
            ["trang_thai", "previous_signature_sha256", "current_signature_sha256", "noi_dung_so_sanh"],
        )

        thoi_gian_ket_thuc = datetime.now().astimezone()
        dataset_stat_sau = DATA_FILE.stat()
        dataset_hash_sau = bam_sha256(DATA_FILE)
        xac_nhan(
            "Dataset không bị sửa trong lần chạy",
            dataset_hash_truoc == dataset_hash_sau
            and dataset_stat_truoc.st_size == dataset_stat_sau.st_size
            and dataset_stat_truoc.st_mtime_ns == dataset_stat_sau.st_mtime_ns,
            f"hash trước={dataset_hash_truoc}, hash sau={dataset_hash_sau}",
        )
        for path in TABLE_FILES:
            if path.name not in ("05_code_line_reference.csv", "05_assertion_checks.csv", "05_run_metadata.csv"):
                xac_nhan(
                    f"Output tồn tại và không rỗng: {path.name}",
                    path.is_file() and path.stat().st_size > 0,
                    str(path),
                )
        for path in CHART_FILES:
            xac_nhan(
                f"Output tồn tại và không rỗng: {path.name}",
                path.is_file() and path.stat().st_size > 0,
                str(path),
            )

        context = {
            "start_time": thoi_gian_bat_dau.isoformat(),
            "end_time": thoi_gian_ket_thuc.isoformat(),
            "duration_seconds": time.perf_counter() - perf_bat_dau,
            "python_version": platform.python_version(),
            "pyspark_version": pyspark.__version__,
            "java_version": java_version,
            "dataset_hash_after": dataset_hash_sau,
            "split_rows": split_rows,
            "preprocessing_rows": preprocessing_rows,
            "mapping_rows": mapping_rows,
            "metadata_rows": metadata_rows,
            "feature_names_final": feature_names_final,
            "coarse_rows": coarse_rows,
            "refine_rows": refine_rows,
            "chosen_candidate": chosen_candidate,
            "top_candidates": top_candidates,
            "candidate_count": len(all_candidate_rows),
            "common_threshold": common_threshold,
            "threshold_mode": threshold_mode,
            "thresholds_so_sanh_thu_cong": thresholds_so_sanh_thu_cong,
            "manual_threshold_rows": manual_threshold_rows,
            "previous_threshold_display": (
                f"{previous_candidate['common_threshold']:.3f}"
                if previous_candidate else "không có"
            ),
            "next_threshold_display": (
                f"{next_candidate['common_threshold']:.3f}"
                if next_candidate else "không có"
            ),
            "previous_reason": mo_ta_ly_do_neighbor(
                previous_candidate, chosen_candidate
            ),
            "next_reason": mo_ta_ly_do_neighbor(
                next_candidate, chosen_candidate
            ),
            "validation_results": validation_results,
            "test_results": test_results,
            "baseline_validation": baseline_validation,
            "baseline_test": baseline_test,
            "baseline_label_validation": baseline_label_validation,
            "baseline_label_test": baseline_label_test,
            "baseline_counts_validation": baseline_counts_validation,
            "baseline_counts_test": baseline_counts_test,
            "comparison_results": comparison_results,
            "demo_model": demo_model,
            "demo_rows": demo_rows,
            "coefficient_rows": coefficient_rows,
            "lr_summaries": lr_summaries,
            "intercept": float(model_lr_final.intercept),
            "importance_rows": importance_rows,
            "tree_rows": tree_rows,
            "rf_summaries": rf_summaries,
            "rf_debug_string": model_rf_final.toDebugString,
            "roc_lr": roc_lr,
            "roc_rf": roc_rf,
            "roc_baseline": roc_baseline,
            "traps_lr": traps_lr,
            "traps_rf": traps_rf,
            "traps_baseline": traps_baseline,
            "auc_manual_lr": auc_manual_lr,
            "auc_manual_rf": auc_manual_rf,
            "auc_manual_baseline": auc_manual_baseline,
            "auc_diff_lr": auc_diff_lr,
            "auc_diff_rf": auc_diff_rf,
            "auc_diff_baseline": auc_diff_baseline,
            "repro_status": repro_status,
            "previous_signature": previous_signature_hash,
            "current_signature": current_signature_hash,
            "all_outputs": TABLE_FILES + CHART_FILES + [REPORT_FILE],
        }
        code_rows = tao_code_reference(context)
        context["code_rows"] = code_rows
        ghi_csv(
            TABLE_DIR / "05_code_line_reference.csv",
            code_rows,
            ["marker", "dong_code_thuc_te", "code_thuc_te", "tinh_nang", "cong_thuc", "so_lieu_that", "cach_kiem_tra"],
        )
        ghi_csv(
            TABLE_DIR / "05_assertion_checks.csv",
            CAC_ASSERTION,
            ["kiem_tra", "trang_thai", "chi_tiet"],
        )
        run_metadata = [
            {"thuoc_tinh": "thoi_gian_bat_dau", "gia_tri": context["start_time"]},
            {"thuoc_tinh": "thoi_gian_ket_thuc", "gia_tri": context["end_time"]},
            {"thuoc_tinh": "thoi_luong_giay", "gia_tri": context["duration_seconds"]},
            {"thuoc_tinh": "project", "gia_tri": str(PROJECT_DIR)},
            {"thuoc_tinh": "dataset", "gia_tri": str(DATA_FILE)},
            {"thuoc_tinh": "dataset_sha256", "gia_tri": dataset_hash_sau},
            {"thuoc_tinh": "python", "gia_tri": context["python_version"]},
            {"thuoc_tinh": "pyspark", "gia_tri": context["pyspark_version"]},
            {"thuoc_tinh": "java", "gia_tri": context["java_version"]},
            {"thuoc_tinh": "run_tag", "gia_tri": RUN_TAG},
            {"thuoc_tinh": "seed", "gia_tri": SEED},
            {"thuoc_tinh": "threshold_mode", "gia_tri": threshold_mode},
            {"thuoc_tinh": "manual_common_threshold", "gia_tri": MANUAL_COMMON_THRESHOLD},
            {"thuoc_tinh": "threshold_test_thu_cong", "gia_tri": thresholds_so_sanh_thu_cong},
            {"thuoc_tinh": "common_threshold", "gia_tri": common_threshold},
            {"thuoc_tinh": "baseline_name", "gia_tri": BASELINE_NAME},
            {"thuoc_tinh": "baseline_label_validation", "gia_tri": baseline_label_validation},
            {"thuoc_tinh": "baseline_label_test", "gia_tri": baseline_label_test},
            {"thuoc_tinh": "signature_version", "gia_tri": SIGNATURE_VERSION},
            {"thuoc_tinh": "demo_model", "gia_tri": demo_model},
            {"thuoc_tinh": "reproducibility", "gia_tri": repro_status},
        ]
        ghi_csv(
            TABLE_DIR / "05_run_metadata.csv",
            run_metadata,
            ["thuoc_tinh", "gia_tri"],
        )
        tao_bao_cao(context)
        for path in TABLE_FILES + CHART_FILES + [REPORT_FILE]:
            if not path.is_file() or path.stat().st_size == 0:
                raise RuntimeError(f"Output thiếu hoặc rỗng sau cùng: {path}")

        print("\nHOÀN TẤT BƯỚC 05")
        print(f"Run tag: {RUN_TAG}")
        print(f"Threshold mode: {threshold_mode}")
        print(f"Common threshold: {common_threshold:.3f}")
        print(f"Model demo (chọn từ validation): {demo_model}")
        print(f"Số candidate: {len(all_candidate_rows)}")
        print(f"Reproducibility: {repro_status}")
        print(
            f"{BASELINE_NAME} | test TP={baseline_test['tp']} "
            f"TN={baseline_test['tn']} FP={baseline_test['fp']} "
            f"FN={baseline_test['fn']} F1={baseline_test['f1']:.6f} "
            f"AUC={baseline_test['auc']:.6f}"
        )
        for name in TEN_MODEL:
            v = validation_results[name]
            t = test_results[name]
            print(
                f"{name} | validation TP={v['tp']} TN={v['tn']} "
                f"FP={v['fp']} FN={v['fn']} F1={v['f1']:.6f} | "
                f"test TP={t['tp']} TN={t['tn']} FP={t['fp']} "
                f"FN={t['fn']} F1={t['f1']:.6f} AUC={t['auc']:.6f}"
            )
        for dong in demo_rows:
            print(
                f"{dong['alias']}={dong['order_id']} label={dong['is_late']} "
                f"LR={dong['probability_logistic']:.12f}/"
                f"{dong['prediction_logistic']} "
                f"RF={dong['probability_random_forest']:.12f}/"
                f"{dong['prediction_random_forest']}"
            )
        print(f"Báo cáo: {REPORT_FILE}")
     
    finally:
        spark.stop()
        print("Đã dừng SparkSession.")


if __name__ == "__main__":
    main()
