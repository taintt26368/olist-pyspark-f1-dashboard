# -*- coding: utf-8 -*-

"""Bước 01 - đọc và kiểm tra chất lượng các bảng dữ liệu Olist gốc.

Mục đích của file:
- Đọc tám CSV nguồn bằng PySpark và giữ đúng schema của các bảng quan trọng.
- Đếm số dòng, số cột, khóa trùng, dòng trùng và giá trị thiếu.
- Phát hiện các bất thường nghiệp vụ cần xử lý ở bước làm sạch.
- Xuất ba bảng bằng chứng vào ``outputs/tables``; file này không sửa dữ liệu gốc.

Luồng xử lý: CSV gốc -> DataFrame -> kiểm tra schema/khóa/null/bất thường
-> các báo cáo CSV chất lượng dữ liệu.
"""

# ``sys.executable`` buộc Spark worker và Spark driver dùng cùng Python đang
# chạy file này, tránh lệch môi trường giữa Spyder, terminal và PySpark.
from pathlib import Path
import csv
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# Cấu hình đường dẫn và schema đầu vào.
PROJECT_DIR = Path(__file__).resolve().parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
TABLE_DIR = PROJECT_DIR / "outputs" / "tables"
WAREHOUSE_DIR = PROJECT_DIR / "spark_warehouse"

TABLE_DIR.mkdir(parents=True, exist_ok=True)
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

# Schema thủ công giữ các cột thời gian ở dạng chuỗi tại bước kiểm tra; việc
# chuyển timestamp được thực hiện có kiểm chứng trong bước 02.
SCHEMA_ORDERS = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
])

# Schema thủ công của payments ngăn Spark suy luận sai kiểu số.
SCHEMA_PAYMENTS = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
])

# Mỗi phần tử mô tả: tên file, schema (nếu có), khóa cần kiểm tra và cờ CSV có
# nội dung nhiều dòng. Cấu hình tập trung giúp tám bảng được kiểm tra nhất quán.
DANH_SACH_BANG = {
    "orders": (
        "olist_orders_dataset.csv",
        SCHEMA_ORDERS,
        ["order_id"],
        False,
    ),
    "order_items": (
        "olist_order_items_dataset.csv",
        None,
        ["order_id", "order_item_id"],
        False,
    ),
    "customers": (
        "olist_customers_dataset.csv",
        None,
        ["customer_id"],
        False,
    ),
    "payments": (
        "olist_order_payments_dataset.csv",
        SCHEMA_PAYMENTS,
        ["order_id", "payment_sequential"],
        False,
    ),
    "reviews": (
        "olist_order_reviews_dataset.csv",
        None,
        ["review_id", "order_id"],
        True,
    ),
    "products": (
        "olist_products_dataset.csv",
        None,
        ["product_id"],
        False,
    ),
    "sellers": (
        "olist_sellers_dataset.csv",
        None,
        ["seller_id"],
        False,
    ),
    "category_translation": (
        "product_category_name_translation.csv",
        None,
        ["product_category_name"],
        False,
    ),
}


# Các hàm hỗ trợ đọc dữ liệu, ghi báo cáo và thống kê chất lượng.
def doc_ten_cot_csv(duong_dan):
    """Đọc riêng header CSV để đối chiếu thứ tự cột với schema thủ công."""
    # ``utf-8-sig`` loại bỏ BOM nếu file được tạo bởi Excel trên Windows.
    with duong_dan.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        return next(csv.reader(f))


def doc_csv(ten_file, schema=None, nhieu_dong=False):
    """Đọc một CSV nguồn thành Spark DataFrame với cấu hình an toàn.

    ``schema`` được dùng khi kiểu dữ liệu cần kiểm soát chặt; nếu không có,
    Spark được phép inferSchema. ``nhieu_dong`` bật chế độ đọc review text có
    thể chứa ký tự xuống dòng. Hàm dừng ngay khi file hoặc header không đúng.
    """
    duong_dan = RAW_DIR / ten_file
    if not duong_dan.is_file():
        raise FileNotFoundError(f"Khong tim thay file: {duong_dan}")

    # PERMISSIVE giữ dòng đầu vào để bước kiểm tra có thể phát hiện vấn đề.
    reader = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
    )
    if nhieu_dong:
        # Cho phép trường text được bao bởi dấu nháy kép và chứa newline.
        reader = (
            reader
            .option("multiLine", True)
            .option("escape", '"')
        )
    if schema is None:
        # Chỉ suy luận schema cho các bảng không có schema thủ công ở trên.
        reader = reader.option("inferSchema", True)
    else:
        # So khớp header trước khi gắn schema để tránh dữ liệu lệch cột âm thầm.
        ten_cot_goc = doc_ten_cot_csv(duong_dan)
        ten_cot_schema = [field.name for field in schema.fields]
        if ten_cot_goc != ten_cot_schema:
            raise RuntimeError(
                f"Ten cot {ten_file} khac schema thu cong: "
                f"{ten_cot_goc}"
            )
        reader = reader.schema(schema)

    return reader.csv(duong_dan.as_uri())


def ghi_csv(duong_dan, du_lieu, danh_sach_cot):
    """Ghi danh sách dictionary thành CSV UTF-8 có header cố định."""
    with duong_dan.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        writer = csv.DictWriter(f, fieldnames=danh_sach_cot)
        writer.writeheader()
        writer.writerows(du_lieu)


def dem_null_theo_cot(df):
    """Đếm số giá trị null của từng cột chỉ bằng một phép aggregation Spark."""
    # Mỗi biểu thức đổi null thành 1 rồi cộng, nhờ đó không collect toàn bảng.
    bieu_thuc = [
        F.sum(
            F.when(F.col(ten_cot).isNull(), 1).otherwise(0)
        ).alias(ten_cot)
        for ten_cot in df.columns
    ]
    return df.agg(*bieu_thuc).first().asDict()


def them_bat_thuong(danh_sach, ten_bang, noi_dung, so_dong):
    """Chuẩn hóa một phát hiện bất thường rồi thêm vào báo cáo tổng hợp."""
    danh_sach.append({
        "ten_bang": ten_bang,
        "noi_dung": noi_dung,
        "so_dong": int(so_dong),
    })


# Khởi tạo SparkSession local; ``finally`` ở cuối luôn đóng session kể cả lỗi.
spark = (
    SparkSession.builder
    .appName("Olist_Doc_Va_Kiem_Tra_Du_Lieu")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR.as_uri())
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


try:
    # Ba danh sách này lần lượt lưu thống kê cấp bảng, cấp cột và bất thường.
    bao_cao_tong_hop = []
    bao_cao_cot = []
    bao_cao_bat_thuong = []
    cac_bang = {}

    for ten_bang, thong_tin in DANH_SACH_BANG.items():
        # Đọc và cache vì cùng DataFrame được dùng cho nhiều phép count.
        ten_file, schema, khoa, nhieu_dong = thong_tin
        df = doc_csv(ten_file, schema, nhieu_dong).cache()
        cac_bang[ten_bang] = df

        # So sánh số dòng với số khóa distinct để định lượng khóa bị trùng.
        so_dong = df.count()
        so_dong_khoa_khac_nhau = (
            df.select(*khoa).dropDuplicates().count()
        )
        so_trung_khoa = so_dong - so_dong_khoa_khac_nhau
        so_trung_toan_dong = so_dong - df.dropDuplicates().count()
        ket_qua_null = dem_null_theo_cot(df)
        tong_gia_tri_thieu = sum(
            int(gia_tri or 0)
            for gia_tri in ket_qua_null.values()
        )

        # Tạo một dòng báo cáo cho từng cột, gồm kiểu dữ liệu và tỷ lệ thiếu.
        kieu_du_lieu = dict(df.dtypes)
        for ten_cot in df.columns:
            so_thieu = int(ket_qua_null[ten_cot] or 0)
            bao_cao_cot.append({
                "ten_bang": ten_bang,
                "ten_cot": ten_cot,
                "kieu_du_lieu": kieu_du_lieu[ten_cot],
                "so_gia_tri_thieu": so_thieu,
                "ty_le_thieu_phan_tram": round(
                    so_thieu * 100 / so_dong,
                    4,
                ) if so_dong else 0.0,
            })

        # Tạo một dòng tóm tắt chất lượng cho toàn bộ bảng.
        bao_cao_tong_hop.append({
            "ten_bang": ten_bang,
            "ten_file": ten_file,
            "so_dong": so_dong,
            "so_cot": len(df.columns),
            "ten_cot": ", ".join(df.columns),
            "kieu_du_lieu": ", ".join(
                f"{cot}:{kieu}" for cot, kieu in df.dtypes
            ),
            "khoa_kiem_tra": ", ".join(khoa),
            "so_trung_khoa": so_trung_khoa,
            "so_trung_toan_dong": so_trung_toan_dong,
            "tong_gia_tri_thieu": tong_gia_tri_thieu,
        })

        print(
            f"{ten_bang:22} | dong={so_dong:,} | "
            f"cot={len(df.columns)} | trung_khoa={so_trung_khoa:,} | "
            f"gia_tri_thieu={tong_gia_tri_thieu:,}"
        )

    # Kiểm tra các quy tắc nghiệp vụ quan trọng ngoài null và khóa trùng.
    orders = cac_bang["orders"]
    items = cac_bang["order_items"]
    payments = cac_bang["payments"]
    reviews = cac_bang["reviews"]
    products = cac_bang["products"]

    them_bat_thuong(
        bao_cao_bat_thuong,
        "orders",
        "Don delivered thieu ngay giao thuc te hoac ngay du kien",
        orders.filter(
            (F.col("order_status") == "delivered")
            & (
                F.col("order_delivered_customer_date").isNull()
                | F.col("order_estimated_delivery_date").isNull()
            )
        ).count(),
    )
    them_bat_thuong(
        bao_cao_bat_thuong,
        "order_items",
        "price khong lon hon 0",
        items.filter(F.col("price") <= 0).count(),
    )
    them_bat_thuong(
        bao_cao_bat_thuong,
        "order_items",
        "freight_value am",
        items.filter(F.col("freight_value") < 0).count(),
    )
    them_bat_thuong(
        bao_cao_bat_thuong,
        "payments",
        "payment_installments khong lon hon 0",
        payments.filter(F.col("payment_installments") <= 0).count(),
    )
    them_bat_thuong(
        bao_cao_bat_thuong,
        "payments",
        "payment_value am",
        payments.filter(F.col("payment_value") < 0).count(),
    )
    them_bat_thuong(
        bao_cao_bat_thuong,
        "reviews",
        "review_id bi lap",
        reviews.count()
        - reviews.select("review_id").dropDuplicates().count(),
    )
    them_bat_thuong(
        bao_cao_bat_thuong,
        "reviews",
        "review_score nam ngoai khoang 1 den 5",
        reviews.filter(~F.col("review_score").between(1, 5)).count(),
    )
    them_bat_thuong(
        bao_cao_bat_thuong,
        "products",
        "Thieu product_category_name",
        products.filter(F.col("product_category_name").isNull()).count(),
    )
    them_bat_thuong(
        bao_cao_bat_thuong,
        "products",
        "Thieu trong luong hoac kich thuoc",
        products.filter(
            F.col("product_weight_g").isNull()
            | F.col("product_length_cm").isNull()
            | F.col("product_height_cm").isNull()
            | F.col("product_width_cm").isNull()
        ).count(),
    )

    # Ghi bằng chứng ra CSV; mọi con số đều lấy từ lần chạy hiện tại.
    ghi_csv(
        TABLE_DIR / "01_tong_hop_chat_luong_du_lieu.csv",
        bao_cao_tong_hop,
        [
            "ten_bang",
            "ten_file",
            "so_dong",
            "so_cot",
            "ten_cot",
            "kieu_du_lieu",
            "khoa_kiem_tra",
            "so_trung_khoa",
            "so_trung_toan_dong",
            "tong_gia_tri_thieu",
        ],
    )
    ghi_csv(
        TABLE_DIR / "01_chi_tiet_cot.csv",
        bao_cao_cot,
        [
            "ten_bang",
            "ten_cot",
            "kieu_du_lieu",
            "so_gia_tri_thieu",
            "ty_le_thieu_phan_tram",
        ],
    )
    ghi_csv(
        TABLE_DIR / "01_bat_thuong_quan_trong.csv",
        bao_cao_bat_thuong,
        ["ten_bang", "noi_dung", "so_dong"],
    )

    print("\nHoan thanh doc va kiem tra du lieu.")
    print(f"Bao cao: {TABLE_DIR}")

finally:
    # Giải phóng JVM và tài nguyên Spark dù chương trình thành công hay lỗi.
    spark.stop()
    print("Da dung SparkSession.")
