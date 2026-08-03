# -*- coding: utf-8 -*-

"""Bước 02 - làm sạch và chuẩn hóa bảy bảng Olist dùng ở các bước sau.

Mục đích của file:
- Chuyển các cột thời gian từ chuỗi sang timestamp theo nhiều định dạng.
- Loại khóa thiếu, khóa trùng và các bản ghi số không hợp lệ.
- Chuẩn hóa city/state/category/payment type và xử lý installments bất thường.
- Ghi bảy CSV sạch vào ``data/processed_csv`` cùng báo cáo trước/sau làm sạch.

File không ghi đè CSV gốc. Mỗi quyết định loại hoặc sửa dữ liệu đều được thống
kê trong các bảng bằng chứng của bước 02.
"""

# Buộc Spark driver và worker dùng đúng interpreter đang chạy chương trình.
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


# Cấu hình input, output và schema cần kiểm soát thủ công.
PROJECT_DIR = Path(__file__).resolve().parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed_csv"
TABLE_DIR = PROJECT_DIR / "outputs" / "tables"
WAREHOUSE_DIR = PROJECT_DIR / "spark_warehouse"

for folder in (PROCESSED_DIR, TABLE_DIR, WAREHOUSE_DIR):
    folder.mkdir(parents=True, exist_ok=True)

# Giữ thời gian ở dạng string để hàm ``chuyen_thoi_gian`` tự thử từng format.
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

# Khóa và giá trị payments cần đúng kiểu số trước khi áp dụng điều kiện lọc.
SCHEMA_PAYMENTS = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
])


# Các hàm hỗ trợ đọc, chuyển thời gian và ghi dữ liệu.
def doc_csv(ten_file, schema=None):
    """Đọc một CSV gốc bằng schema chỉ định hoặc inferSchema của Spark."""
    duong_dan = RAW_DIR / ten_file
    if not duong_dan.is_file():
        raise FileNotFoundError(f"Khong tim thay file: {duong_dan}")

    reader = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
    )
    if schema is None:
        reader = reader.option("inferSchema", True)
    else:
        reader = reader.schema(schema)
    return reader.csv(duong_dan.as_uri())


def bieu_thuc_thoi_gian(ten_cot):
    """Tạo biểu thức Spark đọc một cột theo ba định dạng timestamp hợp lệ.

    ``coalesce`` trả về kết quả parse đầu tiên thành công; nếu cả ba đều thất
    bại thì kết quả là null để báo cáo có thể đếm chính xác giá trị lỗi.
    """
    return F.coalesce(
        F.try_to_timestamp(
            F.col(ten_cot),
            F.lit("yyyy-MM-dd HH:mm:ss"),
        ),
        F.try_to_timestamp(
            F.col(ten_cot),
            F.lit("M/d/yyyy H:mm"),
        ),
        F.try_to_timestamp(
            F.col(ten_cot),
            F.lit("M/d/yyyy H:mm:ss"),
        ),
    )


def chuyen_thoi_gian(ten_bang, df, danh_sach_cot, bao_cao):
    """Chuyển các cột thời gian và ghi số null trước/sau cho từng cột.

    Hàm phân biệt null vốn có với giá trị không null nhưng parse thất bại. Nếu
    phát sinh giá trị không đọc được, chương trình in cảnh báo và ghi vào CSV.
    """
    # Đếm null ban đầu và lỗi parse trước khi thay đổi giá trị cột.
    thong_ke_truoc = df.agg(*[
        F.sum(
            F.when(F.col(cot).isNull(), 1).otherwise(0)
        ).alias(f"{cot}_null_truoc")
        for cot in danh_sach_cot
    ], *[
        F.sum(
            F.when(
                F.col(cot).isNotNull()
                & bieu_thuc_thoi_gian(cot).isNull(),
                1,
            ).otherwise(0)
        ).alias(f"{cot}_khong_doc_duoc")
        for cot in danh_sach_cot
    ]).first().asDict()

    # Thay từng cột chuỗi bằng timestamp đã chuẩn hóa.
    ket_qua = df
    for ten_cot in danh_sach_cot:
        ket_qua = ket_qua.withColumn(
            ten_cot,
            bieu_thuc_thoi_gian(ten_cot),
        )

    # Đếm lại null sau chuyển đổi để chứng minh có hay không null mới phát sinh.
    thong_ke_sau = ket_qua.agg(*[
        F.sum(
            F.when(F.col(cot).isNull(), 1).otherwise(0)
        ).alias(f"{cot}_null_sau")
        for cot in danh_sach_cot
    ]).first().asDict()

    for ten_cot in danh_sach_cot:
        null_truoc = int(
            thong_ke_truoc[f"{ten_cot}_null_truoc"] or 0
        )
        khong_doc_duoc = int(
            thong_ke_truoc[f"{ten_cot}_khong_doc_duoc"] or 0
        )
        null_sau = int(
            thong_ke_sau[f"{ten_cot}_null_sau"] or 0
        )
        bao_cao.append({
            "ten_bang": ten_bang,
            "ten_cot": ten_cot,
            "null_truoc": null_truoc,
            "khong_doc_duoc": khong_doc_duoc,
            "null_sau": null_sau,
            "null_moi_phat_sinh": null_sau - null_truoc,
        })
        if khong_doc_duoc:
            print(
                f"CANH BAO: {ten_bang}.{ten_cot} co "
                f"{khong_doc_duoc:,} gia tri thoi gian khong doc duoc."
            )

    return ket_qua


def ghi_dataframe_csv(df, duong_dan):
    """Ghi Spark DataFrame thành một file CSV UTF-8 theo đúng thứ tự cột."""
    # ``toLocalIterator`` truyền từng dòng về driver, tránh collect toàn bảng.
    danh_sach_cot = df.columns
    with duong_dan.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        writer = csv.writer(f)
        writer.writerow(danh_sach_cot)
        for dong in df.toLocalIterator():
            writer.writerow([dong[ten_cot] for ten_cot in danh_sach_cot])


def ghi_csv(duong_dan, du_lieu, danh_sach_cot):
    """Ghi danh sách dictionary thành CSV báo cáo UTF-8 có header."""
    with duong_dan.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        writer = csv.DictWriter(f, fieldnames=danh_sach_cot)
        writer.writeheader()
        writer.writerows(du_lieu)


# Khởi tạo SparkSession phục vụ toàn bộ bước làm sạch.
spark = (
    SparkSession.builder
    .appName("Olist_Lam_Sach_Du_Lieu")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR.as_uri())
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


try:
    # Đọc bảy bảng nguồn; orders và payments dùng schema thủ công ở trên.
    orders_goc = doc_csv("olist_orders_dataset.csv", SCHEMA_ORDERS)
    customers_goc = doc_csv("olist_customers_dataset.csv")
    items_goc = doc_csv("olist_order_items_dataset.csv")
    payments_goc = doc_csv(
        "olist_order_payments_dataset.csv",
        SCHEMA_PAYMENTS,
    )
    products_goc = doc_csv("olist_products_dataset.csv")
    sellers_goc = doc_csv("olist_sellers_dataset.csv")
    translation_goc = doc_csv(
        "product_category_name_translation.csv"
    )

    # Chuyển thời gian trước khi áp dụng các quy tắc lọc và loại trùng.
    bao_cao_thoi_gian = []
    orders_da_doi_gio = chuyen_thoi_gian(
        "orders",
        orders_goc,
        [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        bao_cao_thoi_gian,
    )
    items_da_doi_gio = chuyen_thoi_gian(
        "order_items",
        items_goc,
        ["shipping_limit_date"],
        bao_cao_thoi_gian,
    )

    # Lưu số lượng bất thường trước xử lý để đưa vào báo cáo bằng chứng.
    so_installments_bat_thuong = payments_goc.filter(
        F.col("payment_installments") <= 0
    ).count()
    so_category_thieu = products_goc.filter(
        F.col("product_category_name").isNull()
        | (F.trim("product_category_name") == "")
    ).count()

    # Mỗi khối dưới đây áp dụng quy tắc làm sạch riêng của từng bảng.
    orders = (
        orders_da_doi_gio
        .filter(F.col("order_id").isNotNull())
        .dropDuplicates(["order_id"])
    )
    customers = (
        customers_goc
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
        .withColumn("customer_city", F.lower(F.trim("customer_city")))
        .withColumn("customer_state", F.upper(F.trim("customer_state")))
    )
    items = (
        items_da_doi_gio
        .dropDuplicates(["order_id", "order_item_id"])
        .filter(
            F.col("order_id").isNotNull()
            & F.col("product_id").isNotNull()
            & F.col("seller_id").isNotNull()
            & (F.col("price") > 0)
            & (F.col("freight_value") >= 0)
        )
    )
    payments = (
        payments_goc
        .dropDuplicates(["order_id", "payment_sequential"])
        .filter(
            F.col("order_id").isNotNull()
            & (F.col("payment_value") >= 0)
        )
        .withColumn("payment_type", F.lower(F.trim("payment_type")))
        .withColumn(
            "payment_installments",
            F.when(
                F.col("payment_installments") <= 0,
                1,
            ).otherwise(F.col("payment_installments")),
        )
    )
    products = (
        products_goc
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
        .withColumn(
            "product_category_name",
            F.when(
                F.col("product_category_name").isNull()
                | (F.trim("product_category_name") == ""),
                F.lit("khong_xac_dinh"),
            ).otherwise(
                F.lower(F.trim("product_category_name"))
            ),
        )
    )
    sellers = (
        sellers_goc
        .filter(F.col("seller_id").isNotNull())
        .dropDuplicates(["seller_id"])
        .withColumn("seller_city", F.lower(F.trim("seller_city")))
        .withColumn("seller_state", F.upper(F.trim("seller_state")))
    )
    translation = (
        translation_goc
        .filter(F.col("product_category_name").isNotNull())
        .dropDuplicates(["product_category_name"])
        .withColumn(
            "product_category_name",
            F.lower(F.trim("product_category_name")),
        )
    )

    # Ghép DataFrame gốc/sạch với mô tả để kiểm tra và xuất thống nhất.
    danh_sach_bang = [
        (
            "orders",
            orders_goc,
            orders,
            "Loai trung theo order_id va loai order_id thieu",
        ),
        (
            "customers",
            customers_goc,
            customers,
            "Loai trung theo customer_id; chuan hoa city va state",
        ),
        (
            "order_items",
            items_goc,
            items,
            "Loai trung khoa ghep; giu price > 0 va freight >= 0",
        ),
        (
            "payments",
            payments_goc,
            payments,
            "Loai trung khoa ghep; giu payment_value >= 0; "
            f"doi installments <= 0 thanh 1: {so_installments_bat_thuong}",
        ),
        (
            "products",
            products_goc,
            products,
            "Loai trung product_id; dien category thieu: "
            f"{so_category_thieu}",
        ),
        (
            "sellers",
            sellers_goc,
            sellers,
            "Loai trung seller_id; chuan hoa city va state",
        ),
        (
            "category_translation",
            translation_goc,
            translation,
            "Loai trung product_category_name va chuan hoa chu thuong",
        ),
    ]

    # Kiểm tra số dòng không tăng sau làm sạch rồi ghi từng bảng sạch.
    bao_cao_lam_sach = []
    for ten_bang, df_goc, df_sach, ly_do in danh_sach_bang:
        so_truoc = df_goc.count()
        so_sau = df_sach.count()
        if so_sau > so_truoc:
            raise RuntimeError(
                f"So dong {ten_bang} tang sau lam sach: "
                f"{so_truoc} -> {so_sau}"
            )

        bao_cao_lam_sach.append({
            "ten_bang": ten_bang,
            "so_dong_truoc": so_truoc,
            "so_dong_sau": so_sau,
            "so_dong_loai": so_truoc - so_sau,
            "ly_do_chinh": ly_do,
        })
        ghi_dataframe_csv(
            df_sach,
            PROCESSED_DIR / f"{ten_bang}.csv",
        )
        print(
            f"{ten_bang:22} | truoc={so_truoc:,} | "
            f"sau={so_sau:,} | loai={so_truoc - so_sau:,}"
        )

    # Xuất tóm tắt số dòng bị loại và kết quả kiểm chứng chuyển timestamp.
    ghi_csv(
        TABLE_DIR / "02_tong_hop_lam_sach.csv",
        bao_cao_lam_sach,
        [
            "ten_bang",
            "so_dong_truoc",
            "so_dong_sau",
            "so_dong_loai",
            "ly_do_chinh",
        ],
    )
    ghi_csv(
        TABLE_DIR / "02_kiem_tra_chuyen_thoi_gian.csv",
        bao_cao_thoi_gian,
        [
            "ten_bang",
            "ten_cot",
            "null_truoc",
            "khong_doc_duoc",
            "null_sau",
            "null_moi_phat_sinh",
        ],
    )

    print("\nHoan thanh lam sach du lieu.")
    print(f"Du lieu sach: {PROCESSED_DIR}")
    print(f"Bao cao: {TABLE_DIR}")

finally:
    # Luôn đóng SparkSession để lần chạy sau không giữ JVM cũ.
    spark.stop()
    print("Da dung SparkSession.")
