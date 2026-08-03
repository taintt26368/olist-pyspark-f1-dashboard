# -*- coding: utf-8 -*-

"""Bước 03 - tạo bảng ``orders_enriched`` ở mức một dòng cho mỗi order.

Mục đích của file:
- Đọc bảy bảng đã làm sạch ở bước 02.
- Tổng hợp order_items và payments về đúng khóa ``order_id`` trước khi join.
- Tạo feature tổng hợp, feature thời gian, feature logistics và label ``is_late``.
- Kiểm tra fan-out, khóa null, công thức, NaN và Infinity sau từng bước quan trọng.
- Xuất ``data/output/orders_enriched.csv`` làm đầu vào chung cho bước 04 và 05.

Nguyên tắc cốt lõi: mọi bảng con phải được đưa về đúng grain trước khi left join,
nhờ đó số dòng của orders không bị nhân bản.
"""

# Buộc Spark driver và worker dùng đúng Python hiện hành.
from pathlib import Path
import csv
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Cấu hình các thư mục dữ liệu và bằng chứng.
PROJECT_DIR = Path(__file__).resolve().parent
PROCESSED_DIR = PROJECT_DIR / "data" / "processed_csv"
OUTPUT_DIR = PROJECT_DIR / "data" / "output"
TABLE_DIR = PROJECT_DIR / "outputs" / "tables"
WAREHOUSE_DIR = PROJECT_DIR / "spark_warehouse"

for folder in (OUTPUT_DIR, TABLE_DIR, WAREHOUSE_DIR):
    folder.mkdir(parents=True, exist_ok=True)


# Các hàm hỗ trợ I/O và kiểm chứng dữ liệu.
def doc_csv(ten_bang):
    """Đọc một bảng sạch từ ``data/processed_csv`` thành Spark DataFrame."""
    duong_dan = PROCESSED_DIR / f"{ten_bang}.csv"
    if not duong_dan.is_file():
        raise FileNotFoundError(f"Khong tim thay: {duong_dan}")
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(duong_dan.as_uri())
    )


def ghi_dataframe_csv(df, duong_dan):
    """Ghi Spark DataFrame thành một CSV UTF-8, giữ nguyên thứ tự cột."""
    # Duyệt từng partition qua ``toLocalIterator`` để không collect toàn bộ.
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
    """Ghi danh sách dictionary thành CSV báo cáo có header cố định."""
    with duong_dan.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        writer = csv.DictWriter(f, fieldnames=danh_sach_cot)
        writer.writeheader()
        writer.writerows(du_lieu)


def kiem_tra_khoa(
    ten_buoc,
    df,
    khoa,
    bao_cao,
    so_dong_mong_doi=None,
):
    """Kiểm chứng uniqueness, null và số dòng tại một bước join/aggregation.

    ``so_dong_mong_doi`` được truyền vào khi phép join phải giữ nguyên số dòng.
    Hàm dừng chương trình nếu có fan-out, khóa null hoặc row count bị thay đổi,
    đồng thời ghi thống kê vào ``bao_cao`` để tạo bằng chứng CSV.
    """
    # Một khóa hợp lệ phải có số dòng bằng số tổ hợp khóa distinct.
    so_dong = df.count()
    so_khoa = df.select(*khoa).distinct().count()

    # Ghép điều kiện OR để phát hiện null ở bất kỳ thành phần nào của khóa.
    dieu_kien_null = F.col(khoa[0]).isNull()
    for ten_cot in khoa[1:]:
        dieu_kien_null = dieu_kien_null | F.col(ten_cot).isNull()
    so_khoa_null = df.filter(dieu_kien_null).count()
    so_dong_nhan_ban = so_dong - so_khoa

    bao_cao.append({
        "buoc": ten_buoc,
        "khoa_kiem_tra": ", ".join(khoa),
        "so_dong": so_dong,
        "so_khoa_khac_nhau": so_khoa,
        "so_dong_nhan_ban": so_dong_nhan_ban,
        "so_khoa_null": so_khoa_null,
    })

    if so_dong_nhan_ban != 0 or so_khoa_null != 0:
        raise RuntimeError(
            f"Phat sinh fan-out hoac khoa khong hop le tai {ten_buoc}: "
            f"dong={so_dong}, khoa={so_khoa}, "
            f"nhan_ban={so_dong_nhan_ban}, khoa_null={so_khoa_null}"
        )
    if so_dong_mong_doi is not None and so_dong != so_dong_mong_doi:
        raise RuntimeError(
            f"So dong thay doi tai {ten_buoc}: "
            f"mong_doi={so_dong_mong_doi}, thuc_te={so_dong}"
        )
    return so_dong


def kiem_tra_cong_thuc(df):
    """Tính lại độc lập các feature dẫn xuất và đếm mọi dòng sai công thức.

    Các quy tắc được kiểm chứng gồm order_value, freight_ratio, số ngày dự kiến,
    chênh lệch giao hàng, label is_late và chỉ báo cùng bang. Chỉ cần một dòng
    sai là chương trình dừng để không chuyển dữ liệu lỗi sang bước 04/05.
    """
    # Biểu thức mong đợi dùng eqNullSafe ở dưới để so sánh cả trường hợp null.
    cung_bang_mong_doi = (
        F.when(
            F.col("customer_state").isNull()
            | F.col("main_seller_state").isNull(),
            F.lit(None).cast("int"),
        )
        .when(
            F.col("customer_state") == F.col("main_seller_state"),
            1,
        )
        .otherwise(0)
    )
    # Một aggregation duy nhất đếm sai lệch của toàn bộ công thức.
    dong = df.agg(
        F.sum(
            F.when(
                F.abs(
                    F.col("order_value")
                    - (F.col("total_price") + F.col("total_freight"))
                ) > 0.011,
                1,
            ).otherwise(0)
        ).alias("sai_order_value"),
        F.sum(
            F.when(
                (F.col("total_price") > 0)
                & (
                    F.abs(
                        F.col("freight_ratio")
                        - F.col("total_freight") / F.col("total_price")
                    ) > 0.00011
                ),
                1,
            ).otherwise(0)
        ).alias("sai_freight_ratio"),
        F.sum(
            F.when(
                F.col("estimated_delivery_days")
                != F.datediff(
                    F.to_date("order_estimated_delivery_date"),
                    F.to_date("order_purchase_timestamp"),
                ),
                1,
            ).otherwise(0)
        ).alias("sai_so_ngay_du_kien"),
        F.sum(
            F.when(
                F.col("delivery_difference_days")
                != F.datediff(
                    F.to_date("order_delivered_customer_date"),
                    F.to_date("order_estimated_delivery_date"),
                ),
                1,
            ).otherwise(0)
        ).alias("sai_chenh_lech_giao"),
        F.sum(
            F.when(
                F.col("is_late")
                != F.when(
                    F.datediff(
                        F.to_date("order_delivered_customer_date"),
                        F.to_date("order_estimated_delivery_date"),
                    ) > 0,
                    1,
                ).otherwise(0),
                1,
            ).otherwise(0)
        ).alias("sai_nhan_is_late"),
        F.sum(
            F.when(
                ~F.col("customer_seller_same_state").eqNullSafe(
                    cung_bang_mong_doi
                ),
                1,
            ).otherwise(0)
        ).alias("sai_cung_bang"),
    ).first().asDict()

    sai = {ten: int(gia_tri or 0) for ten, gia_tri in dong.items()}
    if any(sai.values()):
        raise RuntimeError(f"Phat hien cong thuc sai: {sai}")
    return sai


def kiem_tra_nan_vo_cuc(df, danh_sach_cot):
    """Dừng chương trình nếu feature số chứa NaN hoặc dương/âm Infinity."""
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
    if loi:
        raise RuntimeError(f"Phat hien NaN hoac Infinity: {loi}")


# Khởi tạo SparkSession cho bước tích hợp và tạo feature.
spark = (
    SparkSession.builder
    .appName("Olist_Tao_Orders_Enriched")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR.as_uri())
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


try:
    # Chỉ giữ orders đã delivered và có đủ ngày thực tế/dự kiến để tạo label.
    bao_cao_join = []
    orders = (
        doc_csv("orders")
        .withColumn(
            "order_purchase_timestamp",
            F.to_timestamp("order_purchase_timestamp"),
        )
        .withColumn(
            "order_delivered_customer_date",
            F.to_timestamp("order_delivered_customer_date"),
        )
        .withColumn(
            "order_estimated_delivery_date",
            F.to_timestamp("order_estimated_delivery_date"),
        )
        .filter(
            (F.col("order_status") == "delivered")
            & F.col("order_delivered_customer_date").isNotNull()
            & F.col("order_estimated_delivery_date").isNotNull()
        )
    ).cache()
    # Đọc các dimension/fact table sạch để chuẩn bị enrichment.
    customers = doc_csv("customers")
    items = doc_csv("order_items").cache()
    payments = doc_csv("payments")
    products = doc_csv("products")
    sellers = doc_csv("sellers")
    translation = doc_csv("category_translation")

    # Nối product với category translation; fallback về tên gốc nếu thiếu dịch.
    so_products = products.count()
    products_info = (
        products.alias("p")
        .join(
            translation.alias("t"),
            F.col("p.product_category_name")
            == F.col("t.product_category_name"),
            "left",
        )
        .select(
            F.col("p.product_id"),
            F.coalesce(
                F.col("t.product_category_name_english"),
                F.col("p.product_category_name"),
                F.lit("khong_xac_dinh"),
            ).alias("category_name"),
            F.col("p.product_weight_g"),
            F.col("p.product_length_cm"),
            F.col("p.product_height_cm"),
            F.col("p.product_width_cm"),
        )
    )
    kiem_tra_khoa(
        "products_join_translation",
        products_info,
        ["product_id"],
        bao_cao_join,
        so_products,
    )

    # Bổ sung thông tin product và seller vào từng order item; mọi join đều được
    # kiểm tra giữ nguyên khóa ghép (order_id, order_item_id).
    so_items = items.count()
    items_full = items.join(
        products_info,
        "product_id",
        "left",
    )
    kiem_tra_khoa(
        "items_join_products",
        items_full,
        ["order_id", "order_item_id"],
        bao_cao_join,
        so_items,
    )
    items_full = (
        items_full
        .join(
            sellers.select("seller_id", "seller_state"),
            "seller_id",
            "left",
        )
        .withColumn(
            "item_volume_cm3",
            F.col("product_length_cm")
            * F.col("product_height_cm")
            * F.col("product_width_cm"),
        )
    ).cache()
    kiem_tra_khoa(
        "items_join_sellers",
        items_full,
        ["order_id", "order_item_id"],
        bao_cao_join,
        so_items,
    )

    # Tổng hợp numeric feature của items về một dòng duy nhất cho mỗi order_id.
    items_order = (
        items_full
        .groupBy("order_id")
        .agg(
            F.count("*").alias("item_count"),
            F.countDistinct("product_id").alias("product_count"),
            F.countDistinct("seller_id").alias("seller_count"),
            F.round(F.sum("price"), 2).alias("total_price"),
            F.round(F.sum("freight_value"), 2).alias("total_freight"),
            F.round(F.avg("price"), 2).alias("average_item_price"),
            F.sum("product_weight_g").alias("total_weight_g"),
            F.sum("item_volume_cm3").alias("total_volume_cm3"),
        )
    )

    # Chọn main_category theo số item; tên category tăng dần là tie-break ổn định.
    cua_so_danh_muc = (
        Window.partitionBy("order_id")
        .orderBy(F.col("so_luong").desc(), F.col("category_name").asc())
    )
    danh_muc_chinh = (
        items_full
        .groupBy("order_id", "category_name")
        .agg(F.count("*").alias("so_luong"))
        .withColumn("thu_tu", F.row_number().over(cua_so_danh_muc))
        .filter(F.col("thu_tu") == 1)
        .select(
            "order_id",
            F.col("category_name").alias("main_category"),
        )
    )

    # Chọn main_seller_state bằng quy tắc tương tự để kết quả tái lập được.
    cua_so_nguoi_ban = (
        Window.partitionBy("order_id")
        .orderBy(F.col("so_luong").desc(), F.col("seller_state").asc())
    )
    nguoi_ban_chinh = (
        items_full
        .filter(F.col("seller_state").isNotNull())
        .groupBy("order_id", "seller_state")
        .agg(F.count("*").alias("so_luong"))
        .withColumn("thu_tu", F.row_number().over(cua_so_nguoi_ban))
        .filter(F.col("thu_tu") == 1)
        .select(
            "order_id",
            F.col("seller_state").alias("main_seller_state"),
        )
    )

    so_items_order = items_order.count()
    items_order = (
        items_order
        .join(danh_muc_chinh, "order_id", "left")
        .join(nguoi_ban_chinh, "order_id", "left")
    )
    kiem_tra_khoa(
        "items_order_hoan_chinh",
        items_order,
        ["order_id"],
        bao_cao_join,
        so_items_order,
    )

    # Tổng hợp payments về order_id trước join để không gây fan-out orders.
    payments_order = (
        payments
        .groupBy("order_id")
        .agg(
            F.round(F.sum("payment_value"), 2).alias("payment_value_total"),
            F.count("*").alias("payment_record_count"),
            F.countDistinct("payment_type").alias("payment_type_count"),
            F.max("payment_installments").alias("max_installments"),
        )
    )
    # Chọn main_payment_type theo tổng tiền, dùng tên payment type làm tie-break.
    cua_so_thanh_toan = (
        Window.partitionBy("order_id")
        .orderBy(F.col("tong_tien").desc(), F.col("payment_type").asc())
    )
    thanh_toan_chinh = (
        payments
        .groupBy("order_id", "payment_type")
        .agg(F.sum("payment_value").alias("tong_tien"))
        .withColumn("thu_tu", F.row_number().over(cua_so_thanh_toan))
        .filter(F.col("thu_tu") == 1)
        .select(
            "order_id",
            F.col("payment_type").alias("main_payment_type"),
        )
    )
    so_payments_order = payments_order.count()
    payments_order = payments_order.join(
        thanh_toan_chinh,
        "order_id",
        "left",
    )
    kiem_tra_khoa(
        "payments_order_hoan_chinh",
        payments_order,
        ["order_id"],
        bao_cao_join,
        so_payments_order,
    )

    # Chuỗi left join chỉ bắt đầu sau khi các bảng con đã ở grain order_id.
    so_orders = kiem_tra_khoa(
        "orders",
        orders,
        ["order_id"],
        bao_cao_join,
    )
    orders_enriched = orders.join(
        customers.select("customer_id", "customer_state"),
        "customer_id",
        "left",
    )
    kiem_tra_khoa(
        "orders_join_customers",
        orders_enriched,
        ["order_id"],
        bao_cao_join,
        so_orders,
    )
    orders_enriched = orders_enriched.join(
        items_order,
        "order_id",
        "left",
    )
    kiem_tra_khoa(
        "orders_join_items",
        orders_enriched,
        ["order_id"],
        bao_cao_join,
        so_orders,
    )
    orders_enriched = orders_enriched.join(
        payments_order,
        "order_id",
        "left",
    )
    kiem_tra_khoa(
        "orders_join_payments",
        orders_enriched,
        ["order_id"],
        bao_cao_join,
        so_orders,
    )

    # Tạo feature dẫn xuất và label trực tiếp từ các cột đã kiểm chứng.
    orders_enriched = (
        orders_enriched
        .withColumn(
            "order_value",
            F.round(F.col("total_price") + F.col("total_freight"), 2),
        )
        .withColumn(
            "freight_ratio",
            F.when(
                F.col("total_price") > 0,
                F.round(
                    F.col("total_freight") / F.col("total_price"),
                    4,
                ),
            ),
        )
        .withColumn("purchase_year", F.year("order_purchase_timestamp"))
        .withColumn("purchase_month", F.month("order_purchase_timestamp"))
        .withColumn(
            "purchase_day_of_week",
            F.dayofweek("order_purchase_timestamp"),
        )
        .withColumn("purchase_hour", F.hour("order_purchase_timestamp"))
        .withColumn(
            "estimated_delivery_days",
            F.datediff(
                F.to_date("order_estimated_delivery_date"),
                F.to_date("order_purchase_timestamp"),
            ),
        )
        .withColumn(
            "delivery_difference_days",
            F.datediff(
                F.to_date("order_delivered_customer_date"),
                F.to_date("order_estimated_delivery_date"),
            ),
        )
        .withColumn(
            "is_late",
            F.when(F.col("delivery_difference_days") > 0, 1).otherwise(0),
        )
        .withColumn(
            "customer_seller_same_state",
            F.when(
                F.col("customer_state").isNull()
                | F.col("main_seller_state").isNull(),
                F.lit(None).cast("int"),
            )
            .when(
                F.col("customer_state") == F.col("main_seller_state"),
                1,
            )
            .otherwise(0),
        )
        .select(
            "order_id",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "customer_state",
            "item_count",
            "product_count",
            "seller_count",
            "total_price",
            "total_freight",
            "average_item_price",
            "total_weight_g",
            "total_volume_cm3",
            "main_category",
            "main_seller_state",
            "payment_value_total",
            "payment_record_count",
            "payment_type_count",
            "max_installments",
            "main_payment_type",
            "order_value",
            "freight_ratio",
            "purchase_year",
            "purchase_month",
            "purchase_day_of_week",
            "purchase_hour",
            "estimated_delivery_days",
            "delivery_difference_days",
            "is_late",
            "customer_seller_same_state",
        )
    ).cache()

    # Cổng chất lượng cuối: tên cột, grain, công thức và số hữu hạn phải đúng.
    if len(orders_enriched.columns) != len(set(orders_enriched.columns)):
        raise RuntimeError("orders_enriched co ten cot bi trung")
    kiem_tra_khoa(
        "orders_enriched_cuoi",
        orders_enriched,
        ["order_id"],
        bao_cao_join,
        so_orders,
    )
    kiem_tra_cong_thuc(orders_enriched)
    kiem_tra_nan_vo_cuc(
        orders_enriched,
        [
            "total_price",
            "total_freight",
            "average_item_price",
            "payment_value_total",
            "order_value",
            "freight_ratio",
        ],
    )

    # Ghi dataset duy nhất cho bước EDA/model và bảng bằng chứng từng phép join.
    output_file = OUTPUT_DIR / "orders_enriched.csv"
    ghi_dataframe_csv(orders_enriched, output_file)
    ghi_csv(
        TABLE_DIR / "03_kiem_tra_join.csv",
        bao_cao_join,
        [
            "buoc",
            "khoa_kiem_tra",
            "so_dong",
            "so_khoa_khac_nhau",
            "so_dong_nhan_ban",
            "so_khoa_null",
        ],
    )

    tong_don = orders_enriched.count()
    so_don_tre = orders_enriched.filter(F.col("is_late") == 1).count()
    print("\nKIEM TRA JOIN")
    for dong in bao_cao_join:
        print(
            f"{dong['buoc']:30} | dong={dong['so_dong']:,} | "
            f"khoa={dong['so_khoa_khac_nhau']:,} | "
            f"nhan_ban={dong['so_dong_nhan_ban']:,}"
        )
    print("\nTONG QUAN")
    print(f"Tong don    : {tong_don:,}")
    print(f"Don giao tre: {so_don_tre:,}")
    print(f"Ty le tre   : {so_don_tre * 100 / tong_don:.4f}%")
    print(f"Du lieu: {output_file}")

finally:
    # Luôn đóng SparkSession, kể cả khi một assertion dừng chương trình.
    spark.stop()
    print("Da dung SparkSession.")
