# -*- coding: utf-8 -*-

"""Bước 04 - khám phá dữ liệu giao hàng từ ``orders_enriched``.

Mục đích của file:
- Mô tả prevalence giao trễ trên toàn bộ dataset.
- So sánh tỷ lệ trễ theo tháng, bang khách hàng, phạm vi vận chuyển và số item.
- Kiểm tra tổng của từng bảng nhóm luôn quay về đúng tổng orders_enriched.
- Xuất bốn bảng CSV và ba biểu đồ PNG phục vụ phân tích trong báo cáo.

Đây là Exploratory Data Analysis (EDA), chỉ mô tả quan hệ quan sát được và
không khẳng định quan hệ nhân quả.
"""

# Buộc Spark driver và worker dùng đúng interpreter của Spyder/terminal.
from pathlib import Path
import csv
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# ``matplotlib`` là dependency bắt buộc để tạo PNG. Thông báo dưới đây ghi rõ
# interpreter và lệnh cài, giúp chẩn đoán khi Spyder dùng nhầm Conda environment.
try:
    import matplotlib
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "Thiếu package matplotlib trong Python đang chạy: "
        f"{sys.executable}. Hãy cài bằng lệnh: "
        f'"{sys.executable}" -m pip install matplotlib'
    ) from exc

# Backend Agg tạo ảnh không cần cửa sổ GUI, phù hợp cả terminal và Spyder.
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Cấu hình input orders_enriched và các thư mục output.
PROJECT_DIR = Path(__file__).resolve().parent
DATA_FILE = PROJECT_DIR / "data" / "output" / "orders_enriched.csv"
TABLE_DIR = PROJECT_DIR / "outputs" / "tables"
CHART_DIR = PROJECT_DIR / "outputs" / "charts"
WAREHOUSE_DIR = PROJECT_DIR / "spark_warehouse"

for folder in (TABLE_DIR, CHART_DIR, WAREHOUSE_DIR):
    folder.mkdir(parents=True, exist_ok=True)


# Các hàm hỗ trợ ghi file và kiểm chứng kết quả aggregation.
def ghi_dataframe_csv(df, duong_dan):
    """Ghi Spark DataFrame thành một CSV UTF-8 theo đúng thứ tự cột."""
    # Duyệt từng dòng giúp giảm bộ nhớ driver so với gọi collect toàn DataFrame.
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


def them_ty_le_tre(df):
    """Thêm tỷ lệ trễ (%) = so_don_tre / tong_don cho từng nhóm.

    ``when(tong_don > 0)`` ngăn chia cho 0; kết quả được làm tròn hai chữ số để
    dùng thống nhất trong bảng và biểu đồ EDA.
    """
    return df.withColumn(
        "ty_le_tre_phan_tram",
        F.when(
            F.col("tong_don") > 0,
            F.round(
                F.col("so_don_tre") * 100 / F.col("tong_don"),
                2,
            ),
        ),
    )


def kiem_tra_tong_nhom(ten_bang, df, tong_don, so_don_tre):
    """Xác nhận cộng các nhóm bằng đúng tổng order và tổng order trễ gốc."""
    dong = df.agg(
        F.sum("tong_don").alias("tong_don"),
        F.sum("so_don_tre").alias("so_don_tre"),
    ).first()
    if dong["tong_don"] != tong_don or dong["so_don_tre"] != so_don_tre:
        raise RuntimeError(
            f"Tong cua {ten_bang} khong khop orders_enriched: "
            f"{dong.asDict()}"
        )


def kiem_tra_ty_le_hop_le(ten_bang, df):
    """Dừng chương trình nếu tỷ lệ nhóm là null, NaN hoặc Infinity."""
    for dong in df.select("ty_le_tre_phan_tram").collect():
        gia_tri = dong["ty_le_tre_phan_tram"]
        if gia_tri is None:
            raise RuntimeError(f"Ty le null trong {ten_bang}")
        if gia_tri != gia_tri or gia_tri in (
            float("inf"),
            float("-inf"),
        ):
            raise RuntimeError(f"Ty le NaN hoac Infinity trong {ten_bang}")


# Khởi tạo SparkSession cho các phép groupBy của bước khám phá dữ liệu.
spark = (
    SparkSession.builder
    .appName("Olist_Kham_Pha_Du_Lieu")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR.as_uri())
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


try:
    # Cổng đầu vào: file phải tồn tại và mỗi order_id chỉ được xuất hiện một lần.
    if not DATA_FILE.is_file():
        raise FileNotFoundError(f"Khong tim thay: {DATA_FILE}")

    # Đọc và cache vì cùng dataset được dùng cho năm câu hỏi EDA.
    orders = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(DATA_FILE.as_uri())
    ).cache()

    tong_don = orders.count()
    so_order_id = orders.select("order_id").distinct().count()
    if tong_don != so_order_id:
        raise RuntimeError(
            f"orders_enriched bi trung order_id: {tong_don - so_order_id}"
        )
    so_don_tre = orders.filter(F.col("is_late") == 1).count()

    # Câu 1: đo quy mô dataset, số order trễ và prevalence toàn bộ.
    ty_le_tre = so_don_tre * 100 / tong_don if tong_don else 0.0
    tong_quan = [
        {"chi_so": "Tong so don", "gia_tri": tong_don},
        {"chi_so": "So don giao tre", "gia_tri": so_don_tre},
        {
            "chi_so": "Ty le giao tre phan tram",
            "gia_tri": round(ty_le_tre, 4),
        },
    ]
    ghi_csv(
        TABLE_DIR / "04_tong_quan_giao_tre.csv",
        tong_quan,
        ["chi_so", "gia_tri"],
    )

    # Câu 2: xem tỷ lệ giao trễ biến đổi theo tháng mua hàng.
    theo_thang = (
        orders
        .withColumn(
            "thang",
            F.concat(
                F.col("purchase_year"),
                F.lit("-"),
                F.lpad(F.col("purchase_month").cast("string"), 2, "0"),
            ),
        )
        .groupBy("thang")
        .agg(
            F.count("*").alias("tong_don"),
            F.sum("is_late").alias("so_don_tre"),
        )
        .transform(them_ty_le_tre)
        .orderBy("thang")
        .cache()
    )

    # Câu 3: xếp hạng bang của khách hàng theo tỷ lệ giao trễ.
    theo_bang = (
        orders
        .groupBy("customer_state")
        .agg(
            F.count("*").alias("tong_don"),
            F.sum("is_late").alias("so_don_tre"),
        )
        .transform(them_ty_le_tre)
        .orderBy(F.col("ty_le_tre_phan_tram").desc())
        .cache()
    )

    # Câu 4: so sánh vận chuyển cùng bang, khác bang và không xác định.
    cung_khac_bang = (
        orders
        .withColumn(
            "pham_vi_van_chuyen",
            F.when(
                F.col("customer_seller_same_state").isNull(),
                "Khong xac dinh",
            )
            .when(F.col("customer_seller_same_state") == 1, "Cung bang")
            .when(F.col("customer_seller_same_state") == 0, "Khac bang")
            .otherwise("Khong xac dinh"),
        )
        .groupBy("pham_vi_van_chuyen")
        .agg(
            F.count("*").alias("tong_don"),
            F.sum("is_late").alias("so_don_tre"),
        )
        .transform(them_ty_le_tre)
        .orderBy(F.col("ty_le_tre_phan_tram").desc())
        .cache()
    )

    # Câu 5: phân nhóm số item để quan sát liên hệ với giao trễ.
    theo_so_san_pham = (
        orders
        .withColumn(
            "nhom_so_san_pham",
            F.when(F.col("item_count").isNull(), "Khong xac dinh")
            .when(F.col("item_count") == 1, "1 san pham")
            .when(F.col("item_count") == 2, "2 san pham")
            .when(
                F.col("item_count").between(3, 5),
                "3 den 5 san pham",
            )
            .otherwise("Tu 6 san pham"),
        )
        .groupBy("nhom_so_san_pham")
        .agg(
            F.count("*").alias("tong_don"),
            F.sum("is_late").alias("so_don_tre"),
        )
        .transform(them_ty_le_tre)
        .orderBy(F.col("ty_le_tre_phan_tram").desc())
        .cache()
    )

    # Kiểm chứng aggregation không làm mất hoặc nhân bản order.
    for ten_bang, df in [
        ("theo_thang", theo_thang),
        ("theo_bang", theo_bang),
        ("cung_khac_bang", cung_khac_bang),
        ("theo_so_san_pham", theo_so_san_pham),
    ]:
        kiem_tra_tong_nhom(ten_bang, df, tong_don, so_don_tre)
        kiem_tra_ty_le_hop_le(ten_bang, df)

    # Ghi toàn bộ bảng nhóm; biểu đồ chỉ dùng một phần đã lọc để dễ đọc.
    ghi_dataframe_csv(
        theo_thang,
        TABLE_DIR / "04_giao_tre_theo_thang.csv",
    )
    ghi_dataframe_csv(
        theo_bang,
        TABLE_DIR / "04_giao_tre_theo_bang.csv",
    )
    ghi_dataframe_csv(
        cung_khac_bang,
        TABLE_DIR / "04_giao_tre_cung_khac_bang.csv",
    )
    ghi_dataframe_csv(
        theo_so_san_pham,
        TABLE_DIR / "04_giao_tre_theo_so_san_pham.csv",
    )

    # Biểu đồ 1: ngưỡng 100 order giảm dao động từ các tháng quá ít mẫu.
    du_lieu_thang = (
        theo_thang
        .filter(F.col("tong_don") >= 100)
        .orderBy("thang")
        .collect()
    )
    plt.figure(figsize=(12, 5))
    plt.plot(
        [dong["thang"] for dong in du_lieu_thang],
        [dong["ty_le_tre_phan_tram"] for dong in du_lieu_thang],
        marker="o",
    )
    plt.title("Ty le giao tre theo thang (thang co it nhat 100 don)")
    plt.xlabel("Thang")
    plt.ylabel("Ty le giao tre (%)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(CHART_DIR / "04_giao_tre_theo_thang.png", dpi=200)
    plt.close()

    # Biểu đồ 2: ngưỡng 300 order tạo phép so sánh bang ổn định hơn.
    du_lieu_bang = (
        theo_bang
        .filter(F.col("tong_don") >= 300)
        .orderBy(F.col("ty_le_tre_phan_tram").desc())
        .limit(10)
        .orderBy(F.col("ty_le_tre_phan_tram").asc())
        .collect()
    )
    plt.figure(figsize=(9, 6))
    plt.barh(
        [dong["customer_state"] for dong in du_lieu_bang],
        [dong["ty_le_tre_phan_tram"] for dong in du_lieu_bang],
    )
    plt.title("10 bang co ty le giao tre cao (it nhat 300 don)")
    plt.xlabel("Ty le giao tre (%)")
    plt.ylabel("Bang")
    plt.tight_layout()
    plt.savefig(CHART_DIR / "04_top10_bang_giao_tre.png", dpi=200)
    plt.close()

    # Biểu đồ 3: so sánh trực tiếp ba phạm vi vận chuyển.
    du_lieu_pham_vi = cung_khac_bang.collect()
    plt.figure(figsize=(8, 5))
    plt.bar(
        [dong["pham_vi_van_chuyen"] for dong in du_lieu_pham_vi],
        [dong["ty_le_tre_phan_tram"] for dong in du_lieu_pham_vi],
    )
    plt.title("Ty le giao tre theo pham vi van chuyen")
    plt.xlabel("Pham vi van chuyen")
    plt.ylabel("Ty le giao tre (%)")
    plt.tight_layout()
    plt.savefig(CHART_DIR / "04_giao_tre_cung_khac_bang.png", dpi=200)
    plt.close()

    print("\nCAU 1 - TONG QUAN")
    print(f"Tong don: {tong_don:,}")
    print(f"Don giao tre: {so_don_tre:,}")
    print(f"Ty le giao tre: {ty_le_tre:.4f}%")
    print("\nCAU 2 - THEO THANG")
    theo_thang.show(truncate=False)
    print("\nCAU 3 - THEO BANG")
    theo_bang.filter(F.col("tong_don") >= 300).show(10, truncate=False)
    print("\nCAU 4 - CUNG BANG VA KHAC BANG")
    cung_khac_bang.show(truncate=False)
    print("\nCAU 5 - THEO SO SAN PHAM")
    theo_so_san_pham.show(truncate=False)
    print(f"\nBang ket qua: {TABLE_DIR}")
    print(f"Bieu do: {CHART_DIR}")

finally:
    # Luôn giải phóng SparkSession khi hoàn tất hoặc có lỗi assertion.
    spark.stop()
    print("Da dung SparkSession.")
