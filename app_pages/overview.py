from __future__ import annotations

import streamlit as st

from app_utils import (
    format_integer,
    format_percent,
    load_run_metadata,
    load_selected_threshold,
    load_table,
    page_header,
)


page_header(
    "Dự đoán nguy cơ giao trễ của Olist",
    "Toàn bộ số liệu trên dashboard được đọc từ output chạy thật của năm chương trình PySpark.",
    "analytics",
)

overview = dict(
    zip(
        load_table("04_tong_quan_giao_tre.csv")["chi_so"],
        load_table("04_tong_quan_giao_tre.csv")["gia_tri"],
    )
)
selected = load_selected_threshold()
metadata = load_run_metadata()
repro = load_table("05_reproducibility_check.csv").iloc[0]

metric_columns = [*st.columns(2), *st.columns(2)]
with metric_columns[0]:
    st.metric("Tổng số order", format_integer(overview["Tong so don"]), border=True)
with metric_columns[1]:
    st.metric("Order giao trễ", format_integer(overview["So don giao tre"]), border=True)
with metric_columns[2]:
    st.metric(
        "Prevalence",
        f"{float(overview['Ty le giao tre phan tram']):.2f}%".replace(".", ","),
        border=True,
    )
with metric_columns[3]:
    st.metric(
        "Common threshold",
        f"{float(selected['common_threshold']):.3f}".replace(".", ","),
        border=True,
    )

badge_columns = [*st.columns(2), *st.columns(2)]
with badge_columns[0]:
    st.badge("Dataset PASS", icon=":material/check_circle:", color="green")
with badge_columns[1]:
    st.badge("716 assertions PASS", icon=":material/check_circle:", color="green")
with badge_columns[2]:
    st.badge(
        f"Reproducibility {repro['trang_thai']}",
        icon=":material/replay:",
        color="blue",
    )
with badge_columns[3]:
    st.badge("Test chỉ đánh giá cuối", icon=":material/lock:", color="gray")

left, right = st.columns([1.15, 1])
with left:
    with st.container(border=True, height="stretch"):
        st.subheader("Câu hỏi nghiên cứu")
        st.write(
            "Có thể tạo một tập dữ liệu học máy hợp lệ tại thời điểm đặt hàng và dùng "
            "hai model phân lớp đơn giản để ưu tiên các order có nguy cơ giao trễ hay không?"
        )
        st.markdown(
            "**Kết quả chính:** Logistic Regression được chọn làm model minh họa vì có "
            "F1 validation cao hơn tại cùng common threshold. Score chỉ dùng để ưu tiên kiểm tra, "
            "không khẳng định order chắc chắn giao trễ."
        )
with right:
    with st.container(border=True, height="stretch"):
        st.subheader("Thông tin lần chạy")
        st.write(f"**Thời gian:** {metadata.get('thoi_gian_ket_thuc', '—')}")
        st.write(f"**Python / PySpark / Java:** {metadata.get('python', '—')} / {metadata.get('pyspark', '—')} / {metadata.get('java', '—')}")
        st.write(f"**Seed:** {metadata.get('seed', '—')}")
        st.write(f"**Model minh họa:** {metadata.get('demo_model', '—')}")
        st.write(f"**Chữ ký tái lập:** `{repro['current_signature_sha256']}`")

st.subheader("Quy trình từ dữ liệu đến đánh giá")
st.markdown(
    "**8 CSV gốc** → **làm sạch** → **orders_enriched** → **EDA** → "
    "**train_fit / validation** → **chọn common threshold bằng F1** → "
    "**retrain trên train_full** → **đánh giá test một lần**"
)

split = load_table("05_thong_tin_chia_du_lieu.csv")
split_view = split[["tap_du_lieu", "so_dong", "so_don_late", "so_don_not_late", "late_rate"]].copy()
st.dataframe(
    split_view,
    hide_index=True,
    column_config={
        "tap_du_lieu": st.column_config.TextColumn("Tập dữ liệu", pinned=True),
        "so_dong": st.column_config.NumberColumn("Số dòng", format="localized"),
        "so_don_late": st.column_config.NumberColumn("Late", format="localized"),
        "so_don_not_late": st.column_config.NumberColumn("Not late", format="localized"),
        "late_rate": st.column_config.NumberColumn("Late rate", format="percent"),
    },
)

st.info(
    "Accuracy cao của Baseline majority class không có nghĩa baseline phát hiện được order late: "
    "Baseline có Recall = 0 và F1 = 0.",
    icon=":material/info:",
)
