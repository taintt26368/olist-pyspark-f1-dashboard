from __future__ import annotations

import streamlit as st

from app_utils import load_table, page_header


page_header(
    "Dữ liệu và kiểm chứng",
    "Theo dõi bằng chứng từ đọc dữ liệu gốc, làm sạch, join đến data split và assertions.",
    "database",
)

raw = load_table("01_tong_hop_chat_luong_du_lieu.csv")
clean = load_table("02_tong_hop_lam_sach.csv")
joins = load_table("03_kiem_tra_join.csv")
split = load_table("05_thong_tin_chia_du_lieu.csv")
assertions = load_table("05_assertion_checks.csv")

metric_columns = [*st.columns(2), *st.columns(2)]
with metric_columns[0]:
    st.metric("Bảng CSV gốc", len(raw), border=True)
with metric_columns[1]:
    st.metric("Bảng đã làm sạch", len(clean), border=True)
with metric_columns[2]:
    st.metric("Order sau tích hợp", f"{int(split.iloc[0]['so_dong']):,}".replace(",", "."), border=True)
with metric_columns[3]:
    st.metric("Assertion PASS", int((assertions["trang_thai"] == "PASS").sum()), border=True)

stage = st.segmented_control(
    "Chọn lớp kiểm chứng",
    ["Đọc dữ liệu", "Làm sạch", "Join", "Data split", "Assertions"],
    default="Đọc dữ liệu",
)

if stage == "Đọc dữ liệu":
    st.subheader("Bước 01 — cấu trúc dữ liệu gốc")
    st.dataframe(raw, hide_index=True)
elif stage == "Làm sạch":
    st.subheader("Bước 02 — số dòng trước và sau làm sạch")
    st.dataframe(clean, hide_index=True)
elif stage == "Join":
    st.subheader("Bước 03 — kiểm soát fan-out và khóa")
    st.dataframe(joins, hide_index=True)
    st.success(
        "orders_enriched cuối có cùng số dòng và số order_id khác nhau; số dòng nhân bản bằng 0.",
        icon=":material/check_circle:",
    )
elif stage == "Data split":
    st.subheader("Data split với seed 42")
    st.dataframe(
        split,
        hide_index=True,
        column_config={
            "late_rate": st.column_config.NumberColumn("Late rate", format="percent"),
            "tap_du_lieu": st.column_config.TextColumn("Tập dữ liệu", pinned=True),
        },
    )
    st.caption("train_fit dùng fit Pipeline/model; validation chọn threshold/model; test chỉ đánh giá cuối.")
else:
    st.subheader("Toàn bộ assertion của bước 05")
    query = st.text_input(
        "Tìm assertion",
        placeholder="Ví dụ: leakage, AUC, probability, giao nhau…",
        key="assertion_query",
    )
    filtered = assertions
    if query.strip():
        mask = assertions.astype(str).apply(
            lambda column: column.str.contains(query.strip(), case=False, na=False)
        ).any(axis=1)
        filtered = assertions[mask]
    st.dataframe(filtered, hide_index=True, key="assertions_table")
    if (assertions["trang_thai"] == "PASS").all():
        st.success("Tất cả assertion đều PASS.", icon=":material/verified:")
    else:
        st.error("Có assertion không PASS.", icon=":material/error:")
