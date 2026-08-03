from __future__ import annotations

import streamlit as st

from app_utils import load_run_metadata


st.set_page_config(
    page_title="Olist PySpark — đánh giá mô hình giao trễ",
    page_icon=":material/analytics:",
    layout="wide",
    initial_sidebar_state="collapsed",
)

metadata = load_run_metadata()

with st.sidebar:
    st.subheader("Olist PySpark")
    st.caption("Dashboard kiểm chứng kết quả chạy chính thức dùng F1.")
    st.badge("F1 official", icon=":material/verified:", color="green")
    st.caption(f"Seed: {metadata.get('seed', '—')}")
    st.caption(f"PySpark: {metadata.get('pyspark', '—')}")
    st.caption(f"Reproducibility: {metadata.get('reproducibility', '—')}")

pages = {
    "Báo cáo": [
        st.Page("app_pages/overview.py", title="Tổng quan", icon=":material/home:"),
        st.Page(
            "app_pages/data_quality.py",
            title="Dữ liệu và kiểm chứng",
            icon=":material/database:",
        ),
        st.Page("app_pages/eda.py", title="Khám phá dữ liệu", icon=":material/query_stats:"),
    ],
    "Machine learning": [
        st.Page(
            "app_pages/threshold.py",
            title="Common threshold",
            icon=":material/tune:",
        ),
        st.Page("app_pages/models.py", title="So sánh model", icon=":material/monitoring:"),
        st.Page(
            "app_pages/demo_orders.py",
            title="Bốn order minh họa",
            icon=":material/package_2:",
        ),
    ],
    "Bàn giao": [
        st.Page(
            "app_pages/evidence.py",
            title="Bằng chứng và tải xuống",
            icon=":material/fact_check:",
        )
    ],
}

navigation = st.navigation(pages, position="top")
navigation.run()
