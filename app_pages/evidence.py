from __future__ import annotations

from pathlib import Path

import streamlit as st

from app_utils import (
    CHART_DIR,
    PROJECT_DIR,
    TABLE_DIR,
    existing_files,
    load_run_metadata,
    load_table,
    page_header,
    render_download,
)


page_header(
    "Bằng chứng, tài liệu và source code",
    "Tải trực tiếp báo cáo, tiểu luận, slide, CSV, biểu đồ và năm chương trình PySpark.",
    "fact_check",
)

metadata = load_run_metadata()
assertions = load_table("05_assertion_checks.csv")
repro = load_table("05_reproducibility_check.csv").iloc[0]

metric_columns = [*st.columns(2), *st.columns(2)]
with metric_columns[0]:
    st.metric("Assertion PASS", int((assertions["trang_thai"] == "PASS").sum()), border=True)
with metric_columns[1]:
    st.metric("CSV bằng chứng", len(existing_files(TABLE_DIR, [".csv"])), border=True)
with metric_columns[2]:
    st.metric("Biểu đồ PNG", len(existing_files(CHART_DIR, [".png"])), border=True)
with metric_columns[3]:
    st.metric("Reproducibility", repro["trang_thai"], border=True)

st.subheader("Metadata lần chạy")
metadata_frame = load_table("05_run_metadata.csv")
st.dataframe(metadata_frame, hide_index=True)

st.subheader("Tài liệu bàn giao")
download_columns = [*st.columns(2), *st.columns(2)]
with download_columns[0]:
    render_download(
        PROJECT_DIR / "bao_cao_kiem_chung_05_F1.md",
        "Báo cáo Markdown F1",
        "text/markdown",
        "download_report",
    )
with download_columns[1]:
    render_download(
        PROJECT_DIR / "162_tieu_luan_olist_pyspark_F1.docx",
        "Tiểu luận Word F1",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "download_word",
    )
with download_columns[2]:
    render_download(
        PROJECT_DIR / "slide_thuyet_trinh_olist_F1.pptx",
        "Slide PowerPoint F1",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "download_pptx",
    )
with download_columns[3]:
    render_download(
        PROJECT_DIR / "CHANGELOG_F1.md",
        "CHANGELOG F1",
        "text/markdown",
        "download_changelog",
    )

st.subheader("Source code 01–05")
source_columns_first = st.columns(3)
for index, number in enumerate(range(1, 4)):
    with source_columns_first[index]:
        matches = sorted(PROJECT_DIR.glob(f"{number:02d}_*.py"))
        if matches:
            render_download(matches[0], f"Bước {number:02d}", "text/x-python", f"download_step_{number}")
source_columns_second = st.columns(2)
for index, number in enumerate(range(4, 6)):
    with source_columns_second[index]:
        matches = sorted(PROJECT_DIR.glob(f"{number:02d}_*.py"))
        if matches:
            render_download(matches[0], f"Bước {number:02d}", "text/x-python", f"download_step_{number}")

st.subheader("CSV và biểu đồ bằng chứng")
table_files = existing_files(TABLE_DIR, [".csv"])
chart_files = existing_files(CHART_DIR, [".png"])
left, right = st.columns(2)
with left:
    table_name = st.selectbox("Chọn bảng CSV", [path.name for path in table_files])
    selected_table = TABLE_DIR / table_name
    render_download(selected_table, "Tải bảng CSV", "text/csv", "download_selected_csv")
with right:
    chart_name = st.selectbox("Chọn biểu đồ PNG", [path.name for path in chart_files])
    selected_chart = CHART_DIR / chart_name
    render_download(selected_chart, "Tải biểu đồ PNG", "image/png", "download_selected_png")

st.subheader("Chữ ký tái lập")
st.code(repro["current_signature_sha256"], language=None)
st.success(
    "Common threshold, data split, validation/test confusion matrix, metrics và bốn order A–D khớp giữa hai lần chạy.",
    icon=":material/verified:",
)
