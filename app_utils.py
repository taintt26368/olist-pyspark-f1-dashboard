from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import streamlit as st


PROJECT_DIR = Path(__file__).resolve().parent
TABLE_DIR = PROJECT_DIR / "outputs" / "tables"
CHART_DIR = PROJECT_DIR / "outputs" / "charts"


def _safe_child(parent: Path, name: str) -> Path:
    path = (parent / name).resolve()
    if path.parent != parent.resolve():
        raise ValueError(f"Tên file không hợp lệ: {name}")
    return path


@st.cache_data(show_spinner=False)
def load_table(name: str) -> pd.DataFrame:
    path = _safe_child(TABLE_DIR, name)
    if not path.is_file():
        raise FileNotFoundError(f"Không tìm thấy bảng bằng chứng: {path}")
    return pd.read_csv(path, encoding="utf-8-sig")


@st.cache_data(show_spinner=False)
def load_run_metadata() -> dict[str, str]:
    frame = load_table("05_run_metadata.csv")
    return dict(zip(frame["thuoc_tinh"].astype(str), frame["gia_tri"].fillna("").astype(str)))


@st.cache_data(show_spinner=False)
def load_selected_threshold() -> dict[str, object]:
    frame = load_table("05_common_threshold_duoc_chon.csv")
    if frame.empty:
        raise ValueError("Bảng common threshold không có dữ liệu.")
    return frame.iloc[0].to_dict()


def format_integer(value: object) -> str:
    return f"{int(float(value)):,}".replace(",", ".")


def format_decimal(value: object, digits: int = 4) -> str:
    return f"{float(value):.{digits}f}".replace(".", ",")


def format_percent(value: object, digits: int = 2) -> str:
    return f"{float(value) * 100:.{digits}f}%".replace(".", ",")


def page_header(title: str, description: str, icon: str) -> None:
    st.title(f":material/{icon}: {title}")
    st.caption(description)


def show_chart(name: str, caption: str | None = None) -> None:
    path = _safe_child(CHART_DIR, name)
    if not path.is_file():
        st.warning(f"Không tìm thấy biểu đồ: {name}", icon=":material/warning:")
        return
    st.image(str(path), caption=caption, width="stretch")


def render_download(path: Path, label: str, mime: str, key: str) -> None:
    if not path.is_file():
        st.caption(f"Chưa có file: {path.name}")
        return
    st.download_button(
        label,
        data=path.read_bytes(),
        file_name=path.name,
        mime=mime,
        icon=":material/download:",
        key=key,
    )


def existing_files(directory: Path, suffixes: Iterable[str]) -> list[Path]:
    allowed = {suffix.lower() for suffix in suffixes}
    return sorted(
        [path for path in directory.iterdir() if path.is_file() and path.suffix.lower() in allowed],
        key=lambda path: path.name.lower(),
    )


def result_table(dataset: str) -> pd.DataFrame:
    if dataset == "validation":
        models = load_table("05_ket_qua_validation_common_threshold.csv")
        baseline = load_table("05_ket_qua_baseline.csv").query("tap_du_lieu == 'validation'")
    elif dataset == "test":
        models = load_table("05_ket_qua_test_common_threshold.csv")
        baseline = load_table("05_ket_qua_baseline.csv").query("tap_du_lieu == 'test'")
    else:
        raise ValueError(f"Tập dữ liệu không hợp lệ: {dataset}")

    columns = [
        "model",
        "common_threshold",
        "tp",
        "tn",
        "fp",
        "fn",
        "n",
        "accuracy",
        "precision",
        "recall",
        "specificity",
        "f1",
        "auc",
        "alert_rate",
    ]
    return pd.concat([baseline[columns], models[columns]], ignore_index=True)


PERCENT_COLUMNS = {
    "accuracy": "Accuracy",
    "precision": "Precision",
    "recall": "Recall",
    "specificity": "Specificity",
    "f1": "F1",
    "alert_rate": "alert rate",
}


def metric_column_config() -> dict[str, object]:
    config: dict[str, object] = {
        "model": st.column_config.TextColumn("Model", pinned=True),
        "common_threshold": st.column_config.NumberColumn("common threshold", format="%.3f"),
        "auc": st.column_config.NumberColumn("AUC", format="%.6f"),
    }
    for column, label in PERCENT_COLUMNS.items():
        config[column] = st.column_config.NumberColumn(label, format="percent")
    return config

