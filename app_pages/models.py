from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from app_utils import metric_column_config, page_header, result_table, show_chart


page_header(
    "So sánh Baseline, Logistic Regression và Random Forest",
    "Hai model được đánh giá tại cùng common threshold; Baseline majority class không tham gia chọn threshold.",
    "monitoring",
)

dataset_label = st.segmented_control(
    "Tập đánh giá",
    ["Validation", "Test"],
    default="Test",
)
dataset = dataset_label.lower()
results = result_table(dataset)

st.dataframe(
    results,
    hide_index=True,
    column_config=metric_column_config(),
    key=f"model_results_{dataset}",
)

chart_metrics = ["accuracy", "precision", "recall", "f1", "auc", "alert_rate"]
chart_source = results[["model", *chart_metrics]].melt(
    "model", var_name="Metric", value_name="Giá trị"
)
labels = {
    "accuracy": "Accuracy",
    "precision": "Precision",
    "recall": "Recall",
    "f1": "F1",
    "auc": "AUC",
    "alert_rate": "Alert rate",
}
chart_source["Metric"] = chart_source["Metric"].map(labels)
comparison = (
    alt.Chart(chart_source)
    .mark_bar()
    .encode(
        x=alt.X("Metric:N", title=None),
        y=alt.Y("Giá trị:Q", title="Giá trị", axis=alt.Axis(format=".0%")),
        color=alt.Color("model:N", title="Phương pháp"),
        xOffset="model:N",
        tooltip=["model", "Metric", alt.Tooltip("Giá trị:Q", format=".4f")],
    )
    .properties(height=380, title=f"So sánh metric trên {dataset}")
)
st.altair_chart(comparison)

if dataset == "test":
    lr = results.query("model == 'Logistic Regression'").iloc[0]
    rf = results.query("model == 'Random Forest'").iloc[0]
    metric_columns = [*st.columns(2), *st.columns(2)]
    with metric_columns[0]:
        st.metric("LR F1", f"{lr['f1'] * 100:.2f}%", border=True)
    with metric_columns[1]:
        st.metric("RF F1", f"{rf['f1'] * 100:.2f}%", border=True)
    with metric_columns[2]:
        st.metric("LR AUC", f"{lr['auc']:.6f}", border=True)
    with metric_columns[3]:
        st.metric("RF AUC", f"{rf['auc']:.6f}", border=True)

    st.subheader("Confusion matrix trên test")
    baseline_col, lr_col, rf_col = st.columns(3)
    with baseline_col:
        show_chart("05_confusion_matrix_baseline.png", "Baseline majority class")
    with lr_col:
        show_chart("05_confusion_matrix_logistic_regression.png", "Logistic Regression")
    with rf_col:
        show_chart("05_confusion_matrix_random_forest.png", "Random Forest")

    st.subheader("ROC curve trên test")
    show_chart("05_roc_curve_test.png")
    st.info(
        "AUC của Baseline bằng 0,5. AUC Logistic Regression = 0,701896 và Random Forest = 0,687284; "
        "AUC không phụ thuộc common threshold cụ thể.",
        icon=":material/info:",
    )
else:
    st.success(
        "Logistic Regression được chọn làm model minh họa vì F1 validation cao hơn Random Forest tại threshold 0,094.",
        icon=":material/check_circle:",
    )

st.warning(
    "Dữ liệu mất cân bằng làm Accuracy của Baseline trông cao dù Recall và F1 đều bằng 0. "
    "Vì vậy không được dùng Accuracy đơn lẻ để kết luận.",
    icon=":material/warning:",
)
