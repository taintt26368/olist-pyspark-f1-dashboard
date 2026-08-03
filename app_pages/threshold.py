from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from app_utils import (
    format_percent,
    load_selected_threshold,
    load_table,
    page_header,
    show_chart,
)


page_header(
    "Lựa chọn common threshold bằng F1",
    "Cùng một threshold được áp dụng cho Logistic Regression và Random Forest trên validation.",
    "tune",
)

candidates = load_table("05_validation_common_thresholds.csv")
ranking = load_table("05_xep_hang_common_threshold_F1.csv")
selected = load_selected_threshold()
threshold = float(selected["common_threshold"])
best = ranking.iloc[0]

metric_columns = [*st.columns(2), *st.columns(2)]
with metric_columns[0]:
    st.metric("Candidate đã thử", len(candidates), border=True)
with metric_columns[1]:
    st.metric("Coarse tốt nhất", f"{float(selected['coarse_best_threshold']):.3f}", border=True)
with metric_columns[2]:
    st.metric("Common threshold", f"{threshold:.3f}", border=True)
with metric_columns[3]:
    st.metric("Average F1", format_percent(best["average_f1"]), border=True)

with st.container(border=True):
    st.subheader("Quy tắc xếp hạng")
    st.markdown(
        "Candidate chỉ hợp lệ khi **alert rate của cả hai model ≤ 20%**. Sau đó xếp theo: "
        "**average_f1** giảm dần → **minimum_f1** giảm dần → **average_recall** giảm dần → "
        "**average_alert_rate** tăng dần → threshold cao hơn."
    )
    st.latex(r"average\_f1 = \frac{F1_{LR} + F1_{RF}}{2}")
    st.latex(r"F1 = \frac{2\times Precision\times Recall}{Precision+Recall} = \frac{2TP}{2TP+FP+FN}")

stage = st.pills(
    "Giai đoạn candidate",
    ["coarse", "refine"],
    selection_mode="multi",
    default=["coarse", "refine"],
)
filtered = candidates[candidates["giai_do"].isin(stage)] if stage else candidates.iloc[0:0]

if filtered.empty:
    st.info("Chọn ít nhất một giai đoạn để xem biểu đồ.", icon=":material/info:")
else:
    line_source = filtered[
        ["common_threshold", "logistic_f1", "random_forest_f1", "average_f1"]
    ].melt("common_threshold", var_name="Chuỗi", value_name="F1")
    labels = {
        "logistic_f1": "Logistic Regression F1",
        "random_forest_f1": "Random Forest F1",
        "average_f1": "Average F1",
    }
    line_source["Chuỗi"] = line_source["Chuỗi"].map(labels)
    lines = (
        alt.Chart(line_source)
        .mark_line()
        .encode(
            x=alt.X("common_threshold:Q", title="Common threshold", scale=alt.Scale(zero=False)),
            y=alt.Y("F1:Q", title="F1", axis=alt.Axis(format=".0%"), scale=alt.Scale(zero=False)),
            color=alt.Color("Chuỗi:N", title=None),
            tooltip=[alt.Tooltip("common_threshold:Q", format=".3f"), "Chuỗi", alt.Tooltip("F1:Q", format=".4f")],
        )
    )
    marker = alt.Chart(pd.DataFrame({"common_threshold": [threshold]})).mark_rule(
        color="#D13438", strokeDash=[6, 4], strokeWidth=2
    ).encode(x="common_threshold:Q")
    st.altair_chart((lines + marker).properties(height=380, title="F1 trên validation theo common threshold"))

    alert_source = filtered[
        ["common_threshold", "logistic_alert_rate", "random_forest_alert_rate"]
    ].melt("common_threshold", var_name="Model", value_name="alert_rate")
    alert_source["Model"] = alert_source["Model"].map(
        {
            "logistic_alert_rate": "Logistic Regression",
            "random_forest_alert_rate": "Random Forest",
        }
    )
    alert_lines = (
        alt.Chart(alert_source)
        .mark_line()
        .encode(
            x=alt.X("common_threshold:Q", title="Common threshold", scale=alt.Scale(zero=False)),
            y=alt.Y("alert_rate:Q", title="Alert rate", axis=alt.Axis(format=".0%")),
            color=alt.Color("Model:N", title=None),
            tooltip=[alt.Tooltip("common_threshold:Q", format=".3f"), "Model", alt.Tooltip("alert_rate:Q", format=".2%")],
        )
    )
    limit = alt.Chart(pd.DataFrame({"alert_rate": [0.20]})).mark_rule(
        color="#D13438", strokeDash=[4, 4]
    ).encode(y="alert_rate:Q")
    st.altair_chart((alert_lines + limit).properties(height=300, title="Điều kiện alert rate ≤ 20%"))

st.subheader("Top 10 candidate hợp lệ")
top = ranking.head(10)[
    [
        "xep_hang",
        "giai_do",
        "common_threshold",
        "logistic_f1",
        "random_forest_f1",
        "average_f1",
        "minimum_f1",
        "average_recall",
        "average_alert_rate",
    ]
]
st.dataframe(
    top,
    hide_index=True,
    column_config={
        "common_threshold": st.column_config.NumberColumn("Common threshold", format="%.3f"),
        "logistic_f1": st.column_config.NumberColumn("LR F1", format="percent"),
        "random_forest_f1": st.column_config.NumberColumn("RF F1", format="percent"),
        "average_f1": st.column_config.NumberColumn("Average F1", format="percent"),
        "minimum_f1": st.column_config.NumberColumn("Minimum F1", format="percent"),
        "average_recall": st.column_config.NumberColumn("Average Recall", format="percent"),
        "average_alert_rate": st.column_config.NumberColumn("Average alert rate", format="percent"),
    },
)

image_expander = st.expander("Xem hình chính thức trong báo cáo", icon=":material/image:", on_change="rerun")
if image_expander.open:
    with image_expander:
        show_chart("05_common_threshold_validation.png")

st.success(
    "Common threshold được khóa từ validation trước khi đánh giá test; test không tham gia quyết định.",
    icon=":material/lock:",
)
