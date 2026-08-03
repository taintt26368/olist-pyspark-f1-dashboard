from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from app_utils import load_table, page_header


page_header(
    "Bốn order thật A, B, C, D",
    "Các order được chọn theo quy tắc xác định từ test của model minh họa, không lấy ngẫu nhiên.",
    "package_2",
)

orders = load_table("05_demo_orders_A_B_C_D.csv")
logistic = load_table("05_logistic_score_breakdown_orders.csv")
forest = load_table("05_random_forest_score_breakdown_orders.csv")

alias = st.segmented_control("Order minh họa", orders["alias"].tolist(), default="A")
row = orders.query("alias == @alias").iloc[0]
threshold = float(row["common_threshold"])

metric_columns = [*st.columns(2), *st.columns(2)]
with metric_columns[0]:
    st.metric("Alias", alias, border=True)
with metric_columns[1]:
    st.metric("Label is_late", int(row["is_late"]), border=True)
with metric_columns[2]:
    st.metric("Kết quả Logistic Regression", row["result_logistic"], border=True)
with metric_columns[3]:
    st.metric("Kết quả Random Forest", row["result_random_forest"], border=True)

with st.container(border=True):
    st.write(f"**order_id:** `{row['order_id']}`")
    st.write(f"**Quy tắc chọn:** {row['quy_tac']}")
    st.write(f"**Vị trí so với threshold:** {row['vi_tri_so_voi_threshold']}")

probability_source = pd.DataFrame(
    {
        "Model": ["Logistic Regression", "Random Forest"],
        "Probability": [row["probability_logistic"], row["probability_random_forest"]],
        "Prediction": [row["prediction_logistic"], row["prediction_random_forest"]],
    }
)
bars = (
    alt.Chart(probability_source)
    .mark_bar()
    .encode(
        x=alt.X("Model:N", title=None),
        y=alt.Y("Probability:Q", title="Probability score", scale=alt.Scale(domain=[0, max(0.8, probability_source['Probability'].max() * 1.1)])),
        color=alt.Color("Model:N", legend=None),
        tooltip=["Model", alt.Tooltip("Probability:Q", format=".12f"), "Prediction"],
    )
)
rule = alt.Chart(pd.DataFrame({"threshold": [threshold]})).mark_rule(
    color="#D13438", strokeDash=[6, 4], strokeWidth=2
).encode(y="threshold:Q")
st.altair_chart((bars + rule).properties(height=330, title=f"Probability score và common threshold {threshold:.3f}"))

feature_columns = [
    "customer_state",
    "main_seller_state",
    "main_category",
    "item_count",
    "product_count",
    "seller_count",
    "total_price",
    "total_freight",
    "average_item_price",
    "total_weight_g",
    "total_volume_cm3",
    "freight_ratio",
    "purchase_year",
    "purchase_month",
    "purchase_day_of_week",
    "purchase_hour",
    "estimated_delivery_days",
    "customer_seller_same_state",
]
st.subheader("18 feature gốc")
feature_frame = pd.DataFrame(
    {"Feature": feature_columns, "Giá trị": [row[column] for column in feature_columns]}
)
feature_frame["Giá trị"] = feature_frame["Giá trị"].map(str)
st.dataframe(feature_frame, hide_index=True)

details = st.segmented_control(
    "Kiểm chứng score",
    ["Logistic Regression", "Random Forest"],
    default="Logistic Regression",
)
if details == "Logistic Regression":
    breakdown = logistic.query("alias == @alias").copy()
    nonzero = breakdown[breakdown["feature_value"] != 0][
        [
            "feature_index",
            "transformed_feature_name",
            "feature_value",
            "coefficient",
            "contribution",
        ]
    ]
    st.dataframe(nonzero, hide_index=True)
    check = breakdown.iloc[0]
    st.caption(
        f"probability_manual = {check['probability_manual']:.12f}; "
        f"probability_Spark = {check['probability_spark']:.12f}; "
        f"absolute_difference = {check['absolute_difference']:.3e}."
    )
else:
    breakdown = forest.query("alias == @alias").copy()
    st.dataframe(
        breakdown[
            [
                "tree_index",
                "leaf_index",
                "tree_weight",
                "tree_probability_1",
                "weighted_contribution_1",
            ]
        ],
        hide_index=True,
    )
    check = breakdown.iloc[0]
    st.caption(
        f"model rawPrediction = [{check['model_raw_prediction_0']:.12f}, "
        f"{check['model_raw_prediction_1']:.12f}]; probability_manual = "
        f"{check['probability_manual']:.12f}; probability_Spark = "
        f"{check['probability_spark']:.12f}."
    )

st.warning(
    "Probability score chưa được chứng minh là calibrated probability. Dashboard không dùng score để tự động thông báo khách hàng.",
    icon=":material/warning:",
)
