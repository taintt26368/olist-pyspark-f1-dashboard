from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from app_utils import load_table, page_header, show_chart


page_header(
    "Khám phá dữ liệu giao trễ",
    "Các phép tổng hợp được tạo ở bước 04 trên orders_enriched, trước khi huấn luyện model.",
    "query_stats",
)

overview = dict(
    zip(
        load_table("04_tong_quan_giao_tre.csv")["chi_so"],
        load_table("04_tong_quan_giao_tre.csv")["gia_tri"],
    )
)
monthly = load_table("04_giao_tre_theo_thang.csv")
states = load_table("04_giao_tre_theo_bang.csv")
same_state = load_table("04_giao_tre_cung_khac_bang.csv")
item_groups = load_table("04_giao_tre_theo_so_san_pham.csv")

metric_columns = st.columns(3)
with metric_columns[0]:
    st.metric("Tổng order", f"{int(overview['Tong so don']):,}".replace(",", "."), border=True)
with metric_columns[1]:
    st.metric("Order late", f"{int(overview['So don giao tre']):,}".replace(",", "."), border=True)
with metric_columns[2]:
    st.metric(
        "Late rate",
        f"{float(overview['Ty le giao tre phan tram']):.2f}%".replace(".", ","),
        border=True,
    )

view = st.segmented_control(
    "Chế độ xem",
    ["Tương tác", "Hình trong báo cáo"],
    default="Tương tác",
)

if view == "Tương tác":
    monthly_chart = (
        alt.Chart(monthly)
        .mark_line(point=True, color="#005A9C")
        .encode(
            x=alt.X("thang:N", title="Tháng", sort=None),
            y=alt.Y("ty_le_tre_phan_tram:Q", title="Tỷ lệ late (%)"),
            tooltip=["thang", "tong_don", "so_don_tre", "ty_le_tre_phan_tram"],
        )
        .properties(height=330, title="Tỷ lệ giao trễ theo tháng")
    )
    st.altair_chart(monthly_chart)

    left, right = st.columns(2)
    with left:
        top_states = states.sort_values("ty_le_tre_phan_tram", ascending=False).head(10)
        state_chart = (
            alt.Chart(top_states)
            .mark_bar(color="#005A9C")
            .encode(
                x=alt.X("ty_le_tre_phan_tram:Q", title="Tỷ lệ late (%)"),
                y=alt.Y("customer_state:N", title="Bang khách hàng", sort="-x"),
                tooltip=["customer_state", "tong_don", "so_don_tre", "ty_le_tre_phan_tram"],
            )
            .properties(height=320, title="Top 10 bang theo tỷ lệ late")
        )
        st.altair_chart(state_chart)
    with right:
        scope_chart = (
            alt.Chart(same_state)
            .mark_bar()
            .encode(
                x=alt.X("pham_vi_van_chuyen:N", title="Phạm vi vận chuyển"),
                y=alt.Y("ty_le_tre_phan_tram:Q", title="Tỷ lệ late (%)"),
                color=alt.Color("pham_vi_van_chuyen:N", legend=None),
                tooltip=["pham_vi_van_chuyen", "tong_don", "so_don_tre", "ty_le_tre_phan_tram"],
            )
            .properties(height=320, title="Cùng bang và khác bang")
        )
        st.altair_chart(scope_chart)

    with st.container(border=True):
        st.subheader("Theo số sản phẩm trong order")
        st.dataframe(item_groups, hide_index=True)
else:
    show_chart("04_giao_tre_theo_thang.png", "Tỷ lệ giao trễ theo tháng")
    left, right = st.columns(2)
    with left:
        show_chart("04_top10_bang_giao_tre.png", "Top 10 bang")
    with right:
        show_chart("04_giao_tre_cung_khac_bang.png", "Cùng bang và khác bang")

st.warning(
    "Các quan sát EDA mô tả tương quan trong dữ liệu; chúng không chứng minh quan hệ nhân quả.",
    icon=":material/warning:",
)
