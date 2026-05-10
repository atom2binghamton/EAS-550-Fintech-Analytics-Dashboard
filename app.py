import os
from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text


st.set_page_config(
    page_title="Fintech Analytics Dashboard",
    page_icon="💳",
    layout="wide",
)


st.title("Fintech Analytics Dashboard")
st.write(
    "This dashboard shows interactive financial transaction insights from the live Neon PostgreSQL database."
)


# -------------------------------------------------------------------
# Database connection
# -------------------------------------------------------------------
@st.cache_resource
def get_engine():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        st.error(
            "DATABASE_URL is not set. Add it as an environment variable locally or in Render."
        )
        st.stop()

    # pool_pre_ping helps keep the connection stable after Render cold starts.
    return create_engine(database_url, pool_pre_ping=True)


# -------------------------------------------------------------------
# Load live database data
# -------------------------------------------------------------------
@st.cache_data(ttl=600)
def load_transaction_data():
    query = """
        SELECT
            t.TransactionID,
            t.TransactionDate,
            t.TransactionAmount,
            t.TransactionType,
            t.TransactionChannel,
            t.Status AS TransactionStatus,
            a.AccountType,
            c.Region,
            c.Status AS CustomerStatus,
            p.ProductName,
            ps.ProductSubCategoryName,
            pc.ProductCategoryName
        FROM transactions t
        JOIN accounts a
            ON t.AccountID = a.AccountID
        JOIN customers c
            ON a.CustomerID = c.CustomerID
        JOIN products p
            ON t.ProductID = p.ProductID
        JOIN product_subcategories ps
            ON p.ProductSubcategoryID = ps.ProductSubCategoryID
        JOIN product_categories pc
            ON ps.ProductCategoryID = pc.ProductCategoryID
        ORDER BY t.TransactionDate DESC;
    """

    engine = get_engine()
    return pd.read_sql(text(query), engine)


try:
    df = load_transaction_data()
except Exception as error:
    st.error("Could not load data from the database.")
    st.exception(error)
    st.stop()


if df.empty:
    st.warning("No transaction data found in the database.")
    st.stop()


# -------------------------------------------------------------------
# Data preparation
# -------------------------------------------------------------------
df["transactiondate"] = pd.to_datetime(df["transactiondate"])
df["transactionamount"] = pd.to_numeric(df["transactionamount"], errors="coerce")
df["month"] = df["transactiondate"].dt.to_period("M").astype(str)

min_date = df["transactiondate"].dt.date.min()
max_date = df["transactiondate"].dt.date.max()


# -------------------------------------------------------------------
# Sidebar filters
# -------------------------------------------------------------------
st.sidebar.header("Dashboard Filters")

selected_date_range = st.sidebar.date_input(
    "Select date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

category_options = ["All"] + sorted(df["productcategoryname"].dropna().unique().tolist())
selected_category = st.sidebar.selectbox(
    "Select product category",
    category_options,
)

account_type_options = ["All"] + sorted(df["accounttype"].dropna().unique().tolist())
selected_account_type = st.sidebar.selectbox(
    "Select account type",
    account_type_options,
)

channel_options = ["All"] + sorted(df["transactionchannel"].dropna().unique().tolist())
selected_channel = st.sidebar.selectbox(
    "Select transaction channel",
    channel_options,
)


# -------------------------------------------------------------------
# Apply filters
# -------------------------------------------------------------------
filtered_df = df.copy()

if len(selected_date_range) == 2:
    start_date, end_date = selected_date_range
    filtered_df = filtered_df[
        (filtered_df["transactiondate"].dt.date >= start_date)
        & (filtered_df["transactiondate"].dt.date <= end_date)
    ]

if selected_category != "All":
    filtered_df = filtered_df[
        filtered_df["productcategoryname"] == selected_category
    ]

if selected_account_type != "All":
    filtered_df = filtered_df[
        filtered_df["accounttype"] == selected_account_type
    ]

if selected_channel != "All":
    filtered_df = filtered_df[
        filtered_df["transactionchannel"] == selected_channel
    ]


# -------------------------------------------------------------------
# KPI cards
# -------------------------------------------------------------------
total_transactions = len(filtered_df)
total_amount = filtered_df["transactionamount"].sum()
average_amount = filtered_df["transactionamount"].mean() if total_transactions > 0 else 0

col1, col2, col3 = st.columns(3)

col1.metric("Total Transactions", f"{total_transactions:,}")
col2.metric("Total Transaction Amount", f"${total_amount:,.2f}")
col3.metric("Average Transaction Amount", f"${average_amount:,.2f}")


st.divider()


# -------------------------------------------------------------------
# Visualizations
# -------------------------------------------------------------------
if filtered_df.empty:
    st.warning("No data matches the selected filters.")
    st.stop()


monthly_df = (
    filtered_df.groupby("month", as_index=False)["transactionamount"]
    .sum()
    .rename(columns={"transactionamount": "Total Amount"})
)

category_df = (
    filtered_df.groupby("productcategoryname", as_index=False)["transactionamount"]
    .sum()
    .rename(
        columns={
            "productcategoryname": "Product Category",
            "transactionamount": "Total Amount",
        }
    )
    .sort_values("Total Amount", ascending=False)
)

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Monthly Transaction Amount")
    fig_monthly = px.line(
        monthly_df,
        x="month",
        y="Total Amount",
        markers=True,
        title="Monthly Transaction Amount",
    )
    fig_monthly.update_layout(xaxis_title="Month", yaxis_title="Total Amount")
    st.plotly_chart(fig_monthly, use_container_width=True)

with chart_col2:
    st.subheader("Transaction Amount by Product Category")
    fig_category = px.bar(
        category_df,
        x="Product Category",
        y="Total Amount",
        title="Transaction Amount by Product Category",
    )
    fig_category.update_layout(
        xaxis_title="Product Category",
        yaxis_title="Total Amount",
    )
    st.plotly_chart(fig_category, use_container_width=True)


st.divider()


# -------------------------------------------------------------------
# Extra dashboard section
# -------------------------------------------------------------------
st.subheader("Transaction Count by Channel")

channel_df = (
    filtered_df.groupby("transactionchannel", as_index=False)["transactionid"]
    .count()
    .rename(
        columns={
            "transactionchannel": "Transaction Channel",
            "transactionid": "Transaction Count",
        }
    )
    .sort_values("Transaction Count", ascending=False)
)

fig_channel = px.pie(
    channel_df,
    names="Transaction Channel",
    values="Transaction Count",
    title="Transaction Count by Channel",
)
st.plotly_chart(fig_channel, use_container_width=True)


# -------------------------------------------------------------------
# Data table
# -------------------------------------------------------------------
st.subheader("Recent Transactions")

display_columns = [
    "transactionid",
    "transactiondate",
    "transactionamount",
    "transactiontype",
    "transactionchannel",
    "productcategoryname",
    "productname",
    "accounttype",
    "region",
    "transactionstatus",
]

st.dataframe(
    filtered_df[display_columns]
    .sort_values("transactiondate", ascending=False)
    .head(100),
    use_container_width=True,
)

st.caption(
    "Data is loaded dynamically from the Neon PostgreSQL database. No CSV files are used by this dashboard."
)
