import requests
import pandas as pd
import io
import streamlit as st
from datetime import date

# -----------------------------------
# Page Setup
# -----------------------------------
st.set_page_config(page_title="Inventory Dashboard", layout="wide")
st.title("Inventory Dashboard")

# -----------------------------------
# OrcaScan URLs
# -----------------------------------
SCAN_IN_URL = "https://api.orcascan.com/sheets/Qe5TcdQ-md37f_8B?datetimeformat=DD/MM/YYYY HH:mm:ss&timezone=+00:00"
SCAN_OUT_URL = "https://api.orcascan.com/sheets/L8Wpy42K0h_Mifmb?datetimeformat=DD/MM/YYYY HH:mm:ss&timezone=+00:00"

# -----------------------------------
# Fetch Data
# -----------------------------------
@st.cache_data(ttl=60)
def fetch_csv(url):
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return pd.read_csv(io.StringIO(response.content.decode("utf-8")))

try:
    df_in_raw = fetch_csv(SCAN_IN_URL)
    df_out_raw = fetch_csv(SCAN_OUT_URL)
except Exception as e:
    st.error(f"Failed to fetch inventory data: {e}")
    st.stop()

# -----------------------------------
# Validate Required Columns
# -----------------------------------
required_in_cols = {"Name", "Bulk_or_Indiv", "Multiplier", "Scan_in"}
required_out_cols = {"Name", "Bulk_or_Indiv", "Multiplier", "Scan_out"}

if not required_in_cols.issubset(df_in_raw.columns):
    st.error(f"Missing columns in Scan In sheet: {required_in_cols - set(df_in_raw.columns)}")
    st.stop()

if not required_out_cols.issubset(df_out_raw.columns):
    st.error(f"Missing columns in Scan Out sheet: {required_out_cols - set(df_out_raw.columns)}")
    st.stop()

# -----------------------------------
# Clean Data
# -----------------------------------
df_in_raw["Multiplier"] = pd.to_numeric(
    df_in_raw["Multiplier"], errors="coerce"
).fillna(1)

df_in_raw["Scan_in"] = pd.to_numeric(
    df_in_raw["Scan_in"], errors="coerce"
).fillna(0)

df_out_raw["Multiplier"] = pd.to_numeric(
    df_out_raw["Multiplier"], errors="coerce"
).fillna(1)

df_out_raw["Scan_out"] = pd.to_numeric(
    df_out_raw["Scan_out"], errors="coerce"
).fillna(0)

# -----------------------------------
# Aggregate Scan In
# -----------------------------------
df_in = (
    df_in_raw
    .groupby(["Name", "Bulk_or_Indiv"], dropna=False)
    .agg(
        Multiplier_in=("Multiplier", "max"),
        Scan_in=("Scan_in", "sum")
    )
    .reset_index()
)

# -----------------------------------
# Aggregate Scan Out
# -----------------------------------
df_out = (
    df_out_raw
    .groupby(["Name", "Bulk_or_Indiv"], dropna=False)
    .agg(
        Multiplier_out=("Multiplier", "max"),
        Scan_out=("Scan_out", "sum")
    )
    .reset_index()
)

# -----------------------------------
# Merge
# -----------------------------------
df_merged = df_in.merge(
    df_out,
    on=["Name", "Bulk_or_Indiv"],
    how="outer"
)

df_merged["Scan_in"] = df_merged["Scan_in"].fillna(0)
df_merged["Scan_out"] = df_merged["Scan_out"].fillna(0)

df_merged["Multiplier"] = (
    df_merged["Multiplier_in"]
    .fillna(df_merged["Multiplier_out"])
    .fillna(1)
)

# -----------------------------------
# Calculate Inventory
# -----------------------------------
df_merged["scan_qty"] = (
    df_merged["Scan_in"] - df_merged["Scan_out"]
)

df_merged["indiv_qty"] = (
    df_merged["scan_qty"] * df_merged["Multiplier"]
)

# -----------------------------------
# Final Inventory Table
# -----------------------------------
df_final = (
    df_merged
    .groupby("Name", dropna=False)
    .agg(
        Status=("Bulk_or_Indiv", lambda x: "Indiv"),
        Product_Quantity=("indiv_qty", "sum")
    )
    .reset_index()
)

df_final["Product Quantity"] = (
    df_final["Product_Quantity"]
    .round()
    .astype("int64")
)

df_final.drop(columns=["Product_Quantity"], inplace=True)

# Sort low stock to top
df_final = df_final.sort_values(
    by="Product Quantity",
    ascending=True
)

# -----------------------------------
# Highlighting
# -----------------------------------
def highlight_low_stock(row):

    styles = [""] * len(row)

    qty = row["Product Quantity"]
    name = str(row["Name"]).lower()

    qty_col = row.index.get_loc("Product Quantity")

    # Critical Low Stock Sets
    if "set" in name and qty < 5:
        styles[qty_col] = (
            "background-color: #dc2626; "
            "color: white; "
            "font-weight: bold"
        )

    # Low Stock Warning
    elif qty < 10:
        styles[qty_col] = (
            "background-color: #f59e0b; "
            "color: black; "
            "font-weight: bold"
        )

    return styles

styled_df = df_final.style.apply(
    highlight_low_stock,
    axis=1
)

# -----------------------------------
# Dashboard Metrics
# -----------------------------------
total_products = len(df_final)

low_stock_sets = df_final[
    df_final["Name"].str.lower().str.contains("set", na=False)
    & (df_final["Product Quantity"] < 5)
].shape[0]

negative_stock = df_final[
    df_final["Product Quantity"] < 0
].shape[0]

col1, col2, col3 = st.columns(3)

col1.metric(
    "Total Products",
    total_products
)

col2.metric(
    "Low Stock Sets",
    low_stock_sets
)

col3.metric(
    "Negative Stock Items",
    negative_stock
)

# -----------------------------------
# Inventory Table
# -----------------------------------
st.subheader("Current Inventory")

st.dataframe(
    styled_df,
    use_container_width=True,
    height=700
)

# -----------------------------------
# Negative Stock Warning
# -----------------------------------
if negative_stock > 0:
    st.warning(
        "Some items have negative stock. Please check for scan-out errors."
    )

# -----------------------------------
# Refresh Button
# -----------------------------------
if st.button("Refresh"):
    st.cache_data.clear()
    st.rerun()

# -----------------------------------
# Download CSV
# -----------------------------------
@st.cache_data
def convert_df(df):
    return df.to_csv(
        index=False
    ).encode("utf-8")

csv = convert_df(df_final)

st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name=f"{date.today()}_inventory.csv",
    mime="text/csv"
)
