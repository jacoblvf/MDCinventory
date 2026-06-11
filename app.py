import pandas as pd
import streamlit as st
from datetime import datetime

# -----------------------------------
# Page Setup
# -----------------------------------
st.set_page_config(
    page_title="Inventory Dashboard",
    layout="wide"
)

st.title("Inventory Dashboard")
st.caption("Live inventory from Google Sheets")

# -----------------------------------
# Google Sheet CSV URL
# -----------------------------------
CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQs2y-IXY9MBC4h4VWHds7Wua3QdBqhG-S-ifazVK1Zrl5fWaqaOJV1RLlCfJe51mbvbcNlOA2nH5Lh/pub?gid=1732985597&single=true&output=csv"

# -----------------------------------
# Load Data
# -----------------------------------
@st.cache_data(ttl=60)
def load_data():
    return pd.read_csv(CSV_URL)

try:
    df = load_data()
except Exception as e:
    st.error(f"Failed to load inventory data: {e}")
    st.stop()

# -----------------------------------
# Keep Only Name and Quantity
# -----------------------------------
required_columns = ["Name", "Quantity"]

if not all(col in df.columns for col in required_columns):
    st.error(
        f"Expected columns: {required_columns}. Found: {list(df.columns)}"
    )
    st.stop()

df = df[["Name", "Quantity"]].copy()

df["Quantity"] = pd.to_numeric(
    df["Quantity"],
    errors="coerce"
).fillna(0).astype(int)

# Sort lowest quantity first
df = df.sort_values(
    by="Quantity",
    ascending=True
)

# -----------------------------------
# Metrics
# -----------------------------------
total_items = len(df)
low_stock_items = len(df[df["Quantity"] < 5])
negative_stock_items = len(df[df["Quantity"] < 0])

col1, col2, col3 = st.columns(3)

col1.metric(
    "Total Sets",
    total_items
)

col2.metric(
    "Low Stock Sets",
    low_stock_items
)

col3.metric(
    "Negative Stock",
    negative_stock_items
)

# -----------------------------------
# Highlighting
# -----------------------------------
def highlight_quantity(row):

    styles = [""] * len(row)

    qty_col = row.index.get_loc("Quantity")
    qty = row["Quantity"]

    if qty < 0:
        styles[qty_col] = (
            "background-color:#7f1d1d;"
            "color:white;"
            "font-weight:bold;"
        )

    elif qty < 5:
        styles[qty_col] = (
            "background-color:#dc2626;"
            "color:white;"
            "font-weight:bold;"
        )

    return styles

styled_df = df.style.apply(
    highlight_quantity,
    axis=1
)

# -----------------------------------
# Inventory Table
# -----------------------------------
st.subheader("Current Set Inventory")

st.dataframe(
    styled_df,
    use_container_width=True,
    height=700
)

# -----------------------------------
# Warning Message
# -----------------------------------
if negative_stock_items > 0:
    st.warning(
        "Some items have negative stock. Please verify scan-in and scan-out records."
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
csv = df.to_csv(index=False).encode("utf-8")

st.download_button(
    label="Download Inventory CSV",
    data=csv,
    file_name="inventory.csv",
    mime="text/csv"
)

# -----------------------------------
# Last Updated
# -----------------------------------
st.caption(
    f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
)
