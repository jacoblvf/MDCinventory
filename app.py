import requests
import pandas as pd
import io
import streamlit as st
from datetime import date

# --- Data Fetching ---
df1 = requests.get('https://api.orcascan.com/sheets/Qe5TcdQ-md37f_8B?datetimeformat=DD/MM/YYYY HH:mm:ss&timezone=+00:00').content
df2 = requests.get('https://api.orcascan.com/sheets/L8Wpy42K0h_Mifmb?datetimeformat=DD/MM/YYYY HH:mm:ss&timezone=+00:00').content
df3 = pd.read_csv(io.StringIO(df1.decode('utf-8')))
df4 = pd.read_csv(io.StringIO(df2.decode('utf-8')))

# --- Aggregation ---
df5 = df3.groupby(["Name", "Bulk_or_Indiv"])[["Multiplier", "Scan_in"]].agg(
    Multiplier=("Multiplier", "max"), Scan_in=("Scan_in", "sum")).reset_index()

df6 = df4.groupby(["Name", "Bulk_or_Indiv"])[["Multiplier", "Scan_out"]].agg(
    Multiplier=("Multiplier", "max"), Scan_out=("Scan_out", "sum")).reset_index()

df7_2 = df5.merge(df6, on=['Name', 'Bulk_or_Indiv'], suffixes=[None, '_copy'])

df7_2["scan_qty"] = df7_2["Scan_in"] - df7_2["Scan_out"]
df7_2["indiv_qty"] = df7_2["scan_qty"] * df7_2["Multiplier"]

df8 = df7_2.groupby(["Name"])[["Bulk_or_Indiv", "indiv_qty"]].agg(
    bulkindiv=("Bulk_or_Indiv", lambda x: "Indiv"),
    qty=("indiv_qty", "sum")).reset_index()

df9 = df8.rename(columns={'bulkindiv': 'Status', 'qty': 'Product Quantity'})
df9['Product Quantity'] = df9['Product Quantity'].astype("int64")

# --- Highlight Logic ---
def highlight_low_set(row):
    if "set" in row["Name"].lower() and row["Product Quantity"] < 5:
        return ['background-color: red'] * len(row)
    else:
        return [''] * len(row)

styled_df = df9.style.apply(highlight_low_set, axis=1)

# --- Display ---
st.dataframe(styled_df, use_container_width=True)

# --- Refresh Button ---
refresh_button = st.button("Refresh")
if refresh_button:
    st.experimental_rerun()

# --- Download ---
@st.cache_data
def convert_df(df):
    return df.to_csv().encode('utf-8')

csv = convert_df(df9)

st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name=str(date.today()) + ".csv",
    mime='text/csv',
)
