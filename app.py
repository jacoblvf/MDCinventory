import requests
import pandas as pd
import io
import streamlit as st
from datetime import date
from io import StringIO

df1 = requests.get('https://api.orcascan.com/sheets/Qe5TcdQ-md37f_8B?datetimeformat=DD/MM/YYYY HH:mm:ss&timezone=+00:00').content
df2 = requests.get('https://api.orcascan.com/sheets/L8Wpy42K0h_Mifmb?datetimeformat=DD/MM/YYYY HH:mm:ss&timezone=+00:00').content
df3 = pd.read_csv(io.StringIO(df1.decode('utf-8')))
df4 = pd.read_csv(io.StringIO(df2.decode('utf-8')))

df3 = df3.groupby(["Name", "Bulk_or_Indiv"])[["Multiplier", "Scan_in"]].agg(Multiplier = ("Multiplier", "max"), Scan_in = ("Scan_in", "sum"))
df4 = df4.groupby(["Name", "Bulk_or_Indiv"])[["Multiplier", "Scan_out"]].agg(Multiplier = ("Multiplier", "max"), Scan_out = ("Scan_out", "sum"))

df3 = df3.reset_index()
df4 = df4.reset_index()

df5_2 = df3.merge(df4, on=['Name', 'Bulk_or_Indiv'], suffixes=[None, '_copy'])
df3 = df3.sort_values(by='Name', ascending=False)
df4 = df4.sort_values(by='Name', ascending=False)
df5 = pd.concat([df3,df4["Scan_out"]], axis=1)
df5=df5_2

df5["scan_qty"] = df5["Scan_in"] - df5["Scan_out"]
df5["indiv_qty"] = df5["scan_qty"]*df5["Multiplier"]
df6 = df5.groupby(["Name"])[["Bulk_or_Indiv", "indiv_qty"]].agg(bulkindiv = ("Bulk_or_Indiv", lambda x:"Indiv"), qty = ("indiv_qty", "sum"))

df7 = df6.rename(columns={'bulkindiv': 'Status', 'qty': 'Product Quantity'})
df7['Product Quantity'] = df7['Product Quantity'].astype("int64")

st.dataframe(df7, width=1000, height=600)


refresh_button = st.button("Refresh")
if refresh_button:
    st.experimental_rerun()
    
@st.cache_data
def convert_df(df7):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df7.to_csv().encode('utf-8')

csv = convert_df(df7=df7)

st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name=str(date.today()) + ".csv",
    mime='text/csv',
)

df8 = requests.get('https://docs.google.com/spreadsheets/d/12rvTfpgODsT2R1tf-wBNJN0MVfs2M7AtHz93eA7L-9U/export?format=csv&gid=768037178').content

df9 = pd.read_csv(io.StringIO(df8.decode('utf-8')))
df10 = df6.merge(df9, on=['Name'], suffixes=[None, '_copy'])
df10['qty'] = df10['qty'].astype("int64")
df10['Cost'] = df10['Cost'].str.replace('$', '')
df10['Cost'] = df10['Cost'].astype(float)

df10['Total Stock Price'] = df10['qty'] * df10['Cost']

def convert_df_to_csv(df10):
    csv_buffer = StringIO()
    df10.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()

PASSCODE = "247123"

# Streamlit app layout
st.subheader("Passcode Protected CSV Download")


# Passcode input
input_passcode = st.text_input("Enter passcode", type="password")

# Verify passcode
if input_passcode:
    if input_passcode == PASSCODE:
        st.success("Passcode correct! You can download the CSV file now.")
        
        # Convert DataFrame to CSV
        csv = convert_df_to_csv(df10)
        
        # Provide download link
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=str(date.today()) + "Stocklist.csv",
            mime='text/csv',
        )
    else:
        st.error("Incorrect passcode. Please try again.")