#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import requests
from io import BytesIO
import sqlite3
from pandas.tseries.offsets import BDay

# Function to download the Excel file from the FCA website
def download_fca_short_positions():
    url = "https://www.fca.org.uk/publication/data/short-positions-daily-update.xlsx"
    response = requests.get(url)
    if response.status_code == 200:
        return BytesIO(response.content)
    else:
        raise ValueError("Failed to download the data from the FCA website.")

#read the file into a pandas dataframe and format the dates
df = pd.read_excel(download_fca_short_positions(), sheet_name=1)
df['Position Date'] = pd.to_datetime(df['Position Date'], format='%d/%m/%Y')

#set dates as index
df.set_index('Position Date', inplace=True)

# Group by 'Position Holder' and 'ISIN', and resample to business days
df = df.groupby(['Position Holder', 'ISIN']).resample('B').first()

#Insert rows for dates between disclosures
df=df.drop(['Position Holder', 'ISIN'], axis=1)
df.reset_index(inplace=True)

#fill new rows with last known disclosed value up until the position is considered closed
df['Net Short Position (%)'] = df.groupby(['Position Holder', 'ISIN'])['Net Short Position (%)'].fillna(method='ffill')

# Sort the DataFrame by 'Position Holder', 'ISIN', and 'Position Date'
#df.sort_values(by=['Position Holder', 'ISIN', 'Position Date'], inplace=True)

# Group by 'ISIN' and 'Position Date' and aggregate to get total % short and total # of funds disclosed
result_df = df.groupby(['ISIN', 'Position Date']).agg({
    'Net Short Position (%)': 'sum',
    'Position Holder': 'nunique'  # Count unique funds disclosed
}).reset_index()

# Rename columns for clarity
result_df.rename(columns={
    'Net Short Position (%)': 'Total % Short',
    'Position Holder': 'Total # of Funds Disclosed'
}, inplace=True)

# Store the result_df DataFrame in a SQLite database table
database_file = 'fca_short_scrape.db'
table_name = 'short_positions_summary'

# Connect to the SQLite database
conn = sqlite3.connect(database_file)

# Store the DataFrame in the SQLite database
result_df.to_sql(table_name, conn, if_exists='replace', index=False)

# Close the database connection
conn.close()

