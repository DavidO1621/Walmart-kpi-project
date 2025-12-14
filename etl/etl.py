import pandas as pd
import sqlite3
import os

RAW_DIR = "data/raw"
DB_PATH = "db/walmart.db"

os.makedirs("db", exist_ok =True)

print("Loading CSV files...")
train= pd.read_csv(f"{RAW_DIR}/train.csv")
stores = pd.read_csv(f"{RAW_DIR}/stores.csv")
features = pd.read_csv(f"{RAW_DIR}/features.csv")

train['Date']= pd.to_datetime(train['Date'])
features['Date'] = pd.to_datetime(features['Date'])

df = train.merge(stores , on='Store', how ='left')
df = df.merge(features, on =['Store', 'Date','IsHoliday'], how = 'left')

#lets drop rows that have missing sales data
df= df.dropna(subset=['Weekly_Sales'])

print("Final row count",len(df))

#save and take it to sqlite db
conn = sqlite3.connect(DB_PATH)
df.to_sql("walmart_sales", conn, if_exists="replace", index =False)
conn.close()

print("Database saved!:", DB_PATH)
