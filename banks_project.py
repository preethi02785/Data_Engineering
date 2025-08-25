import pandas as pd 
import numpy as np 
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import sqlite3



def extract(url, attr_list):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    df = pd.DataFrame(columns=attr_list)

    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 3:
            name = cols[1].get_text(strip=True)  # just take text
            mc_text = cols[2].get_text(strip=True).replace(',', '').replace('\n','')
            try:
                mc_val = float(mc_text)
            except:
                continue
            df = pd.concat([df, pd.DataFrame([{"Name": name, "MC_USD_Billion": mc_val}])], ignore_index=True)

    return df


def transform(df,exchange_csv_path):
    exchange_rate = pd.read_csv(exchange_csv_path, index_col=0).to_dict()['Rate']
    df['MC_GDP_Billion']=[np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion']=[np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion']=[np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df,csv_path):
    df.to_csv(csv_path)

def load_to_sql(df,conn,db_name):
    df.to_sql(table_name,conn,if_exists='replace',index=False)

def run_queires(qstmt,conn):
    print(qstmt)
    q_output=pd.read_sql(qstmt,conn)
    print(q_output)


def log_progress(msg):
    timestamp_format='%Y-%h-%d-%H:%M:%S'
    now=datetime.now()
    timestamp=now.strftime(timestamp_format)
    with open("./code_log.txt","a") as f:
        f.write(timestamp + ' : ' + msg + '\n')


url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
db_name="Banks.db"
attr_list=["Name","MC_USD_Billion"]
csv_path= "./Largest_banks_data.csv"
table_name="Largest_banks"

# Function calls

log_progress('Preliminaries complete. Initiating ETL process')

df=extract(url,attr_list)
log_progress('Data extraction complete. Initiating Transformation process')

df=transform(df,"exchange_rate.csv")
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df,csv_path)
log_progress('Data saved to CSV file')

conn=sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

f=load_to_sql(df,conn,db_name)
log_progress('Data loaded to Database as a table, Executing queries')

log_progress('Process Complete')

#1. Query 
qstmt=f"SELECT * FROM Largest_banks"
q= run_queires(qstmt,conn)
print(q)

#2. Query
qstmt=f"SELECT AVG(MC_GDP_Billion) FROM Largest_banks"
r=run_queires(qstmt,conn)
print(r)

#3. Query
qstmt=f"SELECT Name from Largest_banks LIMIT 5"
r=run_queires(qstmt,conn)
print(r)

conn.close()
log_progress('Server Connection closed')