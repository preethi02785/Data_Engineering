import pandas as pd 
import sqlite3
import requests
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime


##Extraction of information from a web page is done using the web scraping process

def extract(url,attr_list): 
    response=requests.get(url).text
    data=BeautifulSoup(response,'html.parser')
    df=pd.DataFrame(columns=attr_list)
    tables=data.find_all('tbody') 
    rows=tables[2].find_all('tr') 
    
    for row in rows:
        col=row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '-' not in col[2]:
                data_dict={'Country':col[0].a.contents[0],'GDP_USD_millions':col[2].contents[0]}
                df1=pd.DataFrame(data_dict,index=[0])
                df=pd.concat([df,df1],ignore_index=True)
    return df


## transform information 

def transform(df):
    GDP_list = df["GDP_USD_millions"].tolist()
    cleaned_GDP = []
    for x in GDP_list:
        # Remove commas, strip spaces
        val = "".join(x.split(',')).strip()
        
        # Skip or replace non-numeric values
        if val.replace('.', '', 1).isdigit():
            cleaned_GDP.append(float(val))
        else:
            cleaned_GDP.append(np.nan)   # or 0 if you prefer

    # Convert to billions
    GDP_list = [np.round(x/1000, 2) if not np.isnan(x) else np.nan for x in cleaned_GDP]
    df["GDP_USD_millions"] = GDP_list
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})
    return df


#def transform(df):
#   GDP_list = df["GDP_USD_millions"].tolist()
#   GDP_list = [float("".join(x.split(','))) for x in GDP_list]
#   GDP_list = [np.round(x/1000,2) for x in GDP_list]
#   df["GDP_USD_millions"] = GDP_list
#   df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
#   return df



## Load information
def load_csv(df,csv_path):
    df.to_csv(csv_path)

def load_to_sql(df,conn,table_name):
    df.to_sql(table_name,conn,if_exists='replace',index=False)

##Querying to database
def run_query(qstmt,conn):
    print(qstmt)
    qoutput=pd.read_sql(qstmt,conn)
    print(qoutput)


## logging progrees into txt file

def logging(msg):
    timestamp_format='%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now=datetime.now()
    timestamp= now.strftime(timestamp_format)
    with open("./etl_project_log.txt","a") as f:
        f.write(timestamp + ' : ' + msg + '\n')


url='https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
db_name='World_Economies.db'
table_name='Countries_by_GDP'
attr_list=['Country','GDP_USD_millions']
csv_path='./Countries_by_GDP.csv'

##function calls

logging('Preliminaries complete. Initiating ETL process')
df=extract(url,attr_list)

logging('Data extraction complete. Initiating Transformation process')

df=transform(df)
logging('Data transformation complete. Initiating loading process')

load_csv(df,csv_path)
logging('Data saved to CSV file')

conn=sqlite3.connect(db_name)
logging('SQL Connection initiated.')

load_to_sql(df,conn,table_name)
logging('Data loaded to Database as table. Running the query')

qstmt=f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(qstmt,conn)
logging('Process Complete.')
conn.close()

