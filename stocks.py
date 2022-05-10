
# import cookielib
import urllib.request as urllib2
import ssl
import requests
from urllib.request import Request, urlopen
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import numpy as np
import pandas as pd
import time
from sqlalchemy import create_engine
# This restores the same behavior as before.
context = ssl._create_unverified_context()
# url=urlopen("https://www.investing.com/equities/trending-stocks", context=context)

site= "https://www.investing.com/equities/trending-stocks"
hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}

req = urllib2.Request(site, headers=hdr)
response1 = urllib2.urlopen(req,context=context)

soup = BeautifulSoup(response1,'html.parser')
        ##not required part
        #1. To Find Trending Section
raw_data = soup.find('table')
        #To fetch Header for Trending Quotes Data
header_quotes_price = pd.DataFrame(repr(soup.find('thead').get_text()).split('\\n')).T
header_quotes_price.drop(columns = [0,1,2,11,12,13])
        #To Fetch Trending Quotes Data
data_quotes_list_price = []
for quote in range(0,len(soup.select(".crossRatesTbl tr"))):
    data_quotes_list_price.append(repr(soup.select(".crossRatesTbl tr")[quote].get_text()).split('\\n'))
data_quotes_price = pd.DataFrame(data_quotes_list_price)
data_quotes_price.drop(columns = [0,1,10,11], inplace=True)
data_quotes_price.rename(columns = data_quotes_price.iloc[0], inplace=True)
data_quotes_price.drop(index=0, inplace=True)

    # data_quotes_price['Name']
df = pd.DataFrame(data=data_quotes_price)
df = df.rename(columns={' Name ': 'Name', ' Last ': 'Last',' High ': 'High', ' Low ': 'Low', ' Chg. ':'Chg', ' Chg. %':'Chg-percentage',' Vol.':'Vol.',' Time':'Time'})
df['Last'] = df['Last'].str.replace(",", '')
df['High'] = df['High'].str.replace(",", '')
df['Low'] = df['Low'].str.replace(",", '')
df['Chg'] = df['Chg'].str.replace(",", '')
df['Chg-percentage'] = df['Chg-percentage'].str.replace(",", '')
df['Chg-percentage'] = df['Chg-percentage'].str.replace("%", '')
df['Chg-percentage'] = df['Chg-percentage'].str.replace("+", '')
df['Vol.'] = df['Vol.'].str.replace(",", '')
df['Time']= pd.to_datetime(df['Time'])
df[['Last', 'High', 'Low', 'Chg', 'Chg-percentage']]=df[['Last', 'High', 'Low', 'Chg', 'Chg-percentage']].apply(pd.to_numeric)
df['Name'] = df['Name'].str.replace('""'," ")

engine = create_engine('postgresql+psycopg2://postgres:warriors@localhost:5432/stocks')

df.to_sql("stocks", engine,if_exists='append')
print("loaded into stocks db")
