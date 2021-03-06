import pyodbc
import pandas as pd
import os
import glob
import numpy as np
from tqdm.notebook import tqdm
import glob2
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
import random
import urllib
import sqlalchemy as sa
from datetime import datetime
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server='tcp:mkgsupport.database.windows.net'
database='mkgsupport'
username='mkguser'
password='Usersqlmkg123!'
driver='/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.7.so.2.1'

params = urllib.parse.quote_plus("DRIVER=/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.7.so.2.1;"
                                 "SERVER=tcp:mkgsupport.database.windows.net;"
                                 "DATABASE=mkgsupport;"
                                 "UID=mkguser;"
                                 "PWD=Usersqlmkg123!")

engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))




files= glob2.glob('/datadrive/**/booking16*.csv')
files = files[33:]
print(files)
print('\n\n')

def split_dataframe(df, chunk_size = 75):
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks



def fillcountry(x):
    if x is None:
        return 'NO COUNTRY/ERROR'
    else:
        try:
            return x.split('/')[4].upper()
        except:
            return 'NO COUNTRY/ERROR'


def etl_pipe(file):

    df = pd.read_csv(file, sep = '\t')

    with open('logio.txt','a') as flog:
        length = str(len(df))
        message = 'Processing df ' + file + ' of length: '+length
        print(message)
        print(message, file = flog)
    try:
        cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()
        df = pd.read_csv(file, sep = '\t')
        df['PAYS'] = df.apply(lambda x: fillcountry(x['url']), axis=1)

        if len(df)>0:
            for a in range(len(df)):
                time.sleep(random.randint(5,15)/10)
                url=str(df.iloc[a]['url'])
                url="'"+url.replace("'",'')+"'"
                name=str(df.iloc[a]['name'])
                name=name.replace("'",'')
                description=str(df.iloc[a]['description'])
                description=description.replace("'",'')
                review=str(df.iloc[a]['review'])
                review="'"+review.replace("'",'')+"'"
                score="'"+str(df.iloc[a]['score'])+"'"
                num_reviews="'"+str(df.iloc[a]['number of reviews'])+"'"
                type_=str(df.iloc[a]['type of property'])
                type_=type_.replace("'",'')
                adrs=str(df.iloc[a]['address'])
                adrs=adrs.replace("'",'')
                star=str(df.iloc[a]['stars'])
                recommendation=str(df.iloc[a]['recommend'])
                recommendation=recommendation.replace("'",'')
                descdetail=str(df.iloc[a]['descdetail'])
                descdetail=descdetail.replace("'",'')
                equip=str(df.iloc[a]['equip'])
                equip=equip.replace("'",'')
                equipdetail=str(df.iloc[a]['equipdetail'])
                equipdetail=equipdetail.replace("'",'')
                lat="'"+str(df.iloc[a]['lat'])+"'"
                long="'"+str(df.iloc[a]['long'])+"'"
                hotelchain=str(df.iloc[a]['hotelchain'])
                hotelchain="'"+hotelchain.replace("'",'')+"'"
                restaurant=str(df.iloc[a]['restaurant'])
                restaurant=restaurant.replace("'",'')
                poi=str(df.iloc[a]['POIs'])
                poi=poi.replace("'",'')
                comment=str(df.iloc[a]['Comments'])
                comment=comment.replace("'",'')
                pays=str(df.iloc[a]['PAYS'])
                pays=pays.replace("'",'')
                query="if (select count(*) from dbo.Booking2021 WHERE URL_=%s)=0 insert into dbo.Booking2021(URL_,NAME_,DESCRIPTION_,REVIEW_,SCORE_,NUMREVIEW_,TYPE_,ADDRESSE_,STARS_,DESCDETAIL_,EQUIP_,EQUIPDETAIL_,LAT,LONG,HOTELCHAIN,RESTAURANT,POIS,COMMENTS,PAYS,RECOMMEND_) values(%s,'%s','%s',%s,%s,%s,'%s','%s','%s','%s','%s','%s',%s,%s,%s,'%s','%s','%s','%s','%s')" % (url,url,name,description,review,score,num_reviews,type_,adrs,star,descdetail,equip,equipdetail,lat,long,hotelchain,restaurant,poi,comment,pays,recommendation)
                try:
                    cursor.execute(query)
                    cnxn.commit()
                except Exception as e :
                    with open('logsql.txt','a') as flog:
                        print(e, file=flog)
                        print(query, file=flog)
    except Exception as e:
        with open('logpandas.txt','a') as flog:
            print(e, file=flog)
            print(query, file=flog)

def etl_pipe_bulk(file):
    df = pd.read_csv(file, sep = '\t')

    with open('logio.txt','a') as flog:
        length = str(len(df))
        message = 'Processing df ' + file + ' of length: '+length
        print(message)
        print(message, file = flog)

    if len(df)>0:
        df['PAYS'] = df.apply(lambda x: fillcountry(x['url']), axis=1)

        df=df.rename(columns={"url": "URL_", "name": "NAME_", "description":"DESCRIPTION_", "review":"REVIEW_", "score":"SCORE_", "number of reviews": "NUMREVIEW_", "type of property":"TYPE_", "address":"ADDRESSE_", "stars":"STARS_", "descdetail": "DESCDETAIL_", "equip":"EQUIP_", "equipdetail": "EQUIPDETAIL_", "lat": "LAT", "long":"LONG", "hotelchain":"HOTELCHAIN", "restaurant": "RESTAURANT", "POIs": "POIS", "Comments":"COMMENTS", "recommend":"RECOMMEND_"})

        lista_df = split_dataframe(df)

        for x in tqdm(lista_df):
            tries=0
            while tries<=4:
                try:
                    params = urllib.parse.quote_plus("DRIVER=/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.7.so.2.1;"
                                                     "SERVER=tcp:mkgsupport.database.windows.net;"
                                                     "DATABASE=mkgsupport;"
                                                     "UID=mkguser;"
                                                     "PWD=Usersqlmkg123!")

                    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
                    x.to_sql('Booking2021', con=engine, if_exists='append', index=False, method='multi')
                    break
                except:
                    time.sleep(2)
                    tries=+1

                    if tries==4:
                        now = datetime.now()
                        timestamp = datetime.timestamp(now)
                        timestamp = str(int(timestamp))
                        filename = '/datadrive/missed/missed_'+timestamp+'.csv'
                        x.to_csv(filename, sep = '\t', index=False)
                        break



for file in tqdm(files):
    etl_pipe_bulk(file)
