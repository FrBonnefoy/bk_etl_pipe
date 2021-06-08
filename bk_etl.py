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
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server='tcp:mkgsupport.database.windows.net'
database='mkgsupport'
username='mkguser'
password='Usersqlmkg123!'
driver='/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.7.so.2.1'


files= glob2.glob('/datadrive/**/booking16*.csv')


def etl_pipe(file):
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    df = pd.read_csv(file, sep = '\t')
    df['PAYS'] = df.apply(lambda x: x['url'].split('/')[4].upper(), axis=1)

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

with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
    	future_to_url = {executor.submit(etl_pipe, file): file for file in files}
    	for future in tqdm(concurrent.futures.as_completed(future_to_url),total=len(files)):
    		url = future_to_url[future]
    		try:
    			data = future.result()
    		except Exception as exc:
    			with open('exception.txt',"a") as flog:
    				print('%r generated an exception: %s' % (url, exc),file=flog)
    		else:
    			with open('completed.txt',"a") as flog:
    				print('%r page is completed' % url,file=flog)
