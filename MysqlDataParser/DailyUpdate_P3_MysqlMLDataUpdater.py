
import sys
sys.setrecursionlimit(5000)
from io import StringIO
import pandas as pd
import numpy as np
import datetime
import time
import sqlite3
import os
import logging
import pymysql
import math

logging.basicConfig(filename='Update.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')


def  RawDataGetByDateRange(end):
    try:
        cnxn = pymysql.connect(host='192.168.50.39',user='jasper', passwd='jasper', db='Finlab')
        cursor = cnxn.cursor()
        start = end + datetime.timedelta(days=-365)
        script = "select date,stock_id,ClosePrice from Price_Feature_Label where date between '%s' and '%s' order by date asc" % (start,end)
        # LatesDate = cursor.execute("select date,stock_id,ClosePrice from Price_Feature_Label where date between '%s' and '%s' order by date asc" % (start,end)) 
        dates = pd.read_sql(script, cnxn, parse_dates=['date']).pivot(index='date', columns='stock_id')
        logging.error(script)

    except Exception as e:
        LatesDate=datetime.date.today()
        logging.error('rawerror')
        logging.error(e)
    return dates

#type(0:Year/1:Season/2:Month/3:2Weeks/4:week)
def UpdateRSVData(data,thisday,_type):
    RSVString = 0
    if _type == 0:
        RSVString = "`RSV(Year)`"
    if _type == 1:
        RSVString = "`RSV(Season)`"
    if _type == 2:
        RSVString = "`RSV(Month)`"
    if _type == 3:
        RSVString = "`RSV(2Weeks)`"
    if _type == 4:
        RSVString = "`RSV(Week)`"
    try:
        cnxn = pymysql.connect(host='192.168.50.39',user='jasper', passwd='jasper', db='Finlab')
        cursor = cnxn.cursor()
        cursor.execute("SET SQL_SAFE_UPDATES=0")
        cnxn.commit()
        script = "Update Finlab.Price_Feature_Label"
        script+=" set %s = CASE stock_id"% RSVString
        
        for stock in data.index:
            RSV = 0
            if math.isnan(data.at[stock,'RSV']):
                RSV=0.5
            else:
                RSV=data.at[stock,'RSV']            
            # script = "update Finlab.Price_Feature_Label SET %s = %s  where date ='%s 00:00:00' and stock_id='%s'"%(RSVString,RSV,str(thisday),stock)
            script+="   WHEN '%s' THEN %s"%(stock,RSV)
        script+="  END  Where date ='%s 00:00:00'"% thisday
        cursor.execute(script)
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return "PASS"
    except Exception as e:
        logging.error(e)
        logging.error('rawerror')

        return "Fail"

def RSVDataPreProcess(rawdata,_type):
    RSVRange = 0
    if _type == 0:
        RSVRange = -240
    if _type == 1:
        RSVRange = -60
    if _type == 2:
        RSVRange = -20
    if _type == 3:
        RSVRange = -10
    if _type == 4:
        RSVRange = -5
    try:
        rsv = (rawdata.iloc[-1] - rawdata.iloc[RSVRange:].min()) / (rawdata.iloc[RSVRange:].max() - rawdata.iloc[RSVRange:].min())
        _rsv= pd.DataFrame(rsv)
        _rsv.reset_index(inplace=True)
        _rsv.set_index('stock_id',inplace=True)
        Output=_rsv.drop(['level_0'],axis=1)
        Output.columns = ['RSV']
        return Output
    except Exception as e:
        logging.error(e)      
        return None



class Main:
    # StartDate = datetime.date(2019,10,1)
    EndDate = datetime.date.today()
    StartDate = datetime.date(EndDate.year,EndDate.month,1)
    datespam = EndDate-StartDate
    print('start date:%s'% (str(StartDate)))
    print('end date:%s'%(str(EndDate)))
    print('Start.......')

    for _days in range(0,int(datespam.days)+1):
        thisday = StartDate + datetime.timedelta(days=_days)
        RawData = RawDataGetByDateRange(thisday)
        for _type in range(0,5):
            RSVData = RSVDataPreProcess(RawData,_type)
            Result = UpdateRSVData(RSVData,thisday,_type)
            print("%s Type: %s parse %s!"%(thisday,_type,Result))
            logging.info("%s Type: %s parse %s!"%(thisday,_type,Result))
        if(_days%30==0):
            print('Month sleep......................................')
            time.sleep(240)            



