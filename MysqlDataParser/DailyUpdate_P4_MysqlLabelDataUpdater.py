
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


def  RawDataGetByDateRange(start):
    try:
        cnxn = pymysql.connect(host='192.168.50.39',user='jasper', passwd='jasper', db='Finlab')
        end = start + datetime.timedelta(days=30)
        script = "select date,stock_id,ClosePrice from Price_Feature_Label where date between '%s' and '%s' order by date asc" % (start,end)
        # LatesDate = cursor.execute("select date,stock_id,ClosePrice from Price_Feature_Label where date between '%s' and '%s' order by date asc" % (start,end)) 
        dates = pd.read_sql(script, cnxn, parse_dates=['date']).pivot(index='date', columns='stock_id')
        logging.error(script)

    except Exception as e:
        logging.error('rawerror')
        logging.error(e)
    return dates

#type(0:1D/1:3D/2:7D)
def UpdateLabelData(data,thisday,_type):
    LabelString = 0
    if _type == 0:
        LabelString = "`Label(1)`"
    if _type == 1:
        LabelString = "`Label(3)`"
    if _type == 2:
        LabelString = "`Label(7)`"

    try:
        cnxn = pymysql.connect(host='192.168.50.39',user='jasper', passwd='jasper', db='Finlab')
        cursor = cnxn.cursor()
        cursor.execute("SET SQL_SAFE_UPDATES=0")
        cnxn.commit()
        script = "Update Finlab.Price_Feature_Label"
        script+=" set %s = CASE stock_id"% LabelString
        
        for stock in data.index:
            Label = 0
            if math.isnan(data.at[stock,'Label']):
                Label=0
            else:
                Label=data.at[stock,'Label']            
            # script = "update Finlab.Price_Feature_Label SET %s = %s  where date ='%s 00:00:00' and stock_id='%s'"%(LabelString,RSV,str(thisday),stock)
            script+="   WHEN '%s' THEN %s"%(stock,Label)
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

def LabelDataPreProcess(rawdata,_type):
    LabelDay =0
    if _type == 0:
        LabelDay =1
    if _type ==1:
        LabelDay=3
    if _type==2:
        LabelDay=7
    try:
        now = rawdata.iloc[0]
        typeday = rawdata.iloc[0+LabelDay]
        result = (typeday-now)/now   
        _result= pd.DataFrame(result)
        _result.reset_index(inplace=True)
        _result.set_index('stock_id',inplace=True)
        Output=_result.drop(['level_0'],axis=1)
        Output.columns = ['Label']
        return Output
    except Exception as e:
        logging.error(e)
        return None

class Main:
    # StartDate = datetime.date.today()+datetime.timedelta(days=-40)
    EndDate = datetime.date.today()+datetime.timedelta(days=-14)
    StartDate = datetime.date(2019,10,10)
    datespam = EndDate-StartDate
    print('start date:%s'% (str(StartDate)))
    print('end date:%s'%(str(EndDate)))
    print('Start.......')

    for _days in range(0,int(datespam.days)+1):
        thisday = StartDate + datetime.timedelta(days=_days)
        RawData = RawDataGetByDateRange(thisday)
        for _type in range(0,3):
            RSVData = LabelDataPreProcess(RawData,_type)
            Result = UpdateLabelData(RSVData,thisday,_type)
            print("%s Type: %s parse %s!"%(thisday,_type,Result))
            logging.info("%s Type: %s parse %s!"%(thisday,_type,Result))
        if(_days%30==0):
            print('Month sleep......................................')
            time.sleep(120)            



