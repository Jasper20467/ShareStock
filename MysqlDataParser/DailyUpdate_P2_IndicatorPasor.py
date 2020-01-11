
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


"""
Ini:
    完全同步SQL Lite 的資料到 Mysql上
Step 1:
    查詢MYSQL ，取得最新的日期。
Step 2:
    以mysql 上的最新日期，作為抓資料條件，篩選出要insert到mysql 的sqllite資料
Step 3:
    上傳到Mysql
"""
logging.basicConfig(filename='Update.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

def GetLatestMysqlDate(Table):
    try:
        cnxn = pymysql.connect(host='192.168.50.39',user='jasper', passwd='jasper', db='Finlab')
        cursor = cnxn.cursor()
        LatesDate = cursor.execute("select date from %s order by date desc limit 1" % (Table))
        for row in cursor:
            LatesDate=row[0]      
    except Exception as e:
        LatesDate=datetime.datetime.today()
        logging.error(e)
    return LatesDate

    
def GetInsertDataFromSQLLite(Table,LatestDate):
    try:
        conn = sqlite3.connect(os.path.join('data', "data.db"))
        cursor = conn.execute('SELECT name FROM sqlite_master WHERE type = "table"')
        s = ("""SELECT * FROM %s where date ='%s'"""%(Table,LatestDate))
        # s = ("""SELECT date FROM %s order by date asc LIMIT 1"""%(Table))
        dates = pd.read_sql(s, conn)    
        return dates
    except Exception as e:
        logging.error(e)
        return None

def UpdateNewDataToMysql(Table):
    cnxn = pymysql.connect(host='192.168.50.39',user='jasper', passwd='jasper', db='Finlab')
    cursor = cnxn.cursor()
    for index, row in Table.iterrows():
        try:
            _date=row['date']
            _script1= "INSERT INTO Finlab.price (stock_id,date,成交筆數,成交股數,成交金額,收盤價,最低價,最後揭示買價,最後揭示買量,最後揭示賣價,最後揭示賣量,最高價,本益比,漲跌價差,開盤價)  VALUES ('%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (str(row['stock_id']),_date,row['成交筆數'],row['成交股數'],row['成交金額'],row['收盤價'],row['最低價'],row['最後揭示買價'],row['最後揭示買量'],row['最後揭示賣價'],row['最後揭示賣量'],row['最高價'],row['本益比'],row['漲跌價差'],row['開盤價'])
            _script2= "INSERT INTO Finlab.Price_Feature_Label (stock_id,date,Volumn,ClosePrice,LowPrice,HighPrice,PER,OpenPrice)  VALUES ('%s','%s',%s,%s,%s,%s,%s,%s)" % (str(row['stock_id']),_date,row['成交股數'],row['收盤價'],row['最低價'],row['最高價'],row['本益比'],row['開盤價'])

            cursor.execute(_script1.replace('nan',"null"))    
            cursor.execute(_script2.replace('nan',"null"))                                                  
            cnxn.commit()
        except Exception as e:
            logging.critical(str(e))
            logging.error(_script1)
            logging.error(_script2)


class Main:

    TableList = ['price','balance_sheet','cash_flows','income_sheet','income_sheet_cumulate','monthly_revenue']
    Table = 'price'

    ThisDay = datetime.datetime.today() 
    DatabaseDate=GetLatestMysqlDate(Table)    
    #DatabaseDate=datetime.datetime(2019,9,4)
    datespam = ThisDay - DatabaseDate
    print(str(ThisDay))
    print(str(DatabaseDate))
    print('Start')

    for d in range(1,datespam.days+1):
        Executedate = DatabaseDate+ + datetime.timedelta(days=d)
        print(str(Executedate))
        data = GetInsertDataFromSQLLite(Table,str(Executedate))
        UpdateNewDataToMysql(data)
        if(d%30==0):
            print('Month sleep......................................')
            time.sleep(180)
            
        if(d%360==0):
            print('Year sleep......................................')
            time.sleep(120)

        # UpdateNewDataToMysql(data)

    

