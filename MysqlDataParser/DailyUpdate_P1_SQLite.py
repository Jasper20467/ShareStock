import datetime
from finlab.crawler import update_table
from finlab.crawler import (
    widget, 
    
    crawl_price, 
    crawl_monthly_report, 
    crawl_finance_statement_by_date,
    
    date_range, month_range, season_range
)

import sqlite3
import os

if __name__=="__main__":

    today = datetime.date.today()
    #today = datetime.date(2019,9,5)
    print(today)
    conn = sqlite3.connect(os.path.join('data', "data.db"))

    print('Update Daily pricee:')
    #Price can update by date range (tyep:list).
    update_table(conn, 'price', crawl_price, [today])   

    print('Update Month Report:')
    # Month only update by one date(type:list), it would update month.
    update_table(conn, 'monthly_revenue', crawl_monthly_report, [today])

    print('Update Season Report:')
    update_table(conn, 'finance_statement', crawl_finance_statement_by_date, [today])


    