import pandas as pd
import numpy as np
import datetime

class TechAnalyze():
    def __init__(self,_sdate,_edate):
        self.sdate = _sdate
        self.edate = _edate
        self.daterangestring = 'and date >= \'%s\' and date <= \'%s\''%(self.sdate.strftime("%Y-%m-%d"),self.edate.strftime("%Y-%m-%d"))

    def GetStockByPriceRange(self,min_price,max_price,number):
        script = 'select * from finlab.price where 收盤價 >= %d and 收盤價 <= %d %s'%(min_price,max_price,self.daterangestring)
        if number > 0:
            numberstring = ' limit %d'%(number)
            script+=numberstring
        return script