import pandas as pd
import numpy as np
import datetime
import sys
from Connection import TechAnalyzeModule


class main():
    sdate = datetime.date(2020,1,1)
    edate = datetime.date.today()
    TA_Module = TechAnalyzeModule.TechAnalyze(sdate,edate)
    ProceRange = TA_Module.GetStockByPriceRange(50,100,20)
    print(ProceRange)

