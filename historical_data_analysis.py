from nsepy import get_history
from datetime import date, datetime
import pandas as pd
#import talib
import matplotlib.pyplot as plt

stocks=['ACC', 'ADANIENT']
start_date=date(2021,1,1)
end_date=date.today()
def Importdata():
    for stock in stocks:
        rawdata = get_history(symbol=stock,start=start_date,end=end_date)
        file_name = 'data/{}.csv'.format(stock)
        df = pd.DataFrame(rawdata)
        df.to_csv(file_name,encoding='utf-8')
        print(stock)

def Loaddata():
    for stock in stocks:
        try:
            rawdata = pd.read_csv('data/{}.csv'.format(stock))       
            print(stock)
            print(rawdata)
        except:
            pass
Loaddata()            
#Importdata()