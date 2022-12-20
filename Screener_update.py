from nsepy import get_history
from datetime import date,datetime
import pandas as pd
import talib  as talib
import numpy as np
#import matplotlib.pyplot as plt

stocks=[ 'ACC'] #,	 'ACCELYA',	'ACCURACY', 'ACE', 'ACEINTEG', 'ACI', 'ADANIENT', 'ADANIGREEN', 'ADANIPORTS', 'ADANIPOWER', 'ADANITRANS', 'ADFFOODS', 'ADL', 'ADORWELD', 'ADROITINFO', 'ADSL', 'ADVANIHOTR', 'ADVENZYMES',]
start_date=date(2021,1,1)
end_date=date.today()
def Importdata():
    for stock in stocks:
        rawdata = get_history(symbol=stock,start=start_date,end=end_date)
        file_name = 'data/{}.csv'.format(stock)
        df = pd.DataFrame(rawdata)
        df.to_csv(file_name,encoding='utf-8')
        print(stock)

def emacross(data):
    return data['ema5'] > data['ema21'] and data['ema21'] > data['ema55']

def Loaddata():
    for stock in stocks:
        try:
            ema = talib.get_functions('EMA', timeperiod=5)
            data = pd.read_csv('data/{}.csv'.format(stock)) 
            print("scanning for : " ,stock) 
            data['ema5']   = talib.EMA(data['close'], timeperiod=5)
            data['ema21']  = talib.EMA(data['close'], timeperiod=21)
            data['ema55']  = talib.EMA(data['close'], timeperiod=55)
            cc = data.iloc[-1]
            print("!!!!!!")
            if (cc['ema5'] > cc['ema21']) and (cc['ema21'] > cc['ema55']):
                print("!!!!!!")
                print("buy signal")
        except:
            pass
Loaddata()            
#Importdata()