from nsepy import get_history
from datetime import date,datetime
import pandas as pd
import pandas_ta as talib
#import talib
import matplotlib.pyplot as plt

stocks=[ 'ACC',	 'ACCELYA',	'ACCURACY', 'ACE', 'ACEINTEG', 'ACI', 'ADANIENT', 'ADANIGREEN', 'ADANIPORTS', 'ADANIPOWER', 'ADANITRANS', 'ADFFOODS', 'ADL', 'ADORWELD', 'ADROITINFO', 'ADSL', 'ADVANIHOTR', 'ADVENZYMES',]
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
    #print("check")
    return data['ema5'] > data['ema21'] and data['ema21'] > data['ema55']

def Loaddata():
    for stock in stocks:
        try:
            data = pd.read_csv('data/{}.csv'.format(stock)) 
            print(stock) 
            print("###################################################################################################")
            close = data.iloc[-1]['Close']
            open = data.iloc[-1]['Open']
            high = data.iloc[-1]['High']
            low = data.iloc[-1]['Low']
#### identificatio of streangth candle
            SC_Candle = False
            CH = high - low
            if close > open:
                BH = close - open
            else:
                BH = open - close
            if BH > 0: 
                SC = (BH/CH) * 100
                SC_Candle = False
                if SC > 50:
                    SC_Candle = True

            data['ema5']  = talib.ema(data['Close'], length=5)
            data['ema21']  = talib.ema(data['Close'], length=21)
            data['ema55']  = talib.ema(data['Close'], length=55)
            print(" Check Data : ", data)
            data['emacross'] = data.apply(emacross, axis=1)
            print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
            if data.iloc[-1]['emacross']:
                print("@@@@@@@@@@@    ema cross over @@@@@@@@@@@@")
                print(stock)
                #print(rawdata)
        except:
            print("you are in exception")
            pass
Loaddata()            
#Importdata()