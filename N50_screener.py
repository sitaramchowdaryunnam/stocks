from nsepy import get_history
from datetime import date,datetime
import pandas as pd
import numpy as np
import pandas_ta as talib
#import talib
import matplotlib.pyplot as plt

#stocks=[ 'ACC',	 'ACCELYA',	'ACCURACY', 'ACE', 'ACEINTEG', 'ACI', 'ADANIENT', 'ADANIGREEN', 'ADANIPORTS', 'ADANIPOWER', 'ADANITRANS', 'ADFFOODS', 'ADL', 'ADORWELD', 'ADROITINFO', 'ADSL', 'ADVANIHOTR', 'ADVENZYMES',]
stocks=['ADANIENT',	'TCS',	'RELIANCE',	'AXISBANK',	'INDUSINDBK',	'ULTRACEMCO',	'ICICIBANK',	'NESTLEIND',	'WIPRO',	'SBIN',	'ONGC',	'DIVISLAB',	'HCLTECH',	'INFY',	'TATACONSUM',	'ASIANPAINT',	'SUNPHARMA',	'ITC',	'BAJAJFINSV',	'HDFC',	'APOLLOHOSP',	'CIPLA',	'BAJAJ-AUTO',	'JSWSTEEL',	'KOTAKBANK',	'TITAN',	'COALINDIA',	'GRASIM',	'BPCL',	'HINDALCO',	'HDFCBANK',	'HEROMOTOCO',	'DRREDDY',	'BAJFINANCE',	'TATASTEEL',	'TECHM',	'POWERGRID',	'HDFCLIFE',	'ADANIPORTS',	'NTPC',	'BRITANNIA',	'MARUTI',	'LT',	'M&M',	'BHARTIARTL',	'HINDUNILVR',	'TATAMOTORS',	'UPL',	'EICHERMOT',	'SBILIFE',]
start_date=date(2021,1,1)
end_date=date.today()
def Importdata():
    for stock in stocks:
        rawdata = get_history(symbol=stock,start=start_date,end=end_date)
        file_name = 'N50data/{}.csv'.format(stock)
        df = pd.DataFrame(rawdata)
        df.to_csv(file_name,encoding='utf-8')
        print(stock)
def buy_sell_function(data):
    buy_list = []
    sell_list = []
    flag_long = False
    flag_short = False
    for i in range(0,len(data)):
        if data['ema21'][i] < data['ema55'][i] and data['ema21'][i] > data['ema5'][i] and flag_long == False and flag_short == False:
            buy_list.append(data['Close'][i])
            sell_list.append(np.nan)
            flag_short = True
        elif data['ema21'][i] > data['ema55'][i] and data['ema21'][i] < data['ema5'][i] and flag_short == False and flag_long == False:
            buy_list.append(data['Close'][i])
            sell_list.append(np.nan)
            flag_long = True
        # elif flag_short == True and data['ema5'][i] > data['ema21'][i]:
        #     sell_list.append(data['Close'][i])
        #     buy_list.append(np.nan)
        #     flag_short = False
        # elif flag_long == True and data['ema5'][i] < data['ema21'][i]:
        #     sell_list.append(data['Close'][i])
        #     buy_list.append(np.nan)
        #     flag_long = False
        else:
            buy_list.append(np.nan)
            sell_list.append(np.nan)
    
    return (buy_list, sell_list)
def emacross(data):
    #print("check")
    return data['ema5'] > data['ema21'] and data['ema21'] > data['ema55']

def Loaddata():
    for stock in stocks:
        try:
            data = pd.read_csv('N50data/{}.csv'.format(stock)) 
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
            buy_sell_function(data)

            #print(buy_sell_function(data)[0])
            data['Buy'] =   buy_sell_function(data)[0]
            data['Sell'] = buy_sell_function(data)[1]
            fresh_buy = data.iloc[-1]['Buy']
            fresh_sell = data.iloc[-1]['Sell']
            if fresh_buy > 0:
                print("EMA cross fresh Buy")
            if fresh_sell > 0:
                print("EMA cross fresh Sell")

            print(data.iloc[-1])
            data['emacross'] = data.apply(emacross, axis=1)
            print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
            # if data.iloc[-1]['emacross']:
            #     print("@@@@@@@@@@@    ema cross over @@@@@@@@@@@@")
            #     print(stock)
            #     #print(rawdata)
        except:
            print("you are in exception")
            pass
Importdata()
Loaddata()            
