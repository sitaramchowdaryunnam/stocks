import urllib.parse
import pandas as pd
import pytz
import os
import requests
import json
from datetime import datetime, timedelta


TIME_ZONE = pytz.timezone('Asia/Kolkata')

# Download the symbols file
fileUrl = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
symboldf = pd.read_csv(fileUrl)
symboldf['expiry'] = pd.to_datetime(symboldf['expiry']).apply(lambda x: x.date())
symboldf = symboldf[symboldf.exchange == 'NSE_EQ']

# Read the equity file
eqCsv = pd.read_csv('EQUITY_L.csv')
isinList = 'NSE_EQ|' + eqCsv[' ISIN NUMBER']
symboldf = symboldf[symboldf.instrument_key.isin(isinList)]

def getHistoricalData(symInfo):
    res = None
    try:
        parseInstrument = urllib.parse.quote(symInfo.instrument_key)
        fromDate = "2007-01-01"
        current_date = datetime.today().date()
        toDate = current_date + timedelta(days=1)
        url = f'https://api.upstox.com/v2/historical-candle/{parseInstrument}/day/{toDate}/{fromDate}'
        
        res = requests.get(url, headers={'accept': 'application/json'}, params={}, timeout=5.0)
        candleRes = res.json()
        
        if 'data' in candleRes and 'candles' in candleRes['data'] and candleRes['data']['candles']:
            candleData = pd.DataFrame(candleRes['data']['candles'])
            candleData.columns = ['date', 'open', 'high', 'low', 'close', 'vol', 'oi']
            candleData['date'] = pd.to_datetime(candleData['date']).dt.tz_convert('Asia/Kolkata')
            candleData['symbol'] = symInfo.tradingsymbol
            print(symInfo.tradingsymbol, len(candleData))
            return candleData
        else:
            print('No data', symInfo.instrument_key, candleRes)
    except Exception as e:
        print(f'Error in data fetch for {symInfo.instrument_key} {res} {e}')
    return pd.DataFrame()

# Collecting all candle data
candleDfList = []
for i, row in symboldf.iterrows():
    candleData = getHistoricalData(row)
    if not candleData.empty:
        candleDfList.append(candleData)

# Concatenating all data frames
finalDataDf = pd.concat(candleDfList, ignore_index=True)
# print(finalDataDf)
# Define the directory to save the files
output_directory = r'C:\Users\muniv\Desktop\Market\upstox_daily'

# Ensure the directory exists
os.makedirs(output_directory, exist_ok=True)
# Save to CSV or Parquet
isCsv = True
for symData in candleDfList:
    try:
        filename = symData.iloc[0]['symbol']
        if isCsv:
            filepath = os.path.join(output_directory, f'{filename}.csv')
            symData.to_csv(filepath, index=False)
        else:
            filepath = os.path.join(output_directory, f'{filename}.parquet')
            symData.to_parquet(filepath, engine='pyarrow')
    except Exception as e:
        print(f'Error {e}')
