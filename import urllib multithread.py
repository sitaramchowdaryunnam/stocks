import urllib.parse
import pandas as pd
import pytz
import os
import requests
import json
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed  # Corrected import
import time

TIME_ZONE = pytz.timezone('Asia/Kolkata')

# Function to calculate and print the elapsed time
def print_execution_time(start_time, message):
    elapsed_time = time.time() - start_time
    print(f'{message} - Elapsed time: {elapsed_time:.2f} seconds')

# Function to fetch historical data for a chunk of stocks
def getHistoricalDataChunk(chunk, isCsv, output_directory):
    candleDfList = []
    for i, row in chunk.iterrows():
        res = None
        try:
            parseInstrument = urllib.parse.quote(row.instrument_key)
            fromDate = "2015-01-01"
            current_date = datetime.today().date()
            toDate = current_date + timedelta(days=1)
            url = f'https://api.upstox.com/v2/historical-candle/{parseInstrument}/day/{toDate}/{fromDate}'
            
            res = requests.get(url, headers={'accept': 'application/json'}, params={}, timeout=5.0)
            candleRes = res.json()
            
            if 'data' in candleRes and 'candles' in candleRes['data'] and candleRes['data']['candles']:
                candleData = pd.DataFrame(candleRes['data']['candles'])
                candleData.columns = ['date', 'open', 'high', 'low', 'close', 'vol', 'oi']
                candleData['date'] = pd.to_datetime(candleData['date']).dt.tz_convert('Asia/Kolkata')
                candleData['symbol'] = row.tradingsymbol
                print(row.tradingsymbol, len(candleData))
                
                # Save the data immediately
                filename = candleData.iloc[0]['symbol']
                if isCsv:
                    filepath = os.path.join(output_directory, f'{filename}.csv')
                    candleData.to_csv(filepath, index=False)
                else:
                    filepath = os.path.join(output_directory, f'{filename}.parquet')
                    candleData.to_parquet(filepath, engine='pyarrow')
                
                candleDfList.append(candleData)
            else:
                print('No data', row.instrument_key, candleRes)
        except Exception as e:
            print(f'Error in data fetch for {row.instrument_key} {res} {e}')
    
    return candleDfList

if __name__ == "__main__":
    start_time = time.time()

    # Download the symbols file
    fileUrl = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
    symboldf = pd.read_csv(fileUrl)
    symboldf['expiry'] = pd.to_datetime(symboldf['expiry']).apply(lambda x: x.date())
    symboldf = symboldf[symboldf.exchange == 'NSE_EQ']

    # Read the equity file
    eqCsv = pd.read_csv('EQUITY_L.csv')
    isinList = 'NSE_EQ|' + eqCsv[' ISIN NUMBER']
    symboldf = symboldf[symboldf.instrument_key.isin(isinList)]

    # Define the directory to save the files
    output_directory = r'C:\Users\muniv\Desktop\Market\upstox_daily'
    os.makedirs(output_directory, exist_ok=True)

    # Processing stocks in chunks and saving data using multiprocessing
    isCsv = True
    chunk_size = 50  # Number of stocks to process in each chunk
    num_chunks = len(symboldf) // chunk_size + 1

    with ProcessPoolExecutor() as executor:  # Using ProcessPoolExecutor
        futures = []
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(symboldf))
            chunk = symboldf.iloc[start_idx:end_idx]
            futures.append(executor.submit(getHistoricalDataChunk, chunk, isCsv, output_directory))

        for future in as_completed(futures):
            candleDfList = future.result()

    print_execution_time(start_time, "Data collection and saving completed")
