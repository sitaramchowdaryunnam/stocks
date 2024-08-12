import urllib.parse
import pandas as pd
import pytz
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import logging
import time

# Set up logging
logging.basicConfig(filename='historical_data_collection.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Set the time zone
TIME_ZONE = pytz.timezone('Asia/Kolkata')

# Directory to save the files
save_directory = r'C:\Users\muniv\Desktop\Market\upstox_daily'
os.makedirs(save_directory, exist_ok=True)

# Read the symbols data
file_url = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
symboldf = pd.read_csv(file_url)
symboldf['expiry'] = pd.to_datetime(symboldf['expiry']).apply(lambda x: x.date())
symboldf = symboldf[symboldf.exchange == 'NSE_EQ']

# Read the equity list
eq_csv = pd.read_csv('EQUITY_List.csv')
isin_list = 'NSE_EQ|' + eq_csv[' ISIN NUMBER']
symboldf = symboldf[symboldf.instrument_key.isin(isin_list)]
symboldf.reset_index(drop=True, inplace=True)

def get_historical_data(sym_info):
    try:
        parse_instrument = urllib.parse.quote(sym_info.instrument_key)
        from_date = '2015-01-01'  # Starting date
        to_date = datetime.now(TIME_ZONE).strftime("%Y-%m-%d")
        url = f'https://api.upstox.com/v2/historical-candle/{parse_instrument}/day/{to_date}/{from_date}'
        
        res = requests.get(url, headers={'accept': 'application/json'}, timeout=5.0)
        candle_res = res.json()
        
        if 'data' in candle_res and 'candles' in candle_res['data'] and candle_res['data']['candles']:
            candle_data = pd.DataFrame(candle_res['data']['candles'])
            candle_data.columns = ['date', 'open', 'high', 'low', 'close', 'vol', 'oi']
            candle_data['date'] = pd.to_datetime(candle_data['date']).dt.tz_convert('Asia/Kolkata')
            candle_data['symbol'] = sym_info.tradingsymbol
            logging.info(f"Fetched data for {sym_info.tradingsymbol} with {len(candle_data)} records")
            return candle_data
        else:
            logging.warning(f'No data for {sym_info.instrument_key}, response: {candle_res}')
            return None
    except Exception as e:
        logging.error(f'Error fetching data for {sym_info.instrument_key}: {e}')
        return None

def save_data_to_csv(data, symbol):
    try:
        filename = os.path.join(save_directory, f'{symbol}.csv')
        data.to_csv(filename, index=False)
        logging.info(f"Saved data to {filename}")
    except Exception as e:
        logging.error(f"Error saving data for {symbol}: {e}")

def process_symbols_chunk(chunk_symbols):
    for symbol in chunk_symbols:
        sym_info = symboldf[symboldf.tradingsymbol == symbol].iloc[0]
        data = get_historical_data(sym_info)
        if data is not None:
            save_data_to_csv(data, symbol)

def chunk_process_symbols(symbols, chunk_size=100):
    num_chunks = (len(symbols) + chunk_size - 1) // chunk_size
    for i in range(num_chunks):
        chunk_start = i * chunk_size
        chunk_end = min((i + 1) * chunk_size, len(symbols))
        chunk_symbols = symbols[chunk_start:chunk_end]
        process_symbols_chunk(chunk_symbols)

if __name__ == "__main__":
    try:
        chunk_size = 50  # Adjust based on performance testing
        chunk_process_symbols(symboldf['tradingsymbol'].tolist(), chunk_size)
    except Exception as e:
        logging.error(f"Main process error: {e}")

    logging.info("Data collection complete.")
