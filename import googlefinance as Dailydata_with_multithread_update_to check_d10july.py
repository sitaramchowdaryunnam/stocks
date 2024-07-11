from datetime import datetime, timedelta
import pandas as pd
import multiprocessing
import time
from alpha_vantage.timeseries import TimeSeries

ALPHA_VANTAGE_API_KEY = 'WEZCLDSD608ATSNG'

def get_alpha_vantage_historical_data(stock_symbol, start_date, end_date, retries=3, delay=5):
    ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')
    for attempt in range(retries):
        try:
            data, meta_data = ts.get_daily(symbol=stock_symbol, outputsize='full')
            data = data[start_date:end_date]
            if data.empty:
                raise ValueError(f"No price data found for {stock_symbol}")
            print(f"Data collected for {stock_symbol}")
            return data
        except Exception as e:
            print(f"Error fetching data for {stock_symbol}: {e}")
            if attempt < retries - 1:
                print(f"Retrying... ({attempt + 1}/{retries})")
                time.sleep(delay)
            else:
                print(f"Failed to fetch data for {stock_symbol} after {retries} attempts.")
                return None

def save_data_to_csv(stock_symbol, data):
    filename = f'C:/Users/muniv/Desktop/Market/Nifty_A1426/google/{stock_symbol}.csv'
    data.reset_index().to_csv(filename, index=False)

def import_stock_symbols_from_csv(filename):
    symbol_df = pd.read_csv(filename)
    stock_symbols = symbol_df['stock_symbol'].tolist()
    return stock_symbols

def get_and_save_data(stock_symbol, start_date, end_date):
    stock_data = get_alpha_vantage_historical_data(stock_symbol, start_date, end_date)
    if stock_data is not None:
        save_data_to_csv(stock_symbol, stock_data)
    return stock_symbol

def process_stocks_chunk(stock_symbols, start_date, end_date, chunk_size=100):
    with multiprocessing.Pool() as pool:
        for i in range(0, len(stock_symbols), chunk_size):
            chunk = stock_symbols[i:i + chunk_size]
            print(f"Processing chunk {i // chunk_size + 1}")
            results = pool.starmap(get_and_save_data, [(stock_symbol, start_date, end_date) for stock_symbol in chunk])
            for result in results:
                if result:
                    print(f"Processed {result}")

if __name__ == "__main__":
    csv_file_path = r'C:/Users/muniv/Desktop/Market/marketdata_analysis/stock_symbols.csv'
    stock_symbols = import_stock_symbols_from_csv(csv_file_path)
    current_date = datetime.today().date()
    tomorrow_date = current_date + timedelta(days=1)
    print("Current Date:", current_date)
    print("Tomorrow's Date:", tomorrow_date)
    start_date = "2015-01-01"
    end_date = tomorrow_date
    chunk_size = 100
    process_stocks_chunk(stock_symbols, start_date, end_date, chunk_size)
