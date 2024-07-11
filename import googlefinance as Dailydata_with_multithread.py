from datetime import datetime, timedelta
import pandas as pd
import requests
import multiprocessing
import time

# Replace 'YOUR_API_KEY' with your actual EODHistoricalData API key
API_KEY = 
# '667edcb5617102.30098789'

def get_eod_historical_data(stock_symbol, start_date, end_date):
    base_url = f'https://eodhistoricaldata.com/api/eod/{stock_symbol}.NS'
    params = {
        'api_token': API_KEY,
        'from': start_date,
        'to': end_date,
        'period': 'd',
        'fmt': 'csv'
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Check for request errors
        data = pd.read_csv(pd.compat.StringIO(response.text))
        print(f"Data collected for {stock_symbol}")
        return data
    except Exception as e:
        print(f"Failed to fetch data for {stock_symbol}: {e}")
        return pd.DataFrame()

def save_data_to_csv(stock_symbol, data):
    filename = f'C:/Users/muniv/Desktop/Market/Nifty_A1426/eod/{stock_symbol}.csv'  # Adjust as needed
    data.to_csv(filename, index=False)

def import_stock_symbols_from_csv(filename):
    symbol_df = pd.read_csv(filename)
    stock_symbols = symbol_df['stock_symbol'].tolist()
    return stock_symbols

def process_stocks_chunk(stock_symbols, start_date, end_date, chunk_size=5):
    with multiprocessing.Pool() as pool:
        for i in range(0, len(stock_symbols), chunk_size):
            chunk = stock_symbols[i:i + chunk_size]
            print(f"Processing chunk {i//chunk_size + 1}")
            results = pool.starmap(get_and_save_data, [(stock_symbol, start_date, end_date) for stock_symbol in chunk])
            time.sleep(60)  # Sleep for 60 seconds to avoid hitting rate limits

def get_and_save_data(stock_symbol, start_date, end_date):
    stock_data = get_eod_historical_data(stock_symbol, start_date, end_date)
    if not stock_data.empty:
        save_data_to_csv(stock_symbol, stock_data)

def import_data(stock_symbols, start_date, end_date):
    for stock_symbol in stock_symbols:
        print(stock_symbol)
        get_and_save_data(stock_symbol, start_date, end_date)
        time.sleep(12)  # Sleep for 12 seconds between each request to avoid hitting rate limits

if __name__ == "__main__":
    csv_file_path = r'C:\Users\muniv\Desktop\Market\marketdata_analysis\stock_symbols_alpha.csv'  # Adjust as needed
    stock_symbols = import_stock_symbols_from_csv(csv_file_path)
    current_date = datetime.today().date()
    tomorrow_date = current_date + timedelta(days=1)
    print("Current Date:", current_date)
    print("Tomorrow's Date:", tomorrow_date)
    start_date = "2015-01-01"  # Replace with the desired start date
    end_date = tomorrow_date
    chunk_size = 5  # Number of stocks to process in each chunk (adjust as needed)
    process_stocks_chunk(stock_symbols, start_date, end_date, chunk_size)
    # import_data(stock_symbols, start_date, end_date)
