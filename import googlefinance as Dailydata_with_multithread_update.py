from datetime import datetime, timedelta
import pandas as pd
import multiprocessing
from alpha_vantage.timeseries import TimeSeries
import time

# Replace 'YOUR_API_KEY' with your actual Alpha Vantage API key
API_KEY = 'WEZCLDSD608ATSNG'
ts = TimeSeries(key=API_KEY, output_format='pandas')

def get_alpha_vantage_historical_data(stock_symbol, start_date, end_date):
    try:
        # Attempt to fetch data from Alpha Vantage
        data, meta_data = ts.get_daily(symbol=stock_symbol, outputsize='full')
        if data.empty:
            raise ValueError("No data returned from API")
        data.index = pd.to_datetime(data.index)
        filtered_data = data.loc[start_date:end_date]
        print(f"Data collected for {stock_symbol}")
        return filtered_data
    except Exception as e:
        print(f"Failed to fetch data for {stock_symbol}: {e}")
        return pd.DataFrame()

def save_data_to_csv(stock_symbol, data):
    filename = f'C:/Users/muniv/Desktop/Market/Nifty_A1426/google/{stock_symbol}.csv'  # Adjust as needed
    data.reset_index().to_csv(filename, index=False)

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
            # Sleep for 60 seconds to avoid hitting rate limits
            # time.sleep(60)

def get_and_save_data(stock_symbol, start_date, end_date):
    stock_data = get_alpha_vantage_historical_data(stock_symbol, start_date, end_date)
    if not stock_data.empty:
        save_data_to_csv(stock_symbol, stock_data)

def import_data(stock_symbols, start_date, end_date):
    for stock_symbol in stock_symbols:
        print(stock_symbol)
        get_and_save_data(stock_symbol, start_date, end_date)
        # Sleep for 12 seconds between each request to avoid hitting rate limits
        # time.sleep(12)

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
