import pandas as pd
import concurrent.futures
import time

# ... (your existing imports and functions)

def analyze_stock(stock, data):
    # Your existing analysis logic for each stock
    print("Stock name : ",stock)
    try:
        for index, row in data.iterrows():    
            # print(" complete row details :", row)
            if row['Buy_Entry'] == 'fresh buy':
                        entry_close = row['Close']
                        entry_date = row['Date_new']
                        ema5 = row['ema5']
                        ema21 = row['ema21']
                        ema55 = row['ema55']
                        cci341d = row['cci34_1D']
                        cci341w = row['cci34_1W']
                        exit_found = False
                        goldentry = ''
                        Highprob = ''
                        checkprob = ''
                        if ((ema5 > ema21) and (ema21 > ema55)) and (cci341d > 100 and cci341w > 100):
                             goldentry = "Golden entry"
                        elif ((ema5 > ema21) and (ema21 > ema55)) and (cci341w > 70 and cci341d > 100):
                             Highprob = "High probability"
                        else:
                             checkprob = "check the chart" 
                        entrytype=''
                        if goldentry != '':
                             entrytype = goldentry
                        elif Highprob != '':
                             entrytype = Highprob
                        else:
                             entrytype = checkprob     
                        # Find the matching exit row
                        matching_exit_rows = data.loc[(data['Date'] > entry_date) & (data['exit_buy'] == 'Exit buy')]
                        if not matching_exit_rows.empty:
                            matching_exit_row = matching_exit_rows.iloc[0]
                            sell_close = matching_exit_row['Close']
                            sell_date = matching_exit_row['Date']

                            if entry_close > sell_close:
                                loss = entry_close - sell_close
                                loss_percent = (sell_close / entry_close - 1) * 100
                                comment = "SL HIT" if loss_percent <= -10 else "Indicator Exit"
                                new_record = {
                                            'Stock Name': stock,
                                            'Entry price': entry_close,
                                            'Entry Date': entry_date,
                                            'LTP': sell_close,
                                            'Exit Date': sell_date,
                                            'Loss': loss,
                                            'LH 5%': (100 - (entry_close / sell_close ) * 100),
                                            'Comment':comment,
                                            'Entry Type': entrytype
                                    
                                        }
                                report_data.append(new_record)
                            elif entry_close < sell_close:
                                profit = sell_close - entry_close
                                profit_percent = (sell_close / entry_close - 1) * 100
                                comment = "Profit HIT" if profit_percent >= 20 else "Indicator Exit"
                                new_record = {
                                                'Stock Name': stock,
                                                'Entry price': entry_close,
                                                'Entry Date': entry_date,
                                                'LTP': sell_close,
                                                'Exit Date': sell_date,
                                                'Profit': profit,
                                                'PH 10%': profit_percent,
                                                'Comment':comment,
                                                'Entry Type': entrytype
                                            }
                                report_data.append(new_record)
                            exit_found = True
                        else:
                            last_row = data.iloc[-1]  # Get the last row of the DataFrame
                            last_close = last_row['Close']
                            
                            # if last_close > entry_close:
                            pnl = last_close - entry_close
                            # else:
                            #     pnl =  entry_close - last_close   
                            profit_percent = (last_close / entry_close - 1) * 100 
                            new_record = {
                                            'Stock Name': stock,
                                            'Entry price': entry_close,
                                            'Entry Date': entry_date,
                                            'Entry Type': entrytype,
                                            'LTP': last_close,
                                            'Comment':"In Progress",
                                            'PNL of In progress': pnl,
                                            'In Progress PNL%': profit_percent
                                        }
                            report_data.append(new_record)
                            
                            
    except FileNotFoundError as e:
            print(f"File not found: {e}")

    
    report_df = pd.DataFrame(report_data)
    report_df.to_csv(report_gen, encoding='utf-8', index=False)
def import_stock_symbols_from_csv(filename):
    symbol_df = pd.read_csv(filename)
    stock_symbols = symbol_df['stock_symbol'].tolist()
    return stock_symbols

if __name__ == "__main__":
    
    report_gen = 'C:/Users/muniv/Desktop/Market/marketdata_analysis/Reports_gen_multi_d30-08-2023__Test.csv'
    # report_gen = 'C:/Users/mvadlamudi/Desktop/activity/QuantAnalysis/Reports_gen_multi_d25-08-2023.csv'
    start_time = time.time()
    csv_file_path = r'C:\Users\muniv\Desktop\Market\marketdata_analysis\stock_symbols.csv'
    # csv_file_path = r'C:/Users/mvadlamudi/Desktop/activity/QuantAnalysis/marketdata_analysis/stock_symbols.csv' #mainlap
    stock_symbols = import_stock_symbols_from_csv(csv_file_path)
    report_data = []

    max_threads = 50  # Adjust the number of threads as needed
    with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
        # Create a dictionary to map stock symbols to their respective dataframes
        stock_data = {}
        for stock in stock_symbols:
            try:
                # data = pd.read_csv(f'C:/Users/mvadlamudi/Desktop/activity/QuantAnalysis/Signals_multi/{stock}.csv')
                # data = pd.read_csv(f'C:/Users/muniv/Desktop/Market/Signals_multi/{stock}.csv')
                data = pd.read_csv(f'C:/Users/muniv/Desktop/Market/Signals_multi/{stock}.csv')
                stock_data[stock] = data
            except FileNotFoundError as e:
                print(f"File not found: {e}")
        
        # Submit tasks to the thread pool for analysis
        future_to_stock = {executor.submit(analyze_stock, stock, data): stock for stock, data in stock_data.items()}
        
        # Wait for the tasks to complete
        for future in concurrent.futures.as_completed(future_to_stock):
            stock = future_to_stock[future]
            try:
                future.result()
                print(f"Analysis complete for {stock}")
            except Exception as exc:
                print(f"Analysis for {stock} generated an exception: {exc}")
    

    # Your code here

    end_time = time.time()
    total_time = end_time - start_time

    print(f"Total runtime: {total_time / 60:.2f} minutes")
    print("Report generation complete.")
