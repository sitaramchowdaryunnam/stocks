#import libraries
import pandas as pdr
import numpy as np
import datetime
import matplotlib.pyplot as plt

#define variables
super_MA = 5
FAST_MA = 21
SLOW_MA = 55
STARTING_BALANCE = 1000000

#define time period
START = datetime.datetime(2005, 1, 1)
END = datetime.datetime(2022, 12, 1)
YEARS = (END - START).days / 365.25

#load data into a pandas dataframe
price = pdr.get_data_yahoo('^NSEI', START, END)

price.head(5)

#drop redundant columns
price = price.drop(['High', 'Low', 'Volume', 'Adj Close'], 1)

price.head(5)

#plot chart
plt.plot(price.Close)
plt.show()

#calculate daily return
price['Return'] = price.Close / price.Open

price['Bench_Bal'] = STARTING_BALANCE * price.Return.cumprod()

price.tail()

#calculate metrics
bench_return = round(((price.Bench_Bal[-1] / price.Bench_Bal[0]) - 1) * 100, 2)
bench_cagr = round((((price.Bench_Bal[-1] / price.Bench_Bal[0]) ** (1/YEARS))-1) * 100, 2)

print(bench_return)
print(bench_cagr)

#calculate drawdown
price['Bench_Peak'] = price.Bench_Bal.cummax()

price['Bench_DD'] = price.Bench_Bal - price.Bench_Peak

bench_dd = round((((price.Bench_DD / price.Bench_Peak).min()) * 100), 2)

bench_dd


#calculate moving averages
price['super_MA'] = price.Close.rolling(window = super_MA).mean()
price['Fast_MA'] = price.Close.rolling(window = FAST_MA).mean()
price['Slow_MA'] = price.Close.rolling(window = SLOW_MA).mean()

price.tail()

#draw graph
plt.plot(price.Close)
plt.plot(price.super_MA)
plt.plot(price.Fast_MA)
plt.plot(price.Slow_MA)

plt.show()

#define entries
price['Long'] = (price.Fast_MA > price.Slow_MA) & (price.super_MA > price.Fast_MA)

price.tail()

#calculate system balance
price['Sys_Ret'] = np.where(price.Long.shift(1) == True, price.Return, 1)

price.tail()

#calculate system balance
price['Sys_Bal'] = STARTING_BALANCE * price.Sys_Ret.cumprod()

price.tail()

plt.plot(price.Bench_Bal)
plt.plot(price.Sys_Bal)

plt.show()


#calculate metrics
sys_return = round(((price.Sys_Bal[-1] / price.Sys_Bal[0]) - 1) * 100, 2)
sys_cagr = round((((price.Sys_Bal[-1] / price.Sys_Bal[0]) ** (1/YEARS))-1) * 100, 2)


print('system returns @@@',sys_return)
print('system carg @@@@',sys_cagr)


#calculate drawdown
price['Sys_Peak'] = price.Sys_Bal.cummax()

price['Sys_DD'] = price.Sys_Bal - price.Sys_Peak

sys_dd = round((((price.Sys_DD / price.Sys_Peak).min()) * 100), 2)

print('system drawdown @@@@@',sys_dd)


print(f'Benchmark Total return: {bench_return}%')
print(f'Benchmark CAGR: {bench_cagr}')
print(f'Benchmark DD: {bench_dd}%')
print('')
print(f'System Total return: {sys_return}%')
print(f'System CAGR: {sys_cagr}')
print(f'System DD: {sys_dd}%')
