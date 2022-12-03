#import libraries
import pandas_datareader as pdr
import numpy as np
import datetime
import matplotlib.pyplot as plt
import yfinance as yf

#define variables
STARTING_BALANCE = 1000000

#define time period
START = datetime.datetime(2000, 1, 1)
END = datetime.datetime(2022, 12, 1)
DAYS = END - START
YEARS = DAYS.days / 365.25

#load data into a pandas dataframe
price = pdr.get_data_yahoo('RELIANCE.NS', START, END)

price.head()

#drop last two columns as they are not needed
price = price.drop(['Open', 'High', 'Low', 'Volume', 'Adj Close'], 1)

price.head()

#plot chart
plt.plot(price.Close)
plt.show()

#calculate daily return
price['Return'] = price.Close / price.Close.shift(1)
price.Return.iat[0] = 1
price['Bench_Bal'] = STARTING_BALANCE * price.Return.cumprod()

price

def backtest(price, period):
    #calculate moving average
    price['SMA'] = price.Close.rolling(window = period).mean()
    
    #Hold a position for every day the close is above the MA
    price['Long'] = price.Close > price.SMA
    
    #calculate the daily return from Open to Close on days in the market
    #on days when there is no trade, return is 1
    price['Sys_Ret'] = np.where(price.Long.shift(1) == True, price.Return, 1)
    
    #calculate balance
    price['Sys_Bal'] = STARTING_BALANCE * price.Sys_Ret.cumprod()
    
    #calculate drawdown
    price['Sys_Peak'] = price['Sys_Bal'].cummax()

    price['Sys_DD'] = price['Sys_Peak'] - price['Sys_Bal']
    
    cagr_sys = (((price.Sys_Bal[-1]/price.Sys_Bal[0])**(1/YEARS))-1)*100
    dd_sys = round(((price.Sys_DD / price.Sys_Peak).max()) * -100, 2)
    mar_sys = cagr_sys / abs(dd_sys)
    
    
    return cagr_sys, dd_sys, mar_sys

    #backtest the optimal value
backtest(price, 200)

#plot balance
plt.plot(price.Bench_Bal)
plt.plot(price.Sys_Bal)

plt.show()

periods = []
periods.extend(range(10, 250, 1))
cagr = []
dd = []
mar = []

for period in periods:
    cagr_sys, dd_sys, mar_sys = backtest(price, period)
    cagr.append(cagr_sys)
    dd.append(dd_sys)
    mar.append(mar_sys)


plt.figure()

#create subplot for price data
plt.subplot(311)
plt.plot(periods, cagr, 'g')

#create subplot for MACD
plt.subplot(312)
plt.plot(periods, dd, 'r')

#create subplot for MAR
plt.subplot(313)
plt.plot(periods, mar)

#show plot
plt.show()


optimal_value = periods[mar.index(max(mar))]

optimal_value


#backtest the optimal value
backtest(price, optimal_value)

plt.plot(price.Close)
plt.plot(price.SMA)

plt.show()

plt.plot(price.Bench_Bal)
plt.plot(price.Sys_Bal)

plt.show()
