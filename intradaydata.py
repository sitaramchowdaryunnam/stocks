import time,requests
import pandas as pd
from datetime import datetime

def datetotimestamp(date):
    time_tuple = date.timetuple()
    timestampe = round(time.mktime(time_tuple))
    return timestampe

def timestamptodate(timestamp):
    return datetime.fromtimestamp(timestamp)

date = datetime.today()
print(datetotimestamp(date))


start = datetotimestamp(datetime(2021,1,1))
end =   datetotimestamp(datetime.today()) 
end = '1670027054'
url = 'https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history?symbol=RELIANCE&resolution=5&from='+str(start)+'&to='+str(end)+'&countback=292&currencyCode=INR'

print(url)

response = requests.get(url).json()
data = pd.DataFrame(response)
date = []
for dt in data['t']:
    date.append({'Date':timestamptodate(dt)})
dt = pd.DataFrame(date)    
print(dt)

dt = pd.DataFrame(date)
intraday_data = pd.concat([dt , data['o'],data['h'],data['l'],data['c'],data['v']],axis=1)\
    .rename(columns={'o':'Open','h':'High','l':'Low','c':'Close','v':'Volume'})
print(intraday_data)
