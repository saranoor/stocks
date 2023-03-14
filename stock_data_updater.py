import yfinance as yf
import pandas as pd
ticker = ['SPY', 'AAPL']#, 'UBER', 'NFLX', 'AMZN', 'GOOGL','QQQ','TSLA','META','MSFT','QYLD', 'XOM','GE','DIS']
columns = ['Ticker', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
df_all = pd.DataFrame(columns=columns)
for t in ticker:
    data = yf.download(tickers=t,  # list of tickers
                       period="5m",  # time period
                       interval="1m",  # trading interval
                       prepost=False)  # download pre/post market hours data?
    data['Ticker']=t
    df_all = pd.concat([df_all, data])
    print(df_all.head())
df_all.to_csv('./data/stock_data.csv', mode='a')