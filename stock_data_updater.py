import yfinance as yf
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

ticker = ['SPY', 'AAPL']#, 'UBER', 'NFLX', 'AMZN', 'GOOGL','QQQ','TSLA','META','MSFT','QYLD', 'XOM','GE','DIS']
columns = ['Ticker', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
df_all = pd.DataFrame(columns=columns)
for t in ticker:
    data = yf.download(tickers=t,  # list of tickers
                       period="1m",  # time period
                       interval="1m",  # trading interval
                       prepost=False)  # download pre/post market hours data?
    print(data)
    data.drop(columns=['Adj Close'],axis=1, inplace=True)
    data.reset_index(inplace=True)
                    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        database="stock_data",
        user="postgres",
        password="postgres"
    )
    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/stock_data')

    data.to_sql(f"{t}", con=engine, schema="raw_stocks",if_exists="append", index=False)
    print(data)
