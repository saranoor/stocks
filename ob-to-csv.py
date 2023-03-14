import pickle
import pandas as pd
# ticker=['SPY','AAPL','NFLX','AMZN']
ticker=['AAPL']
for t in ticker:
    ticker_data='./data/list_'+t+'.ob'
    with open (ticker_data, 'rb') as fp:
        list_ticker = pickle.load(fp)
        # print(list_ticker[0:3])
    df=pd.concat([pd.DataFrame(d, columns=['time', 'open', 'high', 'low', 'close', 'volume']) for d in list_ticker], ignore_index=True)

    print(df.head(5))
    print(df.tail(5))
    df.to_csv('./data/'+t+'.csv')