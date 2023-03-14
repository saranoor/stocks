import csv
import requests
import pandas as pd
import  time
import pickle 

slice_list=['year1month'+ str(m) for m in list(range(1,13))]
slice_list.extend(['year2month'+ str(m) for m in list(range(1,13))])
slice_list=['year1month'+ str(m) for m in list(range(1,3))]
# ticker_ls=['AAPL','NFLX', 'AMZN', 'GOOGL','QQQ','TSLA']
ticker_ls=['AMZN']
for ticker in ticker_ls:
    with requests.Session() as s:
        count=0
        for slice in slice_list:
            list_all_ticker=[]
            CSV_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=' + ticker + \
                      '&interval=1min&slice=' + slice + '&apikey='
            download = s.get(CSV_URL)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            ticker_list = list(cr)
            # print('headers are %s' %ticker_list[0:2] )
            # print('length of ticker list is%s' % len(ticker_list) )
            list_all_ticker.append(ticker_list[1:])
            # print("len of list now %s"%len(list_all_ticker))
            count=count+1
            # if count%3==0:
            #     time.sleep(60)
            print("len",len(list_all_ticker))

            for d in list_all_ticker[0]:
                # print('length of d', len(d))
                import psycopg2

                # Connect to the PostgreSQL database
                conn = psycopg2.connect(
                    host="localhost",
                    database="stock_data",
                    user="postgres",
                    password=""
                )

                # Set the schema name
                schema_name = "raw_stocks"

                # Create a cursor object
                cur = conn.cursor()
                # Create a new table in the desired schema
                cur.execute(f"""CREATE TABLE IF NOT EXISTS {schema_name}.{ticker} (
                        time TIMESTAMP,
                        open NUMERIC,
                        high NUMERIC,
                        low NUMERIC,
                        close NUMERIC,
                        volume NUMERIC
                    )""")
                try:

                    # print("lets try", type(d), d)
                    cur.execute(f"INSERT INTO {schema_name}.{ticker} (time, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s)", 
                                (d[0], d[1], d[2], d[3], d[4], d[5]))
                    conn.commit()
                    # print("done")
                    
                except Exception as e:
                    print(e)

            break
