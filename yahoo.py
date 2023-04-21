# This script will download historical prices fom yahoo and insert them into the database
# Reference:    https://pypi.org/project/yfinance/
#               https://www.analyticsvidhya.com/blog/2021/06/download-financial-dataset-using-yahoo-finance-in-python-a-complete-guide/


import sys
import yfinance as yf
import pyodbc
from datetime import datetime
from datetime import timedelta


symbols = {}


def get_yesterdays_date():
    dt = datetime.today() - timedelta(days=1)
    return dt.strftime('%Y-%m-%d')


def get_tomorrows_date():
    dt = datetime.today() + timedelta(days=1)
    return dt.strftime('%Y-%m-%d')


def get_sql_server_connection():
    conn = pyodbc.connect('DSN=chaos;Trusted_Connection=yes;APP=yahoo_script', autocommit=True)
    return conn


def get_supported_symbols(conn):
    cursor = conn.cursor()
    sql = 'SELECT * FROM view_yahoo_symbols;'
    cursor.execute(sql)

    for row in cursor.fetchall():
        # row[0] = instrument_id
        # row[1] = yahoo symbol
        symbols[row[1]] = row[0]


def download_yahoo_historical_prices():
    dt_from = ""
    dt_to = ""
    if len(sys.argv) == 3:
        # Both dates were provided so ue them
        dt_from = str(sys.argv[1])
        dt_to = str(sys.argv[2])
    else:
        # No dates were provided so just get yesterday
        dt_from = get_yesterdays_date()
        dt_to = get_tomorrows_date()

    conn = get_sql_server_connection()
    get_supported_symbols(conn)

    for s, i in symbols.items():
        df = yf.download(s, dt_from, dt_to)
        # print(df)
        # Retrieve data
        # df.to_csv(rf'C:\repo\yahoo\{s}.csv', index=False)
        # print(df.filter(items=['2023-04-05'], axis=0))
        # print(len(df), df['Open'].values[0], df['High'].values[0])
        # print('Date\t', 'Open\t', 'High\t', 'Low\t', 'Close\t', 'AdjClose\t', 'Volume')
        for r in range(0, len(df)):
            dt = rf"{str(df.index[r])[0:10]}"
            o = float('{:0.2f}'.format(df.iloc[r, 0]))
            h = float('{:0.2f}'.format(df.iloc[r, 1]))
            l = float('{:0.2f}'.format(df.iloc[r, 2]))
            c = float('{:0.2f}'.format(df.iloc[r, 3]))
            adj = float('{:0.2f}'.format(df.iloc[r, 4]))
            v = int(df.iloc[r, 5])
            # print(dt, o, h, l, c, adj, v)
            params = (dt, i, o, h, l, c, adj, v)
            sql = "{CALL sp_upsert_yahoo_ohlc (?,?,?,?,?,?,?,?)}"
            cursor = conn.cursor()
            cursor.execute(sql, params)

        # Additional information that might be helpful
        # sym = yf.Ticker(s)
        # sym.info
        # div = sym.dividends
        # print(div)
        # act = sym.actions
        # print(act)
        # spl = sym.splits
        # print(spl)
        # earn = sym.earnings_dates
        # print(earn)


if __name__ == '__main__':
    download_yahoo_historical_prices()
