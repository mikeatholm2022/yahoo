# This script will download historical prices fom yahoo and insert them into the database
# Reference:    https://pypi.org/project/yfinance/
#               https://github.com/ranaroussi/yfinance
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


def get_todays_date():
    dt = datetime.today()
    return dt.strftime('%Y%m%d')


def get_sql_server_connection():
    conn = pyodbc.connect('DSN=chaos;Trusted_Connection=yes;APP=yahoo_script', autocommit=True)
    return conn


def is_date_within_our_filter(act_dt, dt_from, dt_to):
    pit_date = datetime.strptime(act_dt, "%Y-%m-%d")
    from_date = datetime.strptime(dt_from, "%Y-%m-%d")
    to_date = datetime.strptime(dt_to, "%Y-%m-%d")
    if from_date <= pit_date <= to_date:
        return True

    return False


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
    # symbols.clear()
    # symbols['AMD'] = 49

    logger = open(rf"C:\chaos\logs\yahoo_uploader_{get_todays_date()}.log", "w")

    for s, i in symbols.items():
        # Default from date is the one we generated above
        filter_from = dt_from

        # First we need to know if a corporate action occurred for this symbol
        sym = yf.Ticker(s)
        sym.info
        act = sym.actions
        # print('actions', act)
        if len(act) > 0:
            act_dt = rf"{str(act.index[len(act)-1])[0:10]}"
            if is_date_within_our_filter(act_dt, dt_from, dt_to):
                my_str = f"Corporate Action occurred, you need to download the whole timeseries for {s}\n"
                print(my_str)
                logger.write(my_str)
                filter_from = "2020-01-01"

        df = yf.download(s, filter_from, dt_to)
        # print(df)
        # Retrieve data
        # df.to_csv(rf'C:\repo\yahoo\{s}.csv', index=False)
        # print(df.filter(items=['2023-04-05'], axis=0))
        # print(len(df), df['Open'].values[0], df['High'].values[0])
        # print('Date\t', 'Open\t', 'High\t', 'Low\t', 'Close\t', 'AdjClose\t', 'Volume')
        print("Downloading ", s)
        for r in range(0, len(df)):
            dt = rf"{str(df.index[r])[0:10]}"
            o = float('{:0.2f}'.format(df.iloc[r, 0]))
            h = float('{:0.2f}'.format(df.iloc[r, 1]))
            l = float('{:0.2f}'.format(df.iloc[r, 2]))
            c = float('{:0.2f}'.format(df.iloc[r, 3]))
            adj = float('{:0.2f}'.format(df.iloc[r, 4]))
            v = int(df.iloc[r, 5])
            # print(dt, o, h, l, c, adj, v)
            my_str = f"Uploading {s} for {dt}\n"
            # print(my_str)
            logger.write(my_str)
            params = (dt, i, o, h, l, c, adj, v)
            sql = "{CALL sp_upsert_yahoo_ohlc (?,?,?,?,?,?,?,?)}"
            cursor = conn.cursor()
            cursor.execute(sql, params)

    logger.close()


if __name__ == '__main__':
    download_yahoo_historical_prices()
