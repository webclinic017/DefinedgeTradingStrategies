from datetime import datetime, timedelta
from dateutil import parser
import pandas as pd
pd.set_option('display.max_rows', None)
import time
from logging import INFO, basicConfig, getLogger
from integrate import ConnectToIntegrate, IntegrateData
import requests
import pyotp
import sys
import json
import os
from pymongo import MongoClient

"""
slack_url = os.environ.get('slack_url')
slack_channel = os.environ.get('slack_channel')
CONNECTION_STRING = os.environ.get('CONNECTION_STRING')  #Mongo Connection
trade_end_time = parser.parse(str(os.environ.get('trade_end_time'))).time()
"""

slack_url = "https://hooks.slack.com/services/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"
slack_channel = "straddlebot"
CONNECTION_STRING = "mongodb+srv://adminuser:05NZN7kKp5D4TZnU@bots.vnitakj.mongodb.net/?retryWrites=true&w=majority" #Mongo Connection
trade_end_time = parser.parse("15:25:00").time()
trade_start_time = parser.parse("9:20:00").time()

mongo_client = MongoClient(CONNECTION_STRING)
collection_name = "supertrend"

supertrend_collection = mongo_client['Bots'][collection_name]

basicConfig(level=INFO)
logger = getLogger()

def login_to_integrate(api_token: str, api_secret: str) -> ConnectToIntegrate:
    """
    NZKUOQTJKBAVK3KNPBYUMRDTOBWUU2KV
    Login to Integrate and return the connection object.
    """
    conn = ConnectToIntegrate()
    totp = pyotp.TOTP("NZKUOQTJKBAVK3KNPBYUMRDTOBWUU2KV").now()
    conn.login(api_token=api_token, api_secret=api_secret, totp=totp)
    return conn

def fetch_historical_data(conn: ConnectToIntegrate, exchange: str, trading_symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Fetch historical data and return as a pandas DataFrame.
    """
    ic = IntegrateData(conn)
    history = ic.historical_data(
        exchange=exchange,
        trading_symbol=trading_symbol,
        timeframe=conn.TIMEFRAME_TYPE_MIN,  # Use the specific timeframe value
        start=start,
        end=end,
    )
    df = pd.DataFrame(list(history))  # Ensure conversion to list if generator
    return df

def round_to_nearest(x, base=0.05):
    """
    Round a number to the nearest specified multiple.
    """
    return round(base * round(float(x)/base), 2)

def resample_ohlc_data(df: pd.DataFrame, frequency: str) -> pd.DataFrame:
    """
    Resample OHLC data to specified frequency and return the resulting DataFrame.
    """
    df['datetime'] = pd.to_datetime(df['datetime'])  # Ensure 'datetime' is in datetime format
    df.set_index('datetime', inplace=True)
    df_resampled = df.resample(frequency).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna().reset_index()

    for column in ['open', 'high', 'low', 'close']:
        df_resampled[column] = df_resampled[column].apply(lambda x: round_to_nearest(x, base=0.05))
    return df_resampled

def atr(DF: pd.DataFrame, period: int):
    df = DF.copy()
    df['high-low'] = abs( df['high'] - df['low'] )
    df['high-previousclose'] = abs( df['high'] - df['close'].shift(1) )
    df['low-previousclose'] = abs( df['low'] - df['close'].shift(1) )
    df['TrueRange'] = df[ ['high-low', 'high-previousclose', 'low-previousclose'] ].max(axis=1, skipna=False)
    #df['ATR'] = df['TrueRange'].ewm(com=period, min_periods = period).mean() #Very close to TradingView
    df['ATR'] = df['TrueRange'].rolling(window=period).mean() #Very close to Definedge
    df['ATR'] = df['ATR'].round(2)
    return df['ATR']

def supertrend(df: pd.DataFrame, period: int, multiplier: int):
    df['hl2'] = (df['high'] + df['low']) / 2
    df['basic_upperband'] = df['hl2'] + (df['ATR'] * multiplier)
    df['basic_lowerband'] = df['hl2'] - (df['ATR'] * multiplier)
    df['final_upperband'] = 0
    df['final_lowerband'] = 0
    df['value'] = 0
    df['in_uptrend'] = True
    df['signal'] = "Bullish"
    
    for i in range(period, len(df)):
        # Update final bands
        df.at[i, 'final_upperband'] = df.at[i, 'basic_upperband'] if (df.at[i, 'basic_upperband'] < df.at[i-1, 'final_upperband']) or (df.at[i-1, 'close'] > df.at[i-1, 'final_upperband']) else df.at[i-1, 'final_upperband']
        df.at[i, 'final_lowerband'] = df.at[i, 'basic_lowerband'] if (df.at[i, 'basic_lowerband'] > df.at[i-1, 'final_lowerband']) or (df.at[i-1, 'close'] < df.at[i-1, 'final_lowerband']) else df.at[i-1, 'final_lowerband']    
    

    for current in range(1, len(df.index)):
        previous = current-1
        if df['close'][current] > df['final_upperband'][previous]:
            df['in_uptrend'][current] = True
            df['signal'][current] = "Bullish"
            df['value'][current] = df['final_lowerband'][current]
        elif df['close'][current] < df['final_lowerband'][previous]:
            df['in_uptrend'][current] = False
            df['signal'][current] = "Bearish"
            df['value'][current] = df['final_upperband'][current]
        else:
            df['in_uptrend'][current] = df['in_uptrend'][previous]
            if df['in_uptrend'][current]:
                df['value'][current] = df['final_lowerband'][current]
                df['signal'][current] = "Bullish"
            else:
                df['value'][current] = df['final_upperband'][current]
                df['signal'][current] = "Bearish"
    
    df['value'] = df['value'].round(2)
    print(df)
    df.drop(['open', 'high', 'low', 'basic_upperband', 'basic_lowerband', 'hl2', 'final_upperband', 'final_lowerband', 'volume', 'in_uptrend', 'ATR'], axis='columns', inplace=True)

    return df
    
def notify(title, message, color="#00FF00"):
    channel = "#" + slack_channel
    slack_data = {
        "username": "TradeBot",
        "icon_emoji": ":snowflake:",
        "channel": channel,
        "attachments": [
            {
                "color": color,
                "fields": [
                    {
                        "title": title,
                        "value": message,
                        "short": "false",
                    }
                ]
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json",
               'Content-Length': byte_length}
    requests.post(slack_url, data=json.dumps(slack_data), headers=headers)

def main():
    while True:
        if current_time > trade_start_time:
            current_time = datetime.now().time()
            api_token = "618a0b4c-f173-407e-acdc-0f61080f856c"
            api_secret = "TbfcWNtKL7vaXfPV3m6pKQ=="
            exchange = "NSE"
            trading_symbol = "Nifty 50"
            frequency = '15T'
            # Calculate 10 days ago from today
            ten_days_ago = datetime.now() - timedelta(days=5)

            # Set the time to 9:15 AM on that date
            start = ten_days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
            #start = datetime(2024, 2, 13, 9, 15)
            end = datetime.today()

            conn = login_to_integrate(api_token, api_secret)
            df = fetch_historical_data(conn, exchange, trading_symbol, start, end)
            df_15min = resample_ohlc_data(df, frequency)
            
            logger.info("\n***** 15-minute OHLC Data *****\n")
            df_15min['ATR'] = atr(df_15min, 10)
            df_15min= supertrend(df_15min, 10, 4)
            print(df_15min.iloc[-2])
            #notify("Fetching SuperTrend:",str(df_15min.iloc[-2]) , "#FF0000")
            #print(df_15min)

            if supertrend_collection.count_documents({"_id": "supertrend"}) == 0:
                st = {"_id": "supertrend", "datetime": df_15min.iloc[-2]['datetime'],
                            "close": df_15min.iloc[-2]['close'], "value": df_15min.iloc[-2]['value'], "signal": df_15min.iloc[-2]['signal']}
                supertrend_collection.insert_one(st)
                notify("Strategy details recorded for monitoring", str(st))
            else:
                supertrend_collection.update_one({'_id': "supertrend"}, {'$set': {"datetime": df_15min.iloc[-2]['datetime'],
                            "close": df_15min.iloc[-2]['close'], "value": df_15min.iloc[-2]['value'], "signal": df_15min.iloc[-2]['signal']}})
        

        if current_time > trade_end_time:
            return
        
        time.sleep(60)

if __name__ == "__main__":
    main()
