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
trade_start_time = parser.parse("09:16:00").time()

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

def fetch_historical_data(conn: ConnectToIntegrate, exchange: str, trading_symbol: str, start: datetime, end: datetime, interval = 'min') -> pd.DataFrame:
    """
    Fetch historical data and return as a pandas DataFrame.
    """
    if interval == 'day':
        tf = conn.TIMEFRAME_TYPE_DAY
    elif interval == 'min':
        tf = conn.TIMEFRAME_TYPE_MIN

    ic = IntegrateData(conn)
    history = ic.historical_data(
        exchange=exchange,
        trading_symbol=trading_symbol,
        timeframe=tf,  # Use the specific timeframe value
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


def main():
    print("Supertrend Started")
    while True:
        current_time = datetime.now().time()
        if current_time > trade_start_time:
            api_token = "618a0b4c-f173-407e-acdc-0f61080f856c"
            api_secret = "TbfcWNtKL7vaXfPV3m6pKQ=="
            exchange = "NSE"
            trading_symbol = "Nifty 50"
            frequency = '15T'
            # Calculate 60 days ago from today
            days_ago = datetime.now() - timedelta(days=180)

            # Set the time to 9:15 AM on that date
            start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
            end = datetime.today()

            conn = login_to_integrate(api_token, api_secret)
            df_daily = fetch_historical_data(conn, exchange, trading_symbol, start, end, 'day')
            df_daily.drop(['volume'], axis='columns', inplace=True)
            df_daily['ema_low'] = df_daily['low'].ewm(com=10, min_periods = 21).mean() #Very close to TradingView
            df_daily['ema_high'] = df_daily['high'].ewm(com=10, min_periods = 21).mean() #Very close to TradingView

            df_daily['ema_low'] = df_daily['ema_low'].round(2)
            df_daily['ema_high'] = df_daily['ema_high'].round(2)
            print(df_daily)

            df_15min = df_1min = fetch_historical_data(conn, exchange, trading_symbol, start, end, 'min')
            df_15min = resample_ohlc_data(df_15min, frequency)
            df_15min.drop(['volume'], axis='columns', inplace=True)

            
            #print(df_15min)
            
        
        print("repeating loop for Supertrend")
        if current_time > trade_end_time:
            return
        
        time.sleep(10)

if __name__ == "__main__":
    main()
