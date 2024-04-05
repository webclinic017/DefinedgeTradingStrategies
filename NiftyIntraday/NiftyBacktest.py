from datetime import datetime, timedelta
from dateutil import parser
import pandas as pd
import numpy as np
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
from pprint import pprint

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

backtest = mongo_client['Bots']["backtest"]

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
    print("Backtesting Started")
    api_token = "618a0b4c-f173-407e-acdc-0f61080f856c"
    api_secret = "TbfcWNtKL7vaXfPV3m6pKQ=="
    exchange = "NSE"
    trading_symbol = "Nifty Fin Service"
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
    df_daily.dropna(subset=['ema_low', 'ema_high'], inplace=True)
    df_daily['trend'] = np.where(df_daily['close'] <= df_daily['ema_low'], 'Bearish', 'Bullish')
    #print(df_daily)

    df_15min = fetch_historical_data(conn, exchange, trading_symbol, start, end, 'min')
    df_15min = resample_ohlc_data(df_15min, frequency)
    df_15min.drop(['volume'], axis='columns', inplace=True)
    df_1min = fetch_historical_data(conn, exchange, trading_symbol, start, end, 'min')
    df_1min['datetime'] = pd.to_datetime(df_1min['datetime'])

    df_daily_dic =  df_daily.to_dict(orient='records')
    df_15min_dic =  df_15min.to_dict(orient='records')
    df_1min_dic =  df_1min.to_dict(orient='records')
    trades = []
    sl = 0
    entry_time = None
    for day in df_daily_dic:
        if day['trend'] == "Bullish":
            traded = False
            desired_date = day['datetime'].date()
            # Filter the DataFrame for the desired date
            filtered_df_15 = df_15min[df_15min['datetime'].dt.date >  desired_date]
            if filtered_df_15.empty:
                print('Intraday 15 minute DataFrame is empty!')
                break
            desired_date = filtered_df_15['datetime'].iloc[0].date()
            filtered_df_15 = df_15min[df_15min['datetime'].dt.date ==  desired_date]
            filtered_df_1 = df_1min[df_1min['datetime'].dt.date ==  desired_date]
            high = filtered_df_15['high'].iloc[0]
            low = filtered_df_15['low'].iloc[0]
            for index, row in filtered_df_1.iterrows():
                if row['close'] < low and traded == False:  # If low is breached, avoid entry
                    print('Low of the first candle is breached. Avoiding entry.')
                    break
                if row['close'] > high and traded == False:
                    traded = True
                    entry = row['close']
                    initial_sl = round_to_nearest((0.001 * entry), base=0.05)
                    sl = round_to_nearest((entry - initial_sl), base=0.05)
                    entry_time = row['datetime'].strftime('%H:%M')
                    print()
                    print("********************")
                    print("Trade Date: ", desired_date)
                    print("Trade Type: ", day['trend'])
                    print("Entry: ", entry)
                    print("Abolute_SL: ", initial_sl)
                    print("SL: ", sl)
                    print("********************")
            
            prev_high = None
            if traded == True:
                for index, row in filtered_df_15.iloc[1:].iterrows():
                    if row['low'] <= sl and row['datetime'].strftime('%H:%M') > entry_time:
                        trade = {
                            'date': desired_date,
                            'entry_time': entry_time,
                            'Trade Type': day['trend'],
                            'entry': entry,
                            'initial_sl': round_to_nearest((entry - initial_sl), base=0.05),
                            'trailing_sl': sl,
                            'exit': sl,
                            'pnl': round_to_nearest((sl-entry), base=0.05),
                            'exit_reason': 'SL/Trailing SL'
                        }
                        trades.append(trade)
                        break
                    if row['datetime'].strftime('%H:%M') == '14:45':
                        trade = {
                            'date': desired_date,
                            'entry_time': entry_time,
                            'Trade Type': day['trend'],
                            'entry': entry,
                            'initial_sl': round_to_nearest((entry - initial_sl), base=0.05),
                            'trailing_sl': sl,
                            'exit': row['close'],
                            'pnl': round_to_nearest((row['close']-entry), base=0.05),
                            'exit_reason': 'Closing Time'
                        }
                        trades.append(trade)
                        break
                    if prev_high is not None and row['close'] > prev_high:
                        sl = row['low']  # Update SL to the low of the current candle
                    if prev_high is None or row['high'] > prev_high:
                        prev_high = row['high']
        if day['trend'] == "Bearish":
            traded = False
            desired_date = day['datetime'].date()
            # Filter the DataFrame for the desired date
            filtered_df_15 = df_15min[df_15min['datetime'].dt.date >  desired_date]
            if filtered_df_15.empty:
                print('Intraday 15 minute DataFrame is empty!')
                break
            desired_date = filtered_df_15['datetime'].iloc[0].date()
            filtered_df_15 = df_15min[df_15min['datetime'].dt.date ==  desired_date]
            filtered_df_1 = df_1min[df_1min['datetime'].dt.date ==  desired_date]
            high = filtered_df_15['high'].iloc[0]
            low = filtered_df_15['low'].iloc[0]
            for index, row in filtered_df_1.iterrows():
                if row['close'] > high and traded == False:  # If low is breached, avoid entry
                    print('High of the first candle is breached. Avoiding entry.')
                    break
                if row['close'] < low and traded == False:
                    traded = True
                    entry = row['close']
                    initial_sl = round_to_nearest((0.001 * entry), base=0.05)
                    sl = round_to_nearest((entry + initial_sl), base=0.05)
                    entry_time = row['datetime'].strftime('%H:%M')
                    print()
                    print("********************")
                    print("Trade Date: ", desired_date)
                    print("Trade Type: ", day['trend'])
                    print("Entry: ", entry)
                    print("Abolute_SL: ", initial_sl)
                    print("SL: ", sl)
                    print("********************")
            
            prev_low = None
            if traded == True:
                for index, row in filtered_df_15.iloc[1:].iterrows():
                    if row['high'] >= sl and row['datetime'].strftime('%H:%M') > entry_time:
                        trade = {
                            'date': desired_date,
                            'entry_time': entry_time,
                            'Trade Type': day['trend'],
                            'entry': entry,
                            'initial_sl': round_to_nearest((entry + initial_sl), base=0.05),
                            'trailing_sl': sl,
                            'exit': sl,
                            'pnl': round_to_nearest((entry-sl), base=0.05),
                            'exit_reason': 'SL/Trailing SL'
                        }
                        trades.append(trade)
                        break
                    if row['datetime'].strftime('%H:%M') == '14:45':
                        trade = {
                            'date': desired_date,
                            'entry_time': entry_time,
                            'Trade Type': day['trend'],
                            'entry': entry,
                            'initial_sl': round_to_nearest((entry + initial_sl), base=0.05),
                            'trailing_sl': sl,
                            'exit': row['close'],
                            'pnl': round_to_nearest((entry-row['close']), base=0.05),
                            'exit_reason': 'Closing Time'
                        }
                        trades.append(trade)
                        break
                    if prev_low is not None and row['close'] < prev_low:
                        sl = row['high']  # Update SL to the low of the current candle
                    if prev_low is None or row['low'] < prev_low:
                        prev_low = row['low']
    trades_df = pd.DataFrame(trades)
    trades_df['cumulative_pnl'] = trades_df['pnl'].cumsum()
    print(trades_df)

if __name__ == "__main__":
    main()
