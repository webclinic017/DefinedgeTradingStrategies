import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from lib import connect_definedge as edge
from lib import utils as util

import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser

import numpy as np
pd.set_option('display.max_rows', None)
from pymongo import MongoClient


"""
slack_url = os.environ.get('slack_url')
slack_channel = os.environ.get('slack_channel')
CONNECTION_STRING = os.environ.get('CONNECTION_STRING')  #Mongo Connection
trade_end_time = parser.parse(str(os.environ.get('trade_end_time'))).time()
"""

slack_channel = "niftyweekly"
CONNECTION_STRING = "mongodb+srv://adminuser:05NZN7kKp5D4TZnU@bots.vnitakj.mongodb.net/?retryWrites=true&w=majority" #Mongo Connection
trade_end_time = parser.parse("15:25:00").time()
trade_start_time = parser.parse("09:16:00").time()

mongo_client = MongoClient(CONNECTION_STRING)
slack_client = util.get_slack_client('')


def main():
    print("Backtesting Started")
    util.notify("Backtesting Started",slack_channel = slack_channel, slack_client = slack_client)
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

    conn = edge.login_to_integrate(api_token, api_secret)
    df_daily = edge.fetch_historical_data(conn, exchange, trading_symbol, start, end, 'day')
    df_daily.drop(['volume'], axis='columns', inplace=True)
    df_daily['ema_low'] = df_daily['low'].ewm(com=10, min_periods = 21).mean() #Very close to TradingView
    df_daily['ema_high'] = df_daily['high'].ewm(com=10, min_periods = 21).mean() #Very close to TradingView

    df_daily['ema_low'] = df_daily['ema_low'].round(2)
    df_daily['ema_high'] = df_daily['ema_high'].round(2)
    df_daily.dropna(subset=['ema_low', 'ema_high'], inplace=True)
    df_daily['trend'] = np.where(df_daily['close'] <= df_daily['ema_low'], 'Bearish', 'Bullish')
    #print(df_daily)

    df_15min = edge.fetch_historical_data(conn, exchange, trading_symbol, start, end, 'min')
    df_15min = util.resample_ohlc_data(df_15min, frequency)
    df_15min.drop(['volume'], axis='columns', inplace=True)
    df_1min = edge.fetch_historical_data(conn, exchange, trading_symbol, start, end, 'min')
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
                    initial_sl = util.round_to_nearest((0.001 * entry), base=0.05)
                    sl = util.round_to_nearest((entry - initial_sl), base=0.05)
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
                            'initial_sl': util.round_to_nearest((entry - initial_sl), base=0.05),
                            'trailing_sl': sl,
                            'exit': sl,
                            'pnl': util.round_to_nearest((sl-entry), base=0.05),
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
                            'initial_sl': util.round_to_nearest((entry - initial_sl), base=0.05),
                            'trailing_sl': sl,
                            'exit': row['close'],
                            'pnl': util.round_to_nearest((row['close']-entry), base=0.05),
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
                    initial_sl = util.round_to_nearest((0.001 * entry), base=0.05)
                    sl = util.round_to_nearest((entry + initial_sl), base=0.05)
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
                            'initial_sl': util.round_to_nearest((entry + initial_sl), base=0.05),
                            'trailing_sl': sl,
                            'exit': sl,
                            'pnl': util.round_to_nearest((entry-sl), base=0.05),
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
                            'initial_sl': util.round_to_nearest((entry + initial_sl), base=0.05),
                            'trailing_sl': sl,
                            'exit': row['close'],
                            'pnl': util.round_to_nearest((entry-row['close']), base=0.05),
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
