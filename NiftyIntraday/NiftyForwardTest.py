import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from lib import connect_definedge as edge
from lib import utils as util
from lib import ta
from datetime import datetime, timedelta
from dateutil import parser
import time
from pymongo import MongoClient


slack_channel = os.environ.get('slack_channel')
slack_token = os.environ.get('slack_token')
CONNECTION_STRING = os.environ.get('CONNECTION_STRING')  #Mongo Connection
api_token = os.environ.get('api_token')
api_secret = os.environ.get('api_secret')
instrument_name = os.environ.get('instrument_name')
trading_symbol = os.environ.get('trading_symbol')
quantity = float(os.environ.get('quantity'))
sl_factor = float(os.environ.get('sl_factor'))
 
"""
slack_channel = "niftyweekly"
slack_token =""
CONNECTION_STRING = "mongodb+srv://adminuser:05NZN7kKp5D4TZnU@bots.vnitakj.mongodb.net/?retryWrites=true&w=majority" #Mongo Connection
api_token = "618a0b4c-f173-407e-acdc-0f61080f856c"
api_secret = "TbfcWNtKL7vaXfPV3m6pKQ=="
instrument_name = "NIFTY"
trading_symbol = "Nifty 50"
quantity = 50
sl_factor = .001
"""

frequency = '15T'
exchange = "NSE"

# Variables for fetching historical data
days_ago = datetime.now() - timedelta(days=60)
start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
end = datetime.today()

trade_end_time = parser.parse("15:00:00").time()
trade_start_time = parser.parse("09:30:03").time()
mongo_client = MongoClient(CONNECTION_STRING)
f_test = mongo_client['Bots']['forward_test']
slack_client = util.get_slack_client(slack_token)
query = {'instrument_name': instrument_name, 'strategy_state': 'active'}

def main():
    conn = edge.login_to_integrate(api_token, api_secret)
    util.notify(message=f"Forward Testing Started: {instrument_name}",slack_client=slack_client)
    df_daily = edge.fetch_historical_data(conn, "NSE", trading_symbol, start, end, 'day')
    df_daily = ta.ema_channel(df_daily)
    daily_trend = df_daily.iloc[-1]['trend']
    today = datetime.now()
    today = today.replace(hour=9, minute=15, second=0, microsecond=0)
    future_symbol = edge.get_index_future(instrument_name=instrument_name)
    util.notify(message=f"{instrument_name} trend on Daily Time Frame as per EMA channel: {daily_trend}",slack_client=slack_client)
    high_15min = None
    low_15min = None
    iteration = 0
    while True:
        conn = edge.login_to_integrate(api_token, api_secret)
        current_time = datetime.now().time()
        print(f"current time: {current_time}")
        if current_time > trade_start_time:
            df_15min = edge.fetch_historical_data(conn, conn.EXCHANGE_TYPE_NFO, future_symbol, today, datetime.today(), 'min')
            df_15min = util.resample_ohlc_data(df_15min, frequency)
            print("*** 15 min OHLC data ***")
            print(df_15min.iloc[0])
            df_1min = edge.fetch_historical_data(conn, conn.EXCHANGE_TYPE_NFO, future_symbol, today, datetime.today(), 'min')
            print("*** 1 min OHLC data ***")
            print(df_15min)
            print(df_1min)
            high_15min = df_15min.iloc[0]['high']
            low_15min = df_15min.iloc[0]['low']
            
            if f_test.count_documents(query) == 1:
                if iteration % 18 == 0:
                    util.notify(message="Active Position Found!", slack_client=slack_client)
                    util.notify(message=f"current time: {current_time}", slack_client=slack_client)
                pos = f_test.find_one(query)
                if pos['trend'] == "Bullish":
                    if df_1min.iloc[-1]['low'] <= pos['trailing_sl']:
                        util.notify(message=f"Stop Loss Hit!!: {instrument_name}", slack_client=slack_client)
                        util.notify(message=f"Closing the position: {instrument_name}", slack_client=slack_client)
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'exit_time': datetime.now().strftime('%H:%M')}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'exit_price': pos['trailing_sl']}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'points_captured': util.round_to_nearest((pos['trailing_sl']-pos['entry_price']), base=0.05)}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'pnl': util.round_to_nearest(((pos['trailing_sl']-pos['entry_price']) * quantity), base=0.05)}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'exit_reason': 'SL/Trailing SL'}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'strategy_state': 'closed'}})
                        return
                    if current_time >= trade_end_time:
                        util.notify(message=f"Closing the position: {instrument_name}", slack_client=slack_client)
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'exit_time': datetime.now().strftime('%H:%M')}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'exit_price': df_1min.iloc[-1]['close']}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'points_captured': util.round_to_nearest((df_1min.iloc[-1]['close']-pos['entry_price']), base=0.05)}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'pnl': util.round_to_nearest(((df_1min.iloc[-1]['close']-pos['entry_price']) * quantity), base=0.05)}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'exit_reason': 'Closing Time'}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'strategy_state': 'closed'}})
                        return
                    print(df_15min.iloc[-2])
                    if pos['prev_high'] is not None and df_15min.iloc[-2]['close'] > pos['prev_high']:
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'trailing_sl': df_15min.iloc[-2]['low']}})
                        if iteration % 18 == 0:
                            util.notify(message=f"last 15 min close:  {df_15min.iloc[-2]['close']}", slack_client=slack_client)
                    print(df_15min.iloc[-2]['high'])
                    if pos['prev_high'] is None or df_15min.iloc[-2]['high'] > pos['prev_high']:
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'prev_high': df_15min.iloc[-2]['high']}})
                        if iteration % 18 == 0:
                            util.notify(message=f"last 15 min high:  {df_15min.iloc[-2]['high']}", slack_client=slack_client)
                if pos['trend'] == "Bearish":
                    if df_1min.iloc[-1]['high'] >= pos['trailing_sl']:
                        util.notify(message=f"Closing the position: {instrument_name}", slack_client=slack_client)
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'exit_time': datetime.now().strftime('%H:%M')}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'exit_price': pos['trailing_sl']}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'points_captured': util.round_to_nearest((pos['entry_price']-pos['trailing_sl']), base=0.05)}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'pnl': util.round_to_nearest(((pos['entry_price']-pos['trailing_sl']) * quantity), base=0.05)}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'exit_reason': 'SL/Trailing SL'}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'strategy_state': 'closed'}})
                        return
                    if current_time >= trade_end_time:
                        util.notify(message=f"Closing the position: {instrument_name}", slack_client=slack_client)
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'exit_time': datetime.now().strftime('%H:%M')}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'exit_price': df_1min.iloc[-1]['close']}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'points_captured': util.round_to_nearest((pos['entry_price']-df_1min.iloc[-1]['close']), base=0.05)}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'pnl': util.round_to_nearest(((pos['entry_price']-df_1min.iloc[-1]['close']) * quantity), base=0.05)}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'exit_reason': 'Closing Time'}})
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'strategy_state': 'closed'}})
                        return
                    if pos['prev_low'] is not None and df_15min.iloc[-2]['close'] > pos['prev_low']:
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'trailing_sl': df_15min.iloc[-2]['high']}})
                        if iteration % 18 == 0:
                            util.notify(message=f"last 15 min close:  {df_15min.iloc[-2]['close']}", slack_client=slack_client)
                    if pos['prev_low'] is None or df_15min.iloc[-2]['low'] < pos['prev_low']:
                        f_test.update_one({'_id': pos['_id']}, {'$set': {'prev_low': df_15min.iloc[-2]['low']}})
                        if iteration % 18 == 0:
                            util.notify(message=f"last 15 min high:  {df_15min.iloc[-2]['high']}", slack_client=slack_client)
            else:
                if daily_trend == "Bullish":
                    if df_1min.iloc[-1]['close'] < low_15min:
                        print('Low of the first candle is breached. Avoiding entry.')
                        util.notify(message="Low of the first candle is breached. Avoiding entry.", slack_client=slack_client)
                        return
                    if df_1min.iloc[-1]['close'] > high_15min:
                        entry_price = df_1min.iloc[-1]['close']
                        aboslute_sl = util.round_to_nearest((sl_factor * entry_price), base=0.05)
                        sl_price = util.round_to_nearest((entry_price - aboslute_sl), base=0.05)
                        entry_time = df_1min.iloc[-1]['datetime'].strftime('%H:%M')
                        util.notify(message=f"New Bullish Entry Recorded: {instrument_name}", slack_client=slack_client)
                        strategy = {
                                        'instrument_name': instrument_name,
                                        'date': datetime.now().strftime("%d-%B-%Y"),
                                        'trend': daily_trend,
                                        'strategy_state': 'active',
                                        'symbol': future_symbol,
                                        'entry_time': entry_time,
                                        'entry_price': entry_price,
                                        'absolute_sl': aboslute_sl,
                                        'initial_sl': sl_price,
                                        'trailing_sl': sl_price,
                                        'exit_time': "",
                                        'exit_price': None,
                                        'points_captured': None,
                                        'pnl': None,
                                        'exit_reason': None,
                                        'prev_high': None
                                    }
                        f_test.insert_one(strategy)
                        util.notify(message=str(strategy), slack_client=slack_client)
                elif daily_trend == "Bearish":
                    if df_1min.iloc[-1]['close'] > high_15min:
                        print('High of the first candle is breached. Avoiding entry.')
                        util.notify(message="Low of the first candle is breached. Avoiding entry.", slack_client=slack_client)
                        return
                    if df_1min.iloc[-1]['close'] < low_15min:
                        entry_price = df_1min.iloc[-1]['close']
                        aboslute_sl = util.round_to_nearest((sl_factor * entry_price), base=0.05)
                        sl_price = util.round_to_nearest((entry_price + aboslute_sl), base=0.05)
                        entry_time = df_1min.iloc[-1]['datetime'].strftime('%H:%M')
                        util.notify(message=f"New Bearish Entry Recorded: {instrument_name}", slack_client=slack_client)
                        strategy = {
                                        'instrument_name': instrument_name,
                                        'date': datetime.now().strftime("%d-%B-%Y"),
                                        'trend': daily_trend,
                                        'strategy_state': 'active',
                                        'symbol': future_symbol,
                                        'entry_time': entry_time,
                                        'entry_price': entry_price,
                                        'absolute_sl': aboslute_sl,
                                        'initial_sl': sl_price,
                                        'trailing_sl': sl_price,
                                        'exit_time': "",
                                        'exit_price': None,
                                        'pnl': None,
                                        'exit_reason': None,
                                        'prev_low': None
                                    }
                        f_test.insert_one(strategy)
                        util.notify(message=str(strategy), slack_client=slack_client)
        iteration = iteration + 1
        time.sleep(20)
if __name__ == "__main__":
    main()
