import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from lib import connect_definedge as edge
from lib import utils as util
from lib import ta
from datetime import datetime, timedelta
from dateutil import parser
import pandas as pd
pd.set_option('display.max_rows', None)
import time
from logging import INFO, basicConfig, getLogger
import sys
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
            ten_days_ago = datetime.now() - timedelta(days=60)

            # Set the time to 9:15 AM on that date
            start = ten_days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
            end = datetime.today()

            conn = edge.login_to_integrate(api_token, api_secret)
            df = edge.fetch_historical_data(conn, exchange, trading_symbol, start, end)
            df_15min = util.resample_ohlc_data(df, frequency)
            
            logger.info("\n***** 15-minute OHLC Data *****\n")
            df_15min['ATR'] = ta.atr(df_15min, 10)
            df_15min= ta.supertrend(df_15min, 10, 4)
            print(df_15min.iloc[-2])

            if supertrend_collection.count_documents({"_id": "supertrend"}) == 0:
                st = {"_id": "supertrend", "datetime": df_15min.iloc[-2]['datetime'],
                            "close": df_15min.iloc[-2]['close'], "value": df_15min.iloc[-2]['value'], "running_value": df_15min.iloc[-1]['value'], "signal": df_15min.iloc[-2]['signal']}
                supertrend_collection.insert_one(st)
            else:
                supertrend_collection.update_one({'_id': "supertrend"}, {'$set': {"datetime": df_15min.iloc[-2]['datetime'],
                            "close": df_15min.iloc[-2]['close'], "value": df_15min.iloc[-2]['value'], "running_value": df_15min.iloc[-1]['value'], "signal": df_15min.iloc[-2]['signal']}})
        
        print("repeating loop for Supertrend")
        if current_time > trade_end_time:
            return
        
        time.sleep(10)

if __name__ == "__main__":
    main()
