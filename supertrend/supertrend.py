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
import sys
import os
from slack_sdk import WebClient
from pymongo import MongoClient

"""
slack_url = os.environ.get('slack_url')
slack_channel = os.environ.get('slack_channel')
CONNECTION_STRING = os.environ.get('CONNECTION_STRING')  #Mongo Connection
trade_end_time = parser.parse(str(os.environ.get('trade_end_time'))).time()
"""

slack_channel = "niftyweekly"
slack_client = WebClient(token=os.environ.get('slack_token'))
CONNECTION_STRING = "mongodb+srv://adminuser:05NZN7kKp5D4TZnU@bots.vnitakj.mongodb.net/?retryWrites=true&w=majority" #Mongo Connection
trade_end_time = parser.parse("15:28:00").time()
trade_start_time = parser.parse("09:16:00").time()

mongo_client = MongoClient(CONNECTION_STRING)
collection_name = "supertrend"

supertrend_collection = mongo_client['Bots'][collection_name]

def get_supertrend_start_date():
    supertrend = supertrend_collection.find_one({"_id": "supertrend"})
    return supertrend["start_date"]


def main():
    print("Supertrend Started")
    iteration = 0
    while True:
        if iteration % 35 == 0:
            util.notify("Super Trend Bot is active!", slack_client=slack_client)
        current_time = datetime.now().time()
        if current_time > trade_start_time:
            api_token = "618a0b4c-f173-407e-acdc-0f61080f856c"
            api_secret = "TbfcWNtKL7vaXfPV3m6pKQ=="
            exchange = "NSE"
            trading_symbol = "Nifty 50"
            # Calculate 60 days ago from today

            if supertrend_collection.count_documents({"_id": "supertrend"}) == 0:
                days_ago = datetime.now() - timedelta(days=95)
            else:
                days_ago = get_supertrend_start_date()
                if days_ago < datetime.now() - timedelta(days=180):
                    days_ago = datetime.now() - timedelta(days=95)

            # Set the time to 9:15 AM on that date
            start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
            end = datetime.today()

            conn = edge.login_to_integrate(api_token, api_secret)
            df = ta.renko(conn, exchange, trading_symbol, start, end)
            
            print("\n***** Fetched 1 min Renko Data *****\n")
            df= ta.supertrend(df, 40, 10)
            print(df.iloc[-1])

            if supertrend_collection.count_documents({"_id": "supertrend"}) == 0:
                st = {"_id": "supertrend", "datetime": df.iloc[-1]['datetime'], "value": df.iloc[-1]['ST'], "signal": df.iloc[-1]['signal'], "start_date": start}
                supertrend_collection.insert_one(st)
            else:
                supertrend_collection.update_one({'_id': "supertrend"}, {'$set': {"datetime": df.iloc[-1]['datetime'],
                            "value": df.iloc[-1]['ST'], "close": df.iloc[-1]['close'], "signal": df.iloc[-1]['signal'], "start_date": start}})
        
        print("repeating loop for Supertrend")
        if current_time > trade_end_time:
            return
        
        time.sleep(10)
        iteration = iteration + 1

if __name__ == "__main__":
    main()
