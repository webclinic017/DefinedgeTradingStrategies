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
user_name = os.environ.get('user_name')
trade_start_time = parser.parse("9:29:00").time()
trade_end_time = parser.parse(str(os.environ.get('trade_end_time'))).time()
"""

slack_url = "https://hooks.slack.com/services/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"
slack_channel = "straddlebot"
CONNECTION_STRING = "mongodb+srv://adminuser:05NZN7kKp5D4TZnU@bots.vnitakj.mongodb.net/?retryWrites=true&w=majority" #Mongo Connection
user_name = "sugam"
trade_start_time = parser.parse("9:29:00").time()
trade_end_time = parser.parse("15:25:00").time()

mongo_client = MongoClient(CONNECTION_STRING)

strategies_collection_name = "nifty_weekly" + "_" + user_name
orders_collection_name = "orders_nifty_weekly" + "_" + user_name
trade_diary_collection_name = "nifty_weekly_trade_diary" + "_" + user_name

# trades collection
strategies = mongo_client['Bots'][strategies_collection_name]
orders = mongo_client['Bots'][orders_collection_name]  # orders collection
# trade_diary collection
trade_diary = mongo_client['Bots'][trade_diary_collection_name]


def get_supertrend_direction():
    supertrend_collection = mongo_client['Bots']["supertrend"]
    supertrend = supertrend_collection.find_one({"_id": "supertrend"})
    return supertrend["signal"]


def create_bull_put_spread():
    return

def create_bear_call_spread():
    return

def close_active_positions():
    return



def main():
    while True:
        current_time = datetime.now().time()
        if current_time > trade_start_time:
            print("Started Monitoring")
            if strategies.count_documents({'strategy_state': 'active'}) > 0:
                active_strategies = strategies.find({'strategy_state': 'active'})
                for strategy in active_strategies:
                    if strategy['trend'] == get_supertrend_direction():
                        continue
                    elif strategy['trend'] != get_supertrend_direction():
                        close_active_positions()
            else:
                if get_supertrend_direction == 'Bullish':
                    create_bull_put_spread()
                elif get_supertrend_direction == 'Bearish':
                    create_bear_call_spread()
        if current_time > trade_end_time:
            return    



if __name__ == "__main__":
    main()