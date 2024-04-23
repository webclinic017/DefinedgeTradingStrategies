import pyotp
from integrate import ConnectToIntegrate, IntegrateData, IntegrateOrders
from logging import INFO, basicConfig, getLogger
import pandas as pd
from slack_sdk import WebClient
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from lib import connect_definedge as edge
from lib import utils as util
from lib import ta
pd.set_option('display.max_rows', None)

from datetime import datetime, timedelta
import requests
import zipfile
import io


basicConfig(level=INFO)
logger = getLogger()
slack_channel = "niftyweekly"


def login_to_integrate(api_token: str, api_secret: str) -> ConnectToIntegrate:
    """
    NZKUOQTJKBAVK3KNPBYUMRDTOBWUU2KV
    Login to Integrate and return the connection object.
    """
    conn = ConnectToIntegrate()
    totp = pyotp.TOTP("NZKUOQTJKBAVK3KNPBYUMRDTOBWUU2KV").now()
    conn.login(api_token=api_token, api_secret=api_secret, totp=totp)
    return conn


def get_orders(conn: ConnectToIntegrate):
    io = IntegrateOrders(conn)
    return io.orders()

def get_order_by_order_id(conn: ConnectToIntegrate, order_id):
    io = IntegrateOrders(conn)
    print("Getting order by order ID")
    order = io.order(order_id)
    print(order)
    return order


def notify(message):
    channel = "#" + slack_channel
    client = WebClient(token='')
    client.chat_postMessage(
        channel=channel, 
        text=message, 
        username="TradeBot",
        icon_emoji=":chart_with_upwards_trend:"
    )


def main():
    api_token = "618a0b4c-f173-407e-acdc-0f61080f856c"
    api_secret = "TbfcWNtKL7vaXfPV3m6pKQ=="
    conn = login_to_integrate(api_token, api_secret)
    #days_ago = datetime.now() - timedelta(days=90)
    days_ago = datetime(2024,1,15)
    start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
    df = ta.renko(conn, 'NSE', 'Nifty 50', start, datetime.today(), 'min', .1)
    #ohlc = edge.fetch_historical_data(conn, 'NFO', 'NIFTY18APR24P22600', start, datetime.today(), 'min')
    renko_st = ta.supertrend(df, 40, 10)
    print(renko_st.iloc[-100:-1])
    #print(ohlc.iloc[-100:-1])



if __name__ == "__main__":
    main()




    