from pymongo import MongoClient
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from lib import connect_definedge as edge
from lib import utils as util
import requests
from logging import INFO, basicConfig, getLogger
import time
import zipfile
from retry import retry
import io
import datetime 
from datetime import timedelta
from dateutil import parser
import pandas as pd
from slack_sdk import WebClient
pd.set_option('display.max_rows', None)

"""
slack_url = os.environ.get('slack_url')
slack_channel = os.environ.get('slack_channel')
CONNECTION_STRING = os.environ.get('CONNECTION_STRING')  #Mongo Connection
user_name = os.environ.get('user_name')
quantity = os.environ.get('quantity')
trade_start_time = parser.parse("9:29:00").time()
trade_end_time = parser.parse(str(os.environ.get('trade_end_time'))).time()
slack_client = WebClient(token=os.environ.get('slack_client'))
"""

slack_channel = "niftyweekly"
CONNECTION_STRING = "mongodb+srv://adminuser:05NZN7kKp5D4TZnU@bots.vnitakj.mongodb.net/?retryWrites=true&w=majority"  # Mongo Connection
user_name = "sugam"
quantity = '50'
trade_start_time = parser.parse("9:20:00").time()
trade_end_time = parser.parse("15:28:00").time()
slack_client = WebClient(token=os.environ.get('slack_client'))

api_token = "618a0b4c-f173-407e-acdc-0f61080f856c"
api_secret = "TbfcWNtKL7vaXfPV3m6pKQ=="

mongo_client = MongoClient(CONNECTION_STRING)

strategies_collection_name = "nifty_weekly" + "_" + user_name
orders_collection_name = "orders_nifty_weekly" + "_" + user_name

# trades collection
strategies = mongo_client['Bots'][strategies_collection_name]
orders = mongo_client['Bots'][orders_collection_name]  # orders collection


basicConfig(level=INFO)
logger = getLogger()

@retry(tries=5, delay=5, backoff=2)
def notify(message):
    channel = "#" + slack_channel
    print(message)
    slack_client.chat_postMessage(
        channel=channel, 
        text=message, 
        username="TradeBot",
        icon_emoji=":chart_with_upwards_trend:"
    )

@retry(tries=5, delay=5, backoff=2)
def get_supertrend_direction():
    supertrend_collection = mongo_client['Bots']["supertrend"]
    supertrend = supertrend_collection.find_one({"_id": "supertrend"})
    print(f"Nifty Trend: {supertrend['signal']}")
    return supertrend["signal"]


@retry(tries=5, delay=5, backoff=2)
def get_supertrend_value():
    supertrend_collection = mongo_client['Bots']["supertrend"]
    supertrend = supertrend_collection.find_one({"_id": "supertrend"})
    print(f"Super Trend Value: {supertrend['value']}")
    return supertrend["value"]


@retry(tries=5, delay=5, backoff=2)
def get_nifty_close():
    supertrend_collection = mongo_client['Bots']["supertrend"]
    supertrend = supertrend_collection.find_one({"_id": "supertrend"})
    print(f"Nifty Close: {supertrend['close']}")
    return supertrend['close']


@retry(tries=5, delay=5, backoff=2)
def place_buy_order(api_token, api_secret, symbol, qty):
    conn = edge.login_to_integrate(api_token, api_secret)
    io = edge.IntegrateOrders(conn)
    order = io.place_order(
        exchange=conn.EXCHANGE_TYPE_NFO,
        order_type=conn.ORDER_TYPE_BUY,
        price=0,
        price_type=conn.PRICE_TYPE_MARKET,
        product_type=conn.PRODUCT_TYPE_NORMAL,
        quantity=qty,
        tradingsymbol=symbol,
    )
    orders.insert_one(order)
    order_id = order['order_id']
    order = get_order_by_order_id(conn, order_id)
    print(f"Order Status: {order['order_status']}")
    if order['order_status'] != "COMPLETE":
        print(f"Order Message: {order['message']}")
        raise Exception("Error in placing order - " +
                    str(order['message']))
    print(f"Order placed: {order}")
    notify(f"Order placed: {order}")
    orders.insert_one(order)
    return order


@retry(tries=5, delay=5, backoff=2)
def place_sell_order(api_token, api_secret, symbol, qty):
    conn = edge.login_to_integrate(api_token, api_secret)
    io = edge.IntegrateOrders(conn)
    order = io.place_order(
        exchange=conn.EXCHANGE_TYPE_NFO,
        order_type=conn.ORDER_TYPE_SELL,
        price=0,
        price_type=conn.PRICE_TYPE_MARKET,
        product_type=conn.PRODUCT_TYPE_NORMAL,
        quantity=qty,
        tradingsymbol=symbol,
    )
    orders.insert_one(order)
    order_id = order['order_id']
    order = get_order_by_order_id(conn, order_id)
    print(f"Order Status: {order['order_status']}")
    if order['order_status'] != "COMPLETE":
        print(f"Order Message: {order['message']}")
        raise Exception("Error in placing order - " +
                    str(order['message']))
    print(f"Order placed: {order}")
    notify(f"Order placed: {order}")
    orders.insert_one(order)
    return order


@retry(tries=5, delay=5, backoff=2)
def get_order_by_order_id(conn: edge.ConnectToIntegrate, order_id):
    io = edge.IntegrateOrders(conn)
    print(f"Getting order by order ID: {order_id}")
    order = io.order(order_id)
    print(order)
    return order


@retry(tries=5, delay=5, backoff=2)
def get_nifty_atm(conn: edge.ConnectToIntegrate):
    return round(50 * round(float(get_supertrend_value())/50), 2)



@retry(tries=5, delay=5, backoff=2)
def load_csv_from_zip(url='https://app.definedgesecurities.com/public/allmaster.zip'):
    column_names = ['SEGMENT', 'TOKEN', 'SYMBOL', 'TRADINGSYM', 'INSTRUMENT TYPE', 'EXPIRY', 'TICKSIZE', 'LOTSIZE', 'OPTIONTYPE', 'STRIKE', 'PRICEPREC', 'MULTIPLIER', 'ISIN', 'PRICEMULT', 'UnKnown']
    # Send a GET request to download the zip file
    response = requests.get(url)
    response.raise_for_status()  # This will raise an exception for HTTP errors
    # Open the zip file from the bytes-like object
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        # Extract the name of the first CSV file in the zip archive
        csv_name = thezip.namelist()[0]
        # Extract and read the CSV file into a pandas DataFrame
        with thezip.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file, header=None, names=column_names, low_memory=False, on_bad_lines='skip')
    df = df[(df['SEGMENT'] == 'NFO') & (df['INSTRUMENT TYPE'] == 'OPTIDX')]
    df = df[(df['SYMBOL'].str.startswith('NIFTY'))]
    df['EXPIRY'] = pd.to_datetime(df['EXPIRY'], format='%d%m%Y', errors='coerce')
    df = df.sort_values(by='EXPIRY', ascending=True)
    # Return the loaded DataFrame
    return df



@retry(tries=5, delay=5, backoff=2)
def get_option_symbol(strike=19950, option_type = "PE" ):
    df = load_csv_from_zip()
    df = df[df['TRADINGSYM'].str.contains(str(strike))]
    df = df[df['OPTIONTYPE'].str.match(option_type)]
    # Get the current date
    current_date = datetime.datetime.now()
    # Calculate the start and end dates of the current week
    start_of_week = current_date - timedelta(days=current_date.weekday())
    end_of_week = start_of_week + timedelta(days=12)
    df= df[(df['EXPIRY'] >= start_of_week) & (df['EXPIRY'] > (current_date + timedelta(days=1))) & (df['EXPIRY'] <= end_of_week)]
    df = df.head(1)
    print("Getting options Symbol...")
    print(f"Symbol: {df['TRADINGSYM'].values[0]} , Expiry: {df['EXPIRY'].values[0]}")
    return df['TRADINGSYM'].values[0], df['EXPIRY'].values[0]




@retry(tries=5, delay=5, backoff=2)
def create_bull_put_spread(api_token, api_secret):
    conn = edge.login_to_integrate(api_token, api_secret)
    option_type = "PE"
    atm = get_nifty_atm(conn)
    nifty_close = get_nifty_close()
    sell_strike = atm + 50
    buy_strike = sell_strike - 300
    sell_strike_symbol, expiry = get_option_symbol(sell_strike, option_type)
    buy_strike_symbol, expiry = get_option_symbol(buy_strike, option_type)
    print(expiry)
    expiry = str(expiry)
    expiry = parser.parse(expiry).date()
    print(expiry)
    # buy_order = place_buy_order(api_token, api_secret, buy_strike_symbol, quantity)
    # if buy_order['order_status'] == "COMPLETE":
    #     sell_order = place_sell_order(api_token, api_secret, sell_strike_symbol, quantity)
    # short_option_cost = sell_order['average_traded_price']
    # long_option_cost = buy_order['average_traded_price']
    days_ago = datetime.datetime.now() - timedelta(days=7)
    start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
    short_option_cost = edge.get_option_price(conn, 'NFO', sell_strike_symbol, start, datetime.datetime.today(), 'min')
    long_option_cost = edge.get_option_price(conn, 'NFO', buy_strike_symbol, start, datetime.datetime.today(), 'min')
    notify("created bull put spread!")
    record_details_in_mongo(sell_strike_symbol, buy_strike_symbol, "Bullish", nifty_close, expiry, short_option_cost, long_option_cost)



@retry(tries=5, delay=5, backoff=2)
def record_details_in_mongo(sell_strike_symbol, buy_strike_symbol, trend, nifty_close, expiry, short_option_cost, long_option_cost):
    strategy = {
    'instrument_name': 'Nifty',
    'quantity': int(quantity),
    'lot_size': 50,
    'short_exit_price': 0,
    'long_exit_price': 0,
    'strategy_state': 'active',
    'entry_date': str(datetime.datetime.now().date()),
    'exit_date': '',
    'trend' : trend,
    'short_option_symbol' : sell_strike_symbol,
    'long_option_symbol' : buy_strike_symbol,
    'short_option_cost' : short_option_cost,
    'long_option_cost' : long_option_cost,
    'entry_time' : datetime.datetime.now().strftime('%H:%M'),
    'exit_time' : '',
    'nifty_close' : round(nifty_close,2),
    'expiry' : str(expiry),
    'pnl': ''
    }
    strategies.insert_one(strategy)



@retry(tries=5, delay=5, backoff=2)
def create_bear_call_spread(api_token, api_secret):
    conn = edge.login_to_integrate(api_token, api_secret)
    option_type = "CE"
    atm = get_nifty_atm(conn)
    nifty_close = get_nifty_close()
    sell_strike = atm - 50
    buy_strike = sell_strike + 300
    sell_strike_symbol, expiry = get_option_symbol(sell_strike, option_type)
    buy_strike_symbol, expiry = get_option_symbol(buy_strike, option_type)
    print(expiry)
    expiry = str(expiry)
    expiry = parser.parse(expiry).date()
    print(expiry)
    # buy_order = place_buy_order(api_token, api_secret, buy_strike_symbol, quantity)
    # if buy_order['order_status'] == "COMPLETE":
    #     sell_order = place_sell_order(api_token, api_secret, sell_strike_symbol, quantity)
    # short_option_cost = sell_order['average_traded_price']
    # long_option_cost = buy_order['average_traded_price']
    days_ago = datetime.datetime.now() - timedelta(days=7)
    start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
    short_option_cost = edge.get_option_price(conn, 'NFO', sell_strike_symbol, start, datetime.datetime.today(), 'min')
    long_option_cost = edge.get_option_price(conn, 'NFO', buy_strike_symbol, start, datetime.datetime.today(), 'min')
    notify("created bear call spread!")
    record_details_in_mongo(sell_strike_symbol, buy_strike_symbol, "Bearish", nifty_close, expiry, short_option_cost, long_option_cost)

def calculate_pnl(quantity, long_entry, long_exit, short_entry, short_exit):
    pnl = float(quantity) * ((float(short_entry) - float(short_exit)) + (float(long_exit) - float(long_entry)))
    notify(f"Realized Gains: {round(pnl, 2)}")
    return round(pnl, 2)

@retry(tries=5, delay=5, backoff=2)
def close_active_positions(api_token, api_secret):
    print("Closing active positions")
    notify("Closing active positions")
    active_strategies = strategies.find({'strategy_state': 'active'})
    for strategy in active_strategies:
        #buy_order = place_buy_order(api_token, api_secret, strategy['short_option_symbol'], strategy['quantity'])
        #notify("Short option leg closed")
        # if buy_order['order_status'] == "COMPLETE":
        #     sell_order = place_sell_order(api_token, api_secret, strategy['long_option_symbol'], strategy['quantity'])
        #     notify("Long option leg closed")
        #     strategies.update_one({'_id': strategy['_id']}, {'$set': {'strategy_state': 'closed'}})
        #     strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_date': str(datetime.datetime.now().date())}})
        #     strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_time': datetime.datetime.now().strftime('%H:%M')}})
        #     strategies.update_one({'_id': strategy['_id']}, {'$set': {'short_exit_price': buy_order['average_traded_price']}})
        #     strategies.update_one({'_id': strategy['_id']}, {'$set': {'long_exit_price': sell_order['average_traded_price']}})
        #     pnl = calculate_pnl(strategy['quantity'], strategy['long_option_cost'], sell_order['average_traded_price'], strategy['short_option_cost'],buy_order['average_traded_price'])
        #     strategies.update_one({'_id': strategy['_id']}, {'$set': {'pnl': pnl}})
        #notify("Long option leg closed")
        notify("Short option leg closed")
        notify("Long option leg closed")
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'strategy_state': 'closed'}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_date': str(datetime.datetime.now().date())}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_time': datetime.datetime.now().strftime('%H:%M')}})
        days_ago = datetime.now() - timedelta(days=7)
        start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
        conn = edge.login_to_integrate(api_token, api_secret)
        short_exit_price = edge.get_option_price(conn, 'NFO', strategy['short_option_symbol'], start, datetime.datetime.today(), 'min')
        long_exit_price = edge.get_option_price(conn, 'NFO', strategy['long_option_symbol'], start, datetime.datetime.today(), 'min')
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'short_exit_price': short_exit_price}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'long_exit_price': long_exit_price}})
        pnl = calculate_pnl(strategy['quantity'], strategy['long_option_cost'], long_exit_price, strategy['short_option_cost'],short_exit_price)
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'pnl': pnl}})
    return


def main():
    notify("Nifty Positional bot kicked off")
    print("Nifty Positional bot kicked off")
    util.notify(f"Supertrend Direction: {get_supertrend_direction()}")
    util.notify(f"Supertrend Value: {get_supertrend_value()}")
    iteration = 0
    while True:
        if iteration % 18 == 0:
            util.notify(message=f"Nifty Weekly option Selling bot is Alive!", slack_client=slack_client)
            util.notify(message=f"current time from NiftyWeeklyOptionSelling: {current_time}", slack_client=slack_client)
        current_time = datetime.datetime.now().time()
        print(f"current time: {current_time}")
        if current_time > trade_start_time:
            print("Trading Window is active.")
            if strategies.count_documents({'strategy_state': 'active'}) > 0:
                active_strategies = strategies.find(
                    {'strategy_state': 'active'})
                for strategy in active_strategies:
                    if strategy['trend'] != get_supertrend_direction():
                        notify(f"Supertrend Direction Changed to {get_supertrend_direction()}")
                        close_active_positions(api_token, api_secret)
                        break

                    if strategy['trend'] == 'Bullish' and get_supertrend_value() > (strategy['nifty_close']):
                        notify("Supertrend crossed inception point, shift the strikes!")
                        close_active_positions(api_token, api_secret)
                        break

                    if strategy['trend'] == 'Bearish' and get_supertrend_value() < (strategy['nifty_close']):
                        notify("Supertrend crossed inception point, shift the strikes!")
                        close_active_positions(api_token, api_secret)
                        break

                    if current_time > datetime.time(hour=15, minute=00) and strategy['expiry'] == str(datetime.datetime.now().date()):
                        notify("Rolling over positions to next expiry")
                        close_active_positions(api_token, api_secret)
                        break
            else:
                if get_supertrend_direction() == 'Bullish':
                    create_bull_put_spread(api_token, api_secret)
                elif get_supertrend_direction() == 'Bearish':
                    create_bear_call_spread(api_token, api_secret)
        
        if current_time > trade_end_time:
            notify("Closing Bell, Bot will exit now")
            return   
        time.sleep(10)
        iteration = iteration + 1
if __name__ == "__main__":
    main()
