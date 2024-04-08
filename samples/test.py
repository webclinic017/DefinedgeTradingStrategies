import pyotp
from integrate import ConnectToIntegrate, IntegrateData, IntegrateOrders
from logging import INFO, basicConfig, getLogger
import pandas as pd
from slack_sdk import WebClient
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
    client = WebClient(token='xoxb-4845492646177-6765730768929-As0WGXYuBUlGTIHDFdgbM5oO')
    client.chat_postMessage(
        channel=channel, 
        text=message, 
        username="TradeBot",
        icon_emoji=":chart_with_upwards_trend:"
    )


def get_index_future(url='https://app.definedgesecurities.com/public/allmaster.zip', instrument_name = "NIFTY"):
    current_date = datetime.now()
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
    df = df[(df['SEGMENT'] == 'NFO') & (df['INSTRUMENT TYPE'] == 'FUTIDX')]
    df = df[(df['SYMBOL'] == instrument_name)]
    df['EXPIRY'] = pd.to_datetime(df['EXPIRY'], format='%d%m%Y', errors='coerce')
    df = df.sort_values(by='EXPIRY', ascending=True)
    df= df[df['EXPIRY'] > current_date]
    # Return the loaded DataFrame
    return df.iloc[0]['TRADINGSYM']

def main():
    api_token = "618a0b4c-f173-407e-acdc-0f61080f856c"
    api_secret = "TbfcWNtKL7vaXfPV3m6pKQ=="
    conn = login_to_integrate(api_token, api_secret)
    print(get_index_future())



if __name__ == "__main__":
    main()




    