import pyotp
from integrate import ConnectToIntegrate, IntegrateData, IntegrateOrders
from logging import INFO, basicConfig, getLogger
import pandas as pd
from slack_sdk import WebClient
pd.set_option('display.max_rows', None)


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

def main():
    api_token = "618a0b4c-f173-407e-acdc-0f61080f856c"
    api_secret = "TbfcWNtKL7vaXfPV3m6pKQ=="
    conn = login_to_integrate(api_token, api_secret)
#    get_order_by_order_id(conn, order_id="24030100001302")
#    get_order_by_order_id(conn, order_id="24030100000874")
    message = "This is my first, Slack message from Python!"

    # Set up a WebClient with the Slack OAuth token
    client = WebClient(token='xoxb-4845492646177-6765730768929-As0WGXYuBUlGTIHDFdgbM5oO')

    # Send a message
    # client.chat_postMessage(
    #     channel="#niftyweekly", 
    #     text=message, 
    #     username="TradeBot"
    # )
    notify("test message")



if __name__ == "__main__":
    main()




    