from integrate import ConnectToIntegrate, IntegrateData
import pandas as pd
import pyotp
from datetime import datetime
import requests
import zipfile
import io
from retry import retry

@retry(tries=5, delay=5, backoff=2)
def login_to_integrate(api_token: str, api_secret: str) -> ConnectToIntegrate:
    """
    NZKUOQTJKBAVK3KNPBYUMRDTOBWUU2KV
    Login to Integrate and return the connection object.
    """
    conn = ConnectToIntegrate()
    totp = pyotp.TOTP("NZKUOQTJKBAVK3KNPBYUMRDTOBWUU2KV").now()
    conn.login(api_token=api_token, api_secret=api_secret, totp=totp)
    return conn

@retry(tries=5, delay=5, backoff=2)
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

@retry(tries=5, delay=5, backoff=2)
def get_option_price(api_token: str, api_secret: str, exchange: str , trading_symbol: str, start: datetime, end: datetime, interval = 'min'):
    conn = login_to_integrate(api_token, api_secret)
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
    return df['close'].iloc[-1]  

@retry(tries=5, delay=5, backoff=2)
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