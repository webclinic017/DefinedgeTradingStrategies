import pandas as pd
pd.set_option('mode.chained_assignment', None)
from lib import connect_definedge as edge
from datetime import datetime
import numpy as np

def atr(DF: pd.DataFrame, period: int):
    df = DF.copy()
    df['high-low'] = abs( df['high'] - df['low'] )
    df['high-previousclose'] = abs( df['high'] - df['close'].shift(1) )
    df['low-previousclose'] = abs( df['low'] - df['close'].shift(1) )
    df['TrueRange'] = df[ ['high-low', 'high-previousclose', 'low-previousclose'] ].max(axis=1, skipna=False)
    df['ATR'] = df['TrueRange'].ewm(com=period, min_periods = period).mean() #Very close to TradingView
    #df['ATR'] = df['TrueRange'].rolling(window=period).mean() #Very close to Definedge
    df['ATR'] = df['ATR'].round(2)
    return df['ATR']


def supertrend(df: pd.DataFrame, period: int, multiplier: int):
    df['ATR'] = atr(df, period)
    df['hl2'] = (df['high'] + df['low']) / 2
    df['basic_upperband'] = df['hl2'] + (df['ATR'] * multiplier)
    df['basic_lowerband'] = df['hl2'] - (df['ATR'] * multiplier)
    df['final_upperband'] = 0
    df['final_lowerband'] = 0
    df['ST'] = 0
    df['in_uptrend'] = True
    df['signal'] = "Bullish"
    for i in range(period, len(df)):
        # Update final bands
        df.at[i, 'final_upperband'] = df.at[i, 'basic_upperband'] if (df.at[i, 'basic_upperband'] < df.at[i-1, 'final_upperband']) or (df.at[i-1, 'close'] > df.at[i-1, 'final_upperband']) else df.at[i-1, 'final_upperband']
        df.at[i, 'final_lowerband'] = df.at[i, 'basic_lowerband'] if (df.at[i, 'basic_lowerband'] > df.at[i-1, 'final_lowerband']) or (df.at[i-1, 'close'] < df.at[i-1, 'final_lowerband']) else df.at[i-1, 'final_lowerband']    
    for current in range(1, len(df.index)):
        previous = current-1
        if df['close'][current] > df['final_upperband'][previous]:
            df['in_uptrend'][current] = True
            df['signal'][current] = "Bullish"
            df['ST'][current] = df['final_lowerband'][current]
        elif df['close'][current] < df['final_lowerband'][previous]:
            df['in_uptrend'][current] = False
            df['signal'][current] = "Bearish"
            df['ST'][current] = df['final_upperband'][current]
        else:
            df['in_uptrend'][current] = df['in_uptrend'][previous]
            if df['in_uptrend'][current]:
                df['ST'][current] = df['final_lowerband'][current]
                df['signal'][current] = "Bullish"
            else:
                df['ST'][current] = df['final_upperband'][current]
                df['signal'][current] = "Bearish"
    df['ST'] = df['ST'].round(2)
    df.drop(['basic_upperband', 'basic_lowerband', 'hl2', 'final_upperband', 'final_lowerband', 'in_uptrend', 'ATR'], axis='columns', inplace=True)
    return df


def ema_channel(df: pd.DataFrame, period = 21):
    df['ema_low'] = df['low'].ewm(com=10, min_periods = period).mean() #calculating EMA
    df['ema_high'] = df['high'].ewm(com=10, min_periods = period).mean() #calculating EMA

    df['ema_low'] = df['ema_low'].round(2)
    df['ema_high'] = df['ema_high'].round(2)
    df.dropna(subset=['ema_low', 'ema_high'], inplace=True)
    df['trend'] = np.where(df['close'] <= df['ema_low'], 'Bearish', 'Bullish')    
    return df

def tma(df: pd.DataFrame):
    df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean() #calculating EMA
    df['ema_30'] = df['close'].ewm(span=30, adjust=False).mean() #calculating EMA
    df['ema_40'] = df['close'].ewm(span=40, adjust=False).mean() #calculating EMA

    df['ema_20'] = df['ema_20'].round(2)
    df['ema_30'] = df['ema_30'].round(2)
    df['ema_40'] = df['ema_40'].round(2)
    return df


def rs(df: pd.DataFrame, nifty: pd.DataFrame):
    stock = df.copy()
    if stock['close'].iloc[-1] < 100:
        multiplier = 10000
    else:
        multiplier = 1000
    stock['close'] = (stock['close'] * multiplier)/nifty['close']
    stock['close'] = stock['close'].round(2)
    stock.drop(['open','high','low','volume'], axis='columns', inplace=True)
    return stock

def renko(conn, exchange: str, trading_symbol: str, start: datetime, end: datetime, interval = 'min', brick_size = .1) -> pd.DataFrame:

    brick_size = float(brick_size)/100
    df = edge.fetch_historical_data(conn, exchange, trading_symbol, start, end, interval)
    df['datetime'] = pd.to_datetime(df['datetime'])
    first_brick = {
        'datetime': df['datetime'].iloc[0],
        'low': df['close'].iloc[0],
        'high': df['close'].iloc[0],
        'color': 'green'
    }
    renko = [first_brick]
    dic =  df.to_dict(orient='records')
    for row in dic:
        up_step = round((renko[-1]['high'] * brick_size),2)
        down_step = round((renko[-1]['low'] * brick_size),2)
        if row['close'] >= renko[-1]['high'] + up_step:
            while row['close'] >= renko[-1]['high'] + (renko[-1]['high'] * brick_size):
                new_brick=[{
                    'datetime': row['datetime'],
                    'low': round(renko[-1]['high'], 2),
                    'high': round((renko[-1]['high'] + (renko[-1]['high'] * brick_size)), 2),
                    'color': 'green'  
                }]
                renko = renko + new_brick
        if row['close'] <= renko[-1]['low'] - down_step:
            while row['close'] <= renko[-1]['low'] - (renko[-1]['low'] * brick_size):
                new_brick=[{
                    'datetime': row['datetime'],
                    'low': round((renko[-1]['low'] - (renko[-1]['low'] * brick_size)), 2),
                    'high': round(renko[-1]['low'], 2),
                    'color': 'red'  
                }]
                renko = renko + new_brick
    df = pd.DataFrame(renko)
    df['close'] = np.where(df['color'] == 'red', df['low'], df['high'])
    return df 


def convert_to_renko(brick_size = .1, df: pd.DataFrame = pd.DataFrame) -> pd.DataFrame:
    brick_size = float(brick_size)/100
    df['datetime'] = pd.to_datetime(df['datetime'])
    first_brick = {
        'datetime': df['datetime'].iloc[0],
        'low': df['close'].iloc[0],
        'high': df['close'].iloc[0],
        'color': 'green'
    }
    renko = [first_brick]
    dic =  df.to_dict(orient='records')
    for row in dic:
        up_step = round((renko[-1]['high'] * brick_size),2)
        down_step = round((renko[-1]['low'] * brick_size),2)
        if row['close'] >= renko[-1]['high'] + up_step:
            while row['close'] >= renko[-1]['high'] + (renko[-1]['high'] * brick_size):
                new_brick=[{
                    'datetime': row['datetime'],
                    'low': round(renko[-1]['high'], 2),
                    'high': round((renko[-1]['high'] + (renko[-1]['high'] * brick_size)), 2),
                    'color': 'green'  
                }]
                renko = renko + new_brick
        if row['close'] <= renko[-1]['low'] - down_step:
            while row['close'] <= renko[-1]['low'] - (renko[-1]['low'] * brick_size):
                new_brick=[{
                    'datetime': row['datetime'],
                    'low': round((renko[-1]['low'] - (renko[-1]['low'] * brick_size)), 2),
                    'high': round(renko[-1]['low'], 2),
                    'color': 'red'  
                }]
                renko = renko + new_brick
    df = pd.DataFrame(renko)
    df['close'] = np.where(df['color'] == 'red', df['low'], df['high'])
    return df



def rsi(data, period=14):
    """
    Calculate the Relative Strength Index (RSI) for OHLC data.
    
    Parameters:
    data (DataFrame): DataFrame containing OHLC data with columns ['Open', 'High', 'Low', 'Close'].
    period (int): Period for calculating RSI. Default is 14.
    
    Returns:
    DataFrame: DataFrame containing RSI values.
    """
    data['delta'] = data['close'].diff()
    data['gain'] = (data['delta'].where(data['delta'] > 0, 0)).rolling(window=period).mean()
    data['loss'] = (-data['delta'].where(data['delta'] < 0, 0)).rolling(window=period).mean()

    data['rs'] = data['gain'] / data['loss']
    data['rsi'] = 100 - (100 / (1 + data['rs']))
    data['rsi'] = data['rsi'].round(2)
    data.drop(['delta','gain','loss','rs'], axis='columns', inplace=True)
    
    return data

