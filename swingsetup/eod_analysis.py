import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from lib import connect_definedge as edge
from lib import ta
from datetime import datetime
import time
from pymongo import MongoClient
import pandas as pd
pd.set_option('mode.chained_assignment', None)


CONNECTION_STRING = "mongodb+srv://adminuser:05NZN7kKp5D4TZnU@bots.vnitakj.mongodb.net/?retryWrites=true&w=majority" #Mongo Connection
scripts = ['ZYDUSLIFE-EQ']

mongo_client = MongoClient(CONNECTION_STRING)
eod_analysis = mongo_client['Bots']['eod_analysis']
eod_qualified = mongo_client['Bots']['eod_qualified']
query = {'ema_channel_trend': 'Bullish', 'rs_renko_1_percent': 'Positive', 'rs_renko_3_percent': 'Positive', 'renko_trend_5_percent': 'Bullish', 'renko_trend_3_percent': 'Bullish', 'renko_trend_1_percent': 'Bullish', 'rsi_daily': {'$gt': 60} }

def main():
    api_token = "618a0b4c-f173-407e-acdc-0f61080f856c"
    api_secret = "TbfcWNtKL7vaXfPV3m6pKQ=="

    for script in scripts:
        time.sleep(3)
        conn = edge.login_to_integrate(api_token, api_secret)
        days_ago = datetime(2010,1,1)
        start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
        print()
        print(f"InstrumentName: {script}")

        ohlc = edge.fetch_historical_data(conn, 'NSE', script, start, datetime.today(), 'day')
        print("Fetched OHLC Data")
        nifty_ohlc = edge.fetch_historical_data(conn, 'NSE', 'Nifty 50', ohlc['datetime'].iloc[0], datetime.today(), 'day')
        print("Fetched NIFTY Data")

        renko_5p = ta.convert_to_renko(5, ohlc)
        print("Converted Renko 5%")
        renko_5p = ta.tma(renko_5p)
        print("Calculated TMA on Renko 5%")
        if renko_5p['close'].iloc[-1] > renko_5p['ema_40'].iloc[-1] and renko_5p['color'].iloc[-1] == 'green':
            renko_trend_5_percent = "Bullish"
        else:
            renko_trend_5_percent = "Bearish"

        renko_3p = ta.convert_to_renko(3, ohlc)
        print("Converted Renko 3%")
        renko_3p = ta.tma(renko_3p)
        print("Calculated TMA on Renko 3%")
        if renko_3p['close'].iloc[-1] > renko_3p['ema_40'].iloc[-1] and renko_3p['color'].iloc[-1] == 'green':
            renko_trend_3_percent = "Bullish"
        else:
            renko_trend_3_percent = "Bearish"

        renko_1p = ta.convert_to_renko(1, ohlc)
        print("Converted Renko 1%")
        renko_1p = ta.supertrend(renko_1p, 40, 10)
        print("Calculated Supertrend on Renko 1%")

        rs = ta.rs(ohlc, nifty_ohlc)
        print("Calculated RS on OHLC")
        #print(rs.iloc[25:75])
        rs_1p = ta.convert_to_renko(brick_size=1, df=rs)
        print("Converted RS to Renko 1%")
        rs_1p = ta.supertrend(rs_1p, 40, 10)
        print("Calculated Supertrend on RS Renko 1%")
        rs_1p = ta.rsi(rs_1p)
        print("Calculated RSI on RS Renko 1%")

        rs_3p = ta.convert_to_renko(brick_size=3, df=rs)
        print("Converted RS to Renko 3%")
        rs_3p = ta.supertrend(rs_3p, 40, 10)
        print("Calculated Supertrend on RS Renko 3%")
        rs_3p = ta.rsi(rs_3p)
        print("Calculated RSI on RS Renko 3%")

        ohlc = ta.ema_channel(df=ohlc)
        print("Calculated EMA Channel on OHLC")
        ohlc = ta.rsi(data=ohlc)
        print("Calculated Daily RSI on OHLC")

        # print(ohlc.iloc[-1])
        # print(nifty_ohlc.iloc[-1])
        print(f"*** Finished ***")

        if rs_1p['signal'].iloc[-1] == 'Bullish' and rs_1p['rsi'].iloc[-1] > 60:
            rs_renko_1_percent = "Positive"
        elif rs_1p['signal'].iloc[-1] == 'Bearish' and rs_1p['rsi'].iloc[-1] < 40:
            rs_renko_1_percent = "Negative"
        else:
            rs_renko_1_percent = "Neutral"

        if rs_3p['signal'].iloc[-1] == 'Bullish' and rs_3p['rsi'].iloc[-1] > 60:
            rs_renko_3_percent = "Positive"
        elif rs_3p['signal'].iloc[-1] == 'Bearish' and rs_3p['rsi'].iloc[-1] < 40:
            rs_renko_3_percent = "Negative"
        else:
            rs_renko_3_percent = "Neutral"

        if eod_analysis.count_documents({'instrument_name': script}) > 0:
            stock_analysis = eod_analysis.find({'instrument_name': script})
            for stock in stock_analysis:
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'instrument_name': script}})
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'ema_channel_trend': ohlc['trend'].iloc[-1]}})
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'rsi_daily': ohlc['rsi'].iloc[-1]}})
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'rs_renko_1_percent': rs_renko_1_percent}})
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'rs_renko_3_percent': rs_renko_3_percent}})
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'rs': rs_1p['close'].iloc[-1]}})
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'rs_rsi': rs_1p['rsi'].iloc[-1]}})
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'renko_trend_5_percent': renko_trend_5_percent}})
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'renko_trend_3_percent': renko_trend_3_percent}})
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'renko_trend_1_percent': renko_1p['signal'].iloc[-1]}})
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'date': datetime.now().strftime("%d-%B-%Y")}})
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'time': datetime.now().strftime('%H:%M')}})
        else:
            analysis = {
                'instrument_name': script,
                'ema_channel_trend': ohlc['trend'].iloc[-1],
                'rsi_daily': ohlc['rsi'].iloc[-1],
                'rs_renko_1_percent': rs_renko_1_percent,
                'rs_renko_3_percent': rs_renko_3_percent,
                'rs': rs_1p['close'].iloc[-1],
                'rs_rsi': rs_1p['rsi'].iloc[-1],
                'renko_trend_5_percent': renko_trend_5_percent,
                'renko_trend_3_percent': renko_trend_3_percent,
                'renko_trend_1_percent': renko_1p['signal'].iloc[-1],
                'date': datetime.now().strftime("%d-%B-%Y"),
                'time': datetime.now().strftime('%H:%M')
            }
            eod_analysis.insert_one(analysis)

    if eod_analysis.count_documents(query) > 1:
        qualified_stocks = eod_analysis.find(query)
        if eod_qualified.estimated_document_count() > 0:
            eod_qualified.drop()
        for stock in qualified_stocks:
            qualified_stock = {
                'instrument_name': stock['instrument_name'],
                'rsi_daily': stock['rsi_daily']
            }
            eod_qualified.insert_one(qualified_stock)



if __name__ == "__main__":
    main()




    