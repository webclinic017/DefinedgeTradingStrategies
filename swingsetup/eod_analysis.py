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
scripts = ['360ONE-EQ','3MINDIA-EQ','AARTIIND-EQ','AAVAS-EQ','ABB-EQ','ABBOTINDIA-EQ','ABCAPITAL-EQ','ABFRL-EQ','ACC-EQ','ACE-EQ','ACI-EQ','ADANIENSOL-EQ','ADANIENT-EQ','ADANIGREEN-EQ','ADANIPORTS-EQ','ADANIPOWER-EQ','AEGISCHEM-EQ','AETHER-EQ','AFFLE-EQ','AIAENG-EQ','AJANTPHARM-EQ','ALKEM-EQ','ALKYLAMINE-EQ','ALLCARGO-EQ','ALOKINDS-EQ','AMBER-EQ','AMBUJACEM-EQ','ANANDRATHI-EQ','ANGELONE-EQ','ANURAS-EQ','APARINDS-EQ','APLAPOLLO-EQ','APLLTD-EQ','APOLLOHOSP-EQ','APOLLOTYRE-EQ','APTUS-EQ','ARE&M-EQ','ASAHIINDIA-EQ','ASHOKLEY-EQ','ASIANPAINT-EQ','ASTERDM-EQ','ASTRAL-EQ','ASTRAZEN-EQ','ATGL-EQ','ATUL-EQ','AUBANK-EQ','AUROPHARMA-EQ','AVANTIFEED-EQ','AWL-EQ','AXISBANK-EQ','BAJAJ-AUTO-EQ','BAJAJFINSV-EQ','BAJAJHLDNG-EQ','BAJFINANCE-EQ','BALAMINES-EQ','BALKRISIND-EQ','BALRAMCHIN-EQ','BANDHANBNK-EQ','BANKBARODA-EQ','BANKINDIA-EQ','BATAINDIA-EQ','BAYERCROP-EQ','BBTC-EQ','BDL-EQ','BEL-EQ','BEML-EQ','BERGEPAINT-EQ','BHARATFORG-EQ','BHARTIARTL-EQ','BHEL-EQ','BIKAJI-EQ','BIOCON-EQ','BIRLACORPN-EQ','BLS-EQ','BLUEDART-EQ','BLUESTARCO-EQ','BORORENEW-EQ','BOSCHLTD-EQ','BPCL-EQ','BRIGADE-EQ','BRITANNIA-EQ','BSE-EQ','BSOFT-EQ','CAMPUS-EQ','CAMS-EQ','CANBK-EQ','CANFINHOME-EQ','CAPLIPOINT-EQ','CARBORUNIV-EQ','CASTROLIND-EQ','CCL-EQ','CDSL-EQ','CEATLTD-EQ','CELLO-EQ','CENTRALBK-EQ','CENTURYPLY-EQ','CENTURYTEX-EQ','CERA-EQ','CESC-EQ','CGCL-EQ','CGPOWER-EQ','CHALET-EQ','CHAMBLFERT-EQ','CHEMPLASTS-EQ','CHENNPETRO-EQ','CHOLAFIN-EQ','CHOLAHLDNG-EQ','CIEINDIA-EQ','CIPLA-EQ','CLEAN-EQ','COALINDIA-EQ','COCHINSHIP-EQ','COFORGE-EQ','COLPAL-EQ','CONCOR-EQ','CONCORDBIO-EQ','COROMANDEL-EQ','CRAFTSMAN-EQ','CREDITACC-EQ','CRISIL-EQ','CROMPTON-EQ','CSBBANK-EQ','CUB-EQ','CUMMINSIND-EQ','CYIENT-EQ','DABUR-EQ','DALBHARAT-EQ','DATAPATTNS-EQ','DCMSHRIRAM-EQ','DEEPAKFERT-EQ','DEEPAKNTR-EQ','DELHIVERY-EQ','DEVYANI-EQ','DIVISLAB-EQ','DIXON-EQ','DLF-EQ','DMART-EQ','DOMS-EQ','DRREDDY-EQ','EASEMYTRIP-EQ','ECLERX-EQ','EICHERMOT-EQ','EIDPARRY-EQ','EIHOTEL-EQ','ELECON-EQ','ELGIEQUIP-EQ','EMAMILTD-EQ','ENDURANCE-EQ','ENGINERSIN-EQ','EPL-EQ','EQUITASBNK-EQ','ERIS-EQ','ESCORTS-EQ','EXIDEIND-EQ','FACT-EQ','FDC-EQ','FEDERALBNK-EQ','FINCABLES-EQ','FINEORG-EQ','FINPIPE-EQ','FIVESTAR-EQ','FLUOROCHEM-EQ','FORTIS-EQ','FSL-EQ','GAEL-EQ','GAIL-EQ','GESHIP-EQ','GICRE-EQ','GILLETTE-EQ','GLAND-EQ','GLAXO-EQ','GLENMARK-EQ','GLS-EQ','GMDCLTD-EQ','GMMPFAUDLR-EQ','GMRINFRA-EQ','GNFC-EQ','GODFRYPHLP-EQ','GODREJCP-EQ','GODREJIND-EQ','GODREJPROP-EQ','GPIL-EQ','GPPL-EQ','GRANULES-EQ','GRAPHITE-EQ','GRASIM-EQ','GRINDWELL-EQ','GRSE-EQ','GSFC-EQ','GSPL-EQ','GUJGASLTD-EQ','HAL-EQ','HAPPSTMNDS-EQ','HAPPYFORGE-EQ','HAVELLS-EQ','HBLPOWER-EQ','HCLTECH-EQ','HDFCAMC-EQ','HDFCBANK-EQ','HDFCLIFE-EQ','HEG-EQ','HEROMOTOCO-EQ','HFCL-EQ','HINDALCO-EQ','HINDCOPPER-EQ','HINDPETRO-EQ','HINDUNILVR-EQ','HINDZINC-EQ','HOMEFIRST-EQ','HONASA-EQ','HONAUT-EQ','HSCL-EQ','HUDCO-EQ','IBULHSGFIN-EQ','ICICIBANK-EQ','ICICIGI-EQ','ICICIPRULI-EQ','IDBI-EQ','IDEA-EQ','IDFC-EQ','IDFCFIRSTB-EQ','IEX-EQ','IGL-EQ','IIFL-EQ','INDHOTEL-EQ','INDIACEM-EQ','INDIAMART-EQ','INDIANB-EQ','INDIGO-EQ','INDIGOPNTS-EQ','INDUSINDBK-EQ','INDUSTOWER-EQ','INFY-EQ','INOXWIND-EQ','INTELLECT-EQ','IOB-EQ','IOC-EQ','IPCALAB-EQ','IRB-EQ','IRCON-EQ','IRCTC-EQ','IRFC-EQ','ISEC-EQ','ITC-EQ','ITI-EQ','J&KBANK-EQ','JAIBALAJI-EQ','JBCHEPHARM-EQ','JBMA-EQ','JINDALSAW-EQ','JINDALSTEL-EQ','JIOFIN-EQ','JKCEMENT-EQ','JKLAKSHMI-EQ','JKPAPER-EQ','JMFINANCIL-EQ','JSL-EQ','JSWENERGY-EQ','JSWINFRA-EQ','JSWSTEEL-EQ','JUBLFOOD-EQ','JUBLINGREA-EQ','JUBLPHARMA-EQ','JUSTDIAL-EQ','JWL-EQ','JYOTHYLAB-EQ','KAJARIACER-EQ','KALYANKJIL-EQ','KANSAINER-EQ','KARURVYSYA-EQ','KAYNES-EQ','KEC-EQ','KEI-EQ','KFINTECH-EQ','KIMS-EQ','KNRCON-EQ','KOTAKBANK-EQ','KPIL-EQ','KPITTECH-EQ','KPRMILL-EQ','KRBL-EQ','KSB-EQ','LALPATHLAB-EQ','LATENTVIEW-EQ','LAURUSLABS-EQ','LEMONTREE-EQ','LICHSGFIN-EQ','LICI-EQ','LINDEINDIA-EQ','LLOYDSME-EQ','LODHA-EQ','LT-EQ','LTF-EQ','LTIM-EQ','LTTS-EQ','LUPIN-EQ','LXCHEM-EQ','M&M-EQ','M&MFIN-EQ','MAHABANK-EQ','MAHLIFE-EQ','MAHSEAMLES-EQ','MANAPPURAM-EQ','MANKIND-EQ','MANYAVAR-EQ','MAPMYINDIA-EQ','MARICO-EQ','MARUTI-EQ','MASTEK-EQ','MAXHEALTH-EQ','MAZDOCK-EQ','MCDOWELL-N-EQ','MCX-EQ','MEDANTA-EQ','MEDPLUS-EQ','METROBRAND-EQ','METROPOLIS-EQ','MFSL-EQ','MGL-EQ','MHRIL-EQ','MINDACORP-EQ','MMTC-EQ','MOTHERSON-EQ','MOTILALOFS-EQ','MPHASIS-EQ','MRF-EQ','MRPL-EQ','MSUMI-EQ','MTARTECH-EQ','MUTHOOTFIN-EQ','NAM-INDIA-EQ','NATCOPHARM-EQ','NATIONALUM-EQ','NAUKRI-EQ','NAVINFLUOR-EQ','NBCC-EQ','NCC-EQ','NESTLEIND-EQ','NETWORK18-EQ','NH-EQ','NHPC-EQ','NIACL-EQ','NLCINDIA-EQ','NMDC-EQ','NSLNISP-EQ','NTPC-EQ','NUVAMA-EQ','NUVOCO-EQ','NYKAA-EQ','OBEROIRLTY-EQ','OFSS-EQ','OIL-EQ','OLECTRA-EQ','ONGC-EQ','PAGEIND-EQ','PATANJALI-EQ','PAYTM-EQ','PCBL-EQ','PEL-EQ','PERSISTENT-EQ','PETRONET-EQ','PFC-EQ','PGHH-EQ','PHOENIXLTD-EQ','PIDILITIND-EQ','PIIND-EQ','PNB-EQ','PNBHOUSING-EQ','PNCINFRA-EQ','POLICYBZR-EQ','POLYCAB-EQ','POLYMED-EQ','POONAWALLA-EQ','POWERGRID-EQ','POWERINDIA-EQ','PPLPHARMA-EQ','PRAJIND-EQ','PRESTIGE-EQ','PRINCEPIPE-EQ','PRSMJOHNSN-EQ','PVRINOX-EQ','QUESS-EQ','RADICO-EQ','RAILTEL-EQ','RAINBOW-EQ','RAJESHEXPO-EQ','RAMCOCEM-EQ','RATNAMANI-EQ','RAYMOND-EQ','RBA-EQ','RBLBANK-EQ','RCF-EQ','RECLTD-EQ','REDINGTON-EQ','RELIANCE-EQ','RENUKA-EQ','RHIM-EQ','RITES-EQ','RKFORGE-EQ','ROUTE-EQ','RRKABEL-EQ','RTNINDIA-EQ','RVNL-EQ','SAFARI-EQ','SAIL-EQ','SANOFI-EQ','SAPPHIRE-EQ','SAREGAMA-EQ','SBFC-EQ','SBICARD-EQ','SBILIFE-EQ','SBIN-EQ','SCHAEFFLER-EQ','SCHNEIDER-EQ','SHREECEM-EQ','SHRIRAMFIN-EQ','SHYAMMETL-EQ','SIEMENS-EQ','SIGNATURE-EQ','SJVN-EQ','SKFINDIA-EQ','SOBHA-EQ','SOLARINDS-EQ','SONACOMS-EQ','SONATSOFTW-EQ','SPARC-EQ','SRF-EQ','STARHEALTH-EQ','STLTECH-EQ','SUMICHEM-EQ','SUNDARMFIN-EQ','SUNDRMFAST-EQ','SUNPHARMA-EQ','SUNTECK-EQ','SUNTV-EQ','SUPREMEIND-EQ','SUVENPHAR-EQ','SUZLON-EQ','SWANENERGY-EQ','SWSOLAR-EQ','SYNGENE-EQ','SYRMA-EQ','TANLA-EQ','TATACHEM-EQ','TATACOMM-EQ','TATACONSUM-EQ','TATAELXSI-EQ','TATAINVEST-EQ','TATAMOTORS-EQ','TATAMTRDVR-EQ','TATAPOWER-EQ','TATASTEEL-EQ','TATATECH-EQ','TCS-EQ','TECHM-EQ','TEJASNET-EQ','THERMAX-EQ','TIINDIA-EQ','TIMKEN-EQ','TITAGARH-EQ','TITAN-EQ','TMB-EQ','TORNTPHARM-EQ','TORNTPOWER-EQ','TRENT-EQ','TRIDENT-EQ','TRITURBINE-EQ','TRIVENI-EQ','TTML-EQ','TV18BRDCST-EQ','TVSMOTOR-EQ','TVSSCS-EQ','UBL-EQ','UCOBANK-EQ','UJJIVANSFB-EQ','ULTRACEMCO-EQ','UNIONBANK-EQ','UNOMINDA-EQ','UPL-EQ','USHAMART-EQ','UTIAMC-EQ','VAIBHAVGBL-EQ','VARROC-EQ','VBL-EQ','VEDL-EQ','VGUARD-EQ','VIJAYA-EQ','VIPIND-EQ','VOLTAS-EQ','VTL-EQ','WELCORP-EQ','WELSPUNLIV-EQ','WESTLIFE-EQ','WHIRLPOOL-EQ','WIPRO-EQ','YESBANK-EQ','ZEEL-EQ','ZENSARTECH-EQ','ZFCVINDIA-EQ','ZOMATO-EQ','ZYDUSLIFE-EQ']

mongo_client = MongoClient(CONNECTION_STRING)
eod_analysis = mongo_client['Bots']['eod_analysis']

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
        #print(rs)
        rs_1p = ta.convert_to_renko(brick_size=1, df=rs)
        print("Converted RS to Renko 1%")
        rs_1p = ta.supertrend(rs_1p, 40, 10)
        print("Calculated Supertrend on RS Renko")
        rs_1p = ta.rsi(rs_1p)
        print("Calculated RSI on RS Renko")

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

        if eod_analysis.count_documents({'instrument_name': script}) > 0:
            stock_analysis = eod_analysis.find({'instrument_name': script})
            for stock in stock_analysis:
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'instrument_name': script}})
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'ema_channel_trend': ohlc['trend'].iloc[-1]}})
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'rsi_daily': ohlc['rsi'].iloc[-1]}})
                eod_analysis.update_one({'_id': stock['_id']}, {'$set': {'rs_renko_1_percent': rs_renko_1_percent}})
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
                'rs': rs_1p['close'].iloc[-1],
                'rs_rsi': rs_1p['rsi'].iloc[-1],
                'renko_trend_5_percent': renko_trend_5_percent,
                'renko_trend_3_percent': renko_trend_3_percent,
                'renko_trend_1_percent': renko_1p['signal'].iloc[-1],
                'date': datetime.now().strftime("%d-%B-%Y"),
                'time': datetime.now().strftime('%H:%M')
            }
            eod_analysis.insert_one(analysis)




if __name__ == "__main__":
    main()




    