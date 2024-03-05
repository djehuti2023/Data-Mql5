import pandas as pd
import numpy as np
from datetime import datetime
import MetaTrader5 as mt5
import pytz


# Cette fonction permet de recupererer l'historique lie a la paire EURUSD avec une timeframe de 1H a partir

# les donnees recuperer s arrete au 2018/04/05
def marketpairs(pairs,  timeframe ):
    
    # connexion à MetaTrader 5
    if not mt5.initialize():
        print("initialize() a échoué")
        mt5.shutdown()

    timezone = pytz.timezone("Etc/UTC")
    utc_from = datetime(2023, 12, 1, tzinfo=timezone)
    utc_to = datetime(2024, 1, 1, tzinfo=timezone)
    # rates = MT5CopyRatesFrom("EURUSD", MT5_TIMEFRAME_H1, utc_from, 10000)
    rates = mt5.copy_rates_range(pairs, timeframe, utc_from, utc_to)
    
    return rates

def local_to_utc(dt , UTC_OFFSET_TIMEDELTA):
        return dt + UTC_OFFSET_TIMEDELTA

# Cette fonction permet de transformer les donnee du marche en tableau  et pret a etre analyser par harmonic
def rateshistory():
    rates = marketpairs()
    for rate in rates:
        rate
    ver= np.asarray(rates)
    rates= np.delete(ver, [6,7], axis=1)

    rates_frame = pd.DataFrame(list(rates),
                               columns=['time', 'open', 'high', 'low', 'close', 'tick_volume'])

    UTC_OFFSET_TIMEDELTA = datetime.utcnow() - datetime.now()

    rates_frame['time'] = rates_frame.apply(lambda rate: local_to_utc(rate['time'] , UTC_OFFSET_TIMEDELTA), axis=1)
    rates_frame.time = pd.to_datetime(rates_frame.time, format = '%d.%m.%Y %H:%M:%S.%f')

    rates_frame = rates_frame.set_index(rates_frame.time)

    rates_frame = rates_frame[['open','high','low','close','tick_volume']]
    rates_frame = rates_frame.drop_duplicates(keep= False)
    price = rates_frame.close.copy()

    return price, rates_frame
def ratestaken(pairs, timeframes):


    rates = marketpairs(pairs,timeframes)
    print("###################")
    print(rates)
    print("###################")
    for rate in rates:
        rate
    ver= np.asarray(rates)
    rates= np.delete(ver, [6,7], axis=1)

    rates_frame = pd.DataFrame(list(rates),
                               columns=['time', 'open', 'high', 'low', 'close', 'tick_volume'])

    UTC_OFFSET_TIMEDELTA = datetime.utcnow() - datetime.now()

    rates_frame['time'] = rates_frame.apply(lambda rate: local_to_utc(rate['time'] , UTC_OFFSET_TIMEDELTA), axis=1)
    rates_frame.time = pd.to_datetime(rates_frame.time, format = '%d.%m.%Y %H:%M:%S.%f')

    rates_frame = rates_frame.drop_duplicates(keep= False)
    price = rates_frame.close.copy()



    return price, rates_frame, pairs , timeframes
