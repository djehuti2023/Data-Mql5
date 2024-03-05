from pymongo import MongoClient
from HistoryData.rateshistory import *
import time
import datetime





client = MongoClient('localhost',27017)


# client['Mavoo'] permet de creer une connection avec la base de donnée
db = client['Mavoo']

# Cree une collection pour EURUSD avec pour timeframe 1H
rates = db.GBPCHF_1M
#rateshistory() retourne un array contenant les informations lies à une timeframe de 1h
# price contient les informations de la timeframe avec des valeurs unique sans duplicat
# rates_frame contient les informations de la times avec duplicats
price, rates_frame , pairs , timeframes = ratestaken("GBPCHF",MT5_TIMEFRAME_M1)
for i in range(0, len(rates_frame)):
    datetime64Obj = np.datetime64(rates_frame['time'][i])

    year = datetime64Obj.astype(object).year
    # 2002
    day = datetime64Obj.astype(object).day
    # 4

    month = datetime64Obj.astype(object).month

    hour = datetime64Obj.astype(object).hour

    minute = datetime64Obj.astype(object).minute

    second = datetime64Obj.astype(object).second

    dt = datetime.datetime(year, month, day, hour, minute, second)

    test = {
        'timeframe': 'MT5_TIMEFRAME_M1',
        'pairs': pairs,
        'open': rates_frame['open'][i],
        'high': rates_frame['high'][i],
        'low': rates_frame['low'][i],
        'close': rates_frame['close'][i],
        'volume': int(rates_frame['tick_volume'][i]),
        'datetime': time.mktime(dt.timetuple())
    }
    rates.insert_one(test)

'''
******************** Timeframe 5 min   ********************************************************************
'''


rates = db.GBPCHF_5M
#rateshistory() retourne un array contenant les informations lies à une timeframe de 1h
# price contient les informations de la timeframe avec des valeurs unique sans duplicat
# rates_frame contient les informations de la times avec duplicats
price, rates_frame , pairs , timeframes = ratestaken("GBPCHF",MT5_TIMEFRAME_M5)


for i in range(0, len(rates_frame)):
    datetime64Obj = np.datetime64(rates_frame['time'][i])

    year = datetime64Obj.astype(object).year
    # 2002
    day = datetime64Obj.astype(object).day
    # 4

    month = datetime64Obj.astype(object).month

    hour = datetime64Obj.astype(object).hour

    minute = datetime64Obj.astype(object).minute

    second = datetime64Obj.astype(object).second

    dt = datetime.datetime(year, month, day, hour, minute, second)

    test = {
        'timeframe': 'MT5_TIMEFRAME_M1',
        'pairs': pairs,
        'open': rates_frame['open'][i],
        'high': rates_frame['high'][i],
        'low': rates_frame['low'][i],
        'close': rates_frame['close'][i],
        'volume': int(rates_frame['tick_volume'][i]),
        'datetime': time.mktime(dt.timetuple())
    }
    rates.insert_one(test)

'''
******************** Timeframe 15 min   ********************************************************************
'''


rates = db.GBPCHF_15M
#rateshistory() retourne un array contenant les informations lies à une timeframe de 1h
# price contient les informations de la timeframe avec des valeurs unique sans duplicat
# rates_frame contient les informations de la times avec duplicats
price, rates_frame , pairs , timeframes = ratestaken("GBPCHF",MT5_TIMEFRAME_M15)


for i in range(0, len(rates_frame)):
    datetime64Obj = np.datetime64(rates_frame['time'][i])

    year = datetime64Obj.astype(object).year
    # 2002
    day = datetime64Obj.astype(object).day
    # 4

    month = datetime64Obj.astype(object).month

    hour = datetime64Obj.astype(object).hour

    minute = datetime64Obj.astype(object).minute

    second = datetime64Obj.astype(object).second

    dt = datetime.datetime(year, month, day, hour, minute, second)

    test = {
        'timeframe': 'MT5_TIMEFRAME_M1',
        'pairs': pairs,
        'open': rates_frame['open'][i],
        'high': rates_frame['high'][i],
        'low': rates_frame['low'][i],
        'close': rates_frame['close'][i],
        'volume': int(rates_frame['tick_volume'][i]),
        'datetime': time.mktime(dt.timetuple())
    }
    rates.insert_one(test)


'''
******************** Timeframe 30 min   ********************************************************************
'''

rates = db.GBPCHF_30M
#rateshistory() retourne un array contenant les informations lies à une timeframe de 1h
# price contient les informations de la timeframe avec des valeurs unique sans duplicat
# rates_frame contient les informations de la times avec duplicats
price, rates_frame , pairs , timeframes = ratestaken("GBPCHF",MT5_TIMEFRAME_M30)


for i in range(0, len(rates_frame)):
    datetime64Obj = np.datetime64(rates_frame['time'][i])

    year = datetime64Obj.astype(object).year
    # 2002
    day = datetime64Obj.astype(object).day
    # 4

    month = datetime64Obj.astype(object).month

    hour = datetime64Obj.astype(object).hour

    minute = datetime64Obj.astype(object).minute

    second = datetime64Obj.astype(object).second

    dt = datetime.datetime(year, month, day, hour, minute, second)

    test = {
        'timeframe': 'MT5_TIMEFRAME_M1',
        'pairs': pairs,
        'open': rates_frame['open'][i],
        'high': rates_frame['high'][i],
        'low': rates_frame['low'][i],
        'close': rates_frame['close'][i],
        'volume': int(rates_frame['tick_volume'][i]),
        'datetime': time.mktime(dt.timetuple())
    }
    rates.insert_one(test)

'''
******************** Timeframe 1 Heure   ********************************************************************
'''


rates = db.GBPCHF_1H
#rateshistory() retourne un array contenant les informations lies à une timeframe de 1h
# price contient les informations de la timeframe avec des valeurs unique sans duplicat
# rates_frame contient les informations de la times avec duplicats
price, rates_frame , pairs , timeframes = ratestaken("GBPCHF",MT5_TIMEFRAME_H1)


for i in range(0, len(rates_frame)):
    datetime64Obj = np.datetime64(rates_frame['time'][i])

    year = datetime64Obj.astype(object).year
    # 2002
    day = datetime64Obj.astype(object).day
    # 4

    month = datetime64Obj.astype(object).month

    hour = datetime64Obj.astype(object).hour

    minute = datetime64Obj.astype(object).minute

    second = datetime64Obj.astype(object).second

    dt = datetime.datetime(year, month, day, hour, minute, second)

    test = {
        'timeframe': 'MT5_TIMEFRAME_M1',
        'pairs': pairs,
        'open': rates_frame['open'][i],
        'high': rates_frame['high'][i],
        'low': rates_frame['low'][i],
        'close': rates_frame['close'][i],
        'volume': int(rates_frame['tick_volume'][i]),
        'datetime': time.mktime(dt.timetuple())
    }
    rates.insert_one(test)

'''
******************** Timeframe 2 Heures   ********************************************************************
'''

rates = db.GBPCHF_2H
#rateshistory() retourne un array contenant les informations lies à une timeframe de 1h
# price contient les informations de la timeframe avec des valeurs unique sans duplicat
# rates_frame contient les informations de la times avec duplicats
price, rates_frame , pairs , timeframes = ratestaken("GBPCHF",MT5_TIMEFRAME_H2)


for i in range(0, len(rates_frame)):
    datetime64Obj = np.datetime64(rates_frame['time'][i])

    year = datetime64Obj.astype(object).year
    # 2002
    day = datetime64Obj.astype(object).day
    # 4

    month = datetime64Obj.astype(object).month

    hour = datetime64Obj.astype(object).hour

    minute = datetime64Obj.astype(object).minute

    second = datetime64Obj.astype(object).second

    dt = datetime.datetime(year, month, day, hour, minute, second)

    test = {
        'timeframe': 'MT5_TIMEFRAME_M1',
        'pairs': pairs,
        'open': rates_frame['open'][i],
        'high': rates_frame['high'][i],
        'low': rates_frame['low'][i],
        'close': rates_frame['close'][i],
        'volume': int(rates_frame['tick_volume'][i]),
        'datetime': time.mktime(dt.timetuple())
    }
    rates.insert_one(test)

'''
******************** Timeframe 3 heures   ********************************************************************
'''


rates = db.GBPCHF_3H
#rateshistory() retourne un array contenant les informations lies à une timeframe de 1h
# price contient les informations de la timeframe avec des valeurs unique sans duplicat
# rates_frame contient les informations de la times avec duplicats
price, rates_frame , pairs , timeframes = ratestaken("GBPCHF",MT5_TIMEFRAME_H3)


for i in range(0, len(rates_frame)):
    datetime64Obj = np.datetime64(rates_frame['time'][i])

    year = datetime64Obj.astype(object).year
    # 2002
    day = datetime64Obj.astype(object).day
    # 4

    month = datetime64Obj.astype(object).month

    hour = datetime64Obj.astype(object).hour

    minute = datetime64Obj.astype(object).minute

    second = datetime64Obj.astype(object).second

    dt = datetime.datetime(year, month, day, hour, minute, second)

    test = {
        'timeframe': 'MT5_TIMEFRAME_M1',
        'pairs': pairs,
        'open': rates_frame['open'][i],
        'high': rates_frame['high'][i],
        'low': rates_frame['low'][i],
        'close': rates_frame['close'][i],
        'volume': int(rates_frame['tick_volume'][i]),
        'datetime': time.mktime(dt.timetuple())
    }
    rates.insert_one(test)

'''
******************** Timeframe 4 Heures  ********************************************************************
'''


rates = db.GBPCHF_4H
#rateshistory() retourne un array contenant les informations lies à une timeframe de 1h
# price contient les informations de la timeframe avec des valeurs unique sans duplicat
# rates_frame contient les informations de la times avec duplicats
price, rates_frame , pairs , timeframes = ratestaken("GBPCHF",MT5_TIMEFRAME_H4)


for i in range(0, len(rates_frame)):
    datetime64Obj = np.datetime64(rates_frame['time'][i])

    year = datetime64Obj.astype(object).year
    # 2002
    day = datetime64Obj.astype(object).day
    # 4

    month = datetime64Obj.astype(object).month

    hour = datetime64Obj.astype(object).hour

    minute = datetime64Obj.astype(object).minute

    second = datetime64Obj.astype(object).second

    dt = datetime.datetime(year, month, day, hour, minute, second)

    test = {
        'timeframe': 'MT5_TIMEFRAME_M1',
        'pairs': pairs,
        'open': rates_frame['open'][i],
        'high': rates_frame['high'][i],
        'low': rates_frame['low'][i],
        'close': rates_frame['close'][i],
        'volume': int(rates_frame['tick_volume'][i]),
        'datetime': time.mktime(dt.timetuple())
    }
    rates.insert_one(test)

'''
******************** Timeframe 6 Heures   ********************************************************************
'''


rates = db.GBPCHF_6H
#rateshistory() retourne un array contenant les informations lies à une timeframe de 1h
# price contient les informations de la timeframe avec des valeurs unique sans duplicat
# rates_frame contient les informations de la times avec duplicats
price, rates_frame , pairs , timeframes = ratestaken("GBPCHF",MT5_TIMEFRAME_H6)


for i in range(0, len(rates_frame)):
    datetime64Obj = np.datetime64(rates_frame['time'][i])

    year = datetime64Obj.astype(object).year
    # 2002
    day = datetime64Obj.astype(object).day
    # 4

    month = datetime64Obj.astype(object).month

    hour = datetime64Obj.astype(object).hour

    minute = datetime64Obj.astype(object).minute

    second = datetime64Obj.astype(object).second

    dt = datetime.datetime(year, month, day, hour, minute, second)

    test = {
        'timeframe': 'MT5_TIMEFRAME_M1',
        'pairs': pairs,
        'open': rates_frame['open'][i],
        'high': rates_frame['high'][i],
        'low': rates_frame['low'][i],
        'close': rates_frame['close'][i],
        'volume': int(rates_frame['tick_volume'][i]),
        'datetime': time.mktime(dt.timetuple())
    }
    rates.insert_one(test)

'''
******************** Timeframe 8H   ********************************************************************
'''


rates = db.GBPCHF_8H
#rateshistory() retourne un array contenant les informations lies à une timeframe de 1h
# price contient les informations de la timeframe avec des valeurs unique sans duplicat
# rates_frame contient les informations de la times avec duplicats
price, rates_frame , pairs , timeframes = ratestaken("GBPCHF",MT5_TIMEFRAME_H8)


for i in range(0, len(rates_frame)):
    datetime64Obj = np.datetime64(rates_frame['time'][i])

    year = datetime64Obj.astype(object).year
    # 2002
    day = datetime64Obj.astype(object).day
    # 4

    month = datetime64Obj.astype(object).month

    hour = datetime64Obj.astype(object).hour

    minute = datetime64Obj.astype(object).minute

    second = datetime64Obj.astype(object).second

    dt = datetime.datetime(year, month, day, hour, minute, second)

    test = {
        'timeframe': 'MT5_TIMEFRAME_M1',
        'pairs': pairs,
        'open': rates_frame['open'][i],
        'high': rates_frame['high'][i],
        'low': rates_frame['low'][i],
        'close': rates_frame['close'][i],
        'volume': int(rates_frame['tick_volume'][i]),
        'datetime': time.mktime(dt.timetuple())
    }
    rates.insert_one(test)

'''
******************** Timeframe 12 Heures   ********************************************************************
'''

rates = db.GBPCHF_12H
#rateshistory() retourne un array contenant les informations lies à une timeframe de 1h
# price contient les informations de la timeframe avec des valeurs unique sans duplicat
# rates_frame contient les informations de la times avec duplicats
price, rates_frame , pairs , timeframes = ratestaken("GBPCHF",MT5_TIMEFRAME_H12)


for i in range(0, len(rates_frame)):
    datetime64Obj = np.datetime64(rates_frame['time'][i])

    year = datetime64Obj.astype(object).year
    # 2002
    day = datetime64Obj.astype(object).day
    # 4

    month = datetime64Obj.astype(object).month

    hour = datetime64Obj.astype(object).hour

    minute = datetime64Obj.astype(object).minute

    second = datetime64Obj.astype(object).second

    dt = datetime.datetime(year, month, day, hour, minute, second)

    test = {
        'timeframe': 'MT5_TIMEFRAME_M1',
        'pairs': pairs,
        'open': rates_frame['open'][i],
        'high': rates_frame['high'][i],
        'low': rates_frame['low'][i],
        'close': rates_frame['close'][i],
        'volume': int(rates_frame['tick_volume'][i]),
        'datetime': time.mktime(dt.timetuple())
    }
    rates.insert_one(test)

'''
******************** Timeframe 1 Day   ********************************************************************
'''


rates = db.GBPCHF_1D
#rateshistory() retourne un array contenant les informations lies à une timeframe de 1h
# price contient les informations de la timeframe avec des valeurs unique sans duplicat
# rates_frame contient les informations de la times avec duplicats
price, rates_frame , pairs , timeframes = ratestaken("GBPCHF",MT5_TIMEFRAME_D1)


for i in range(0, len(rates_frame)):
    datetime64Obj = np.datetime64(rates_frame['time'][i])

    year = datetime64Obj.astype(object).year
    # 2002
    day = datetime64Obj.astype(object).day
    # 4

    month = datetime64Obj.astype(object).month

    hour = datetime64Obj.astype(object).hour

    minute = datetime64Obj.astype(object).minute

    second = datetime64Obj.astype(object).second

    dt = datetime.datetime(year, month, day, hour, minute, second)

    test = {
        'timeframe': 'MT5_TIMEFRAME_M1',
        'pairs': pairs,
        'open': rates_frame['open'][i],
        'high': rates_frame['high'][i],
        'low': rates_frame['low'][i],
        'close': rates_frame['close'][i],
        'volume': int(rates_frame['tick_volume'][i]),
        'datetime': time.mktime(dt.timetuple())
    }
    rates.insert_one(test)

'''
******************** Timeframe 1 week   ********************************************************************
'''


rates = db.GBPCHF_1W
#rateshistory() retourne un array contenant les informations lies à une timeframe de 1h
# price contient les informations de la timeframe avec des valeurs unique sans duplicat
# rates_frame contient les informations de la times avec duplicats
price, rates_frame , pairs , timeframes = ratestaken("GBPCHF",MT5_TIMEFRAME_W1)


for i in range(0, len(rates_frame)):
    datetime64Obj = np.datetime64(rates_frame['time'][i])

    year = datetime64Obj.astype(object).year
    # 2002
    day = datetime64Obj.astype(object).day
    # 4

    month = datetime64Obj.astype(object).month

    hour = datetime64Obj.astype(object).hour

    minute = datetime64Obj.astype(object).minute

    second = datetime64Obj.astype(object).second

    dt = datetime.datetime(year, month, day, hour, minute, second)

    test = {
        'timeframe': 'MT5_TIMEFRAME_M1',
        'pairs': pairs,
        'open': rates_frame['open'][i],
        'high': rates_frame['high'][i],
        'low': rates_frame['low'][i],
        'close': rates_frame['close'][i],
        'volume': int(rates_frame['tick_volume'][i]),
        'datetime': time.mktime(dt.timetuple())
    }
    rates.insert_one(test)

'''
******************** Timeframe 1 Month   ********************************************************************
'''

rates = db.GBPCHF_1MONTH
#rateshistory() retourne un array contenant les informations lies à une timeframe de 1h
# price contient les informations de la timeframe avec des valeurs unique sans duplicat
# rates_frame contient les informations de la times avec duplicats
price, rates_frame , pairs , timeframes = ratestaken("GBPCHF",MT5_TIMEFRAME_MON1)


for i in range(0, len(rates_frame)):
    datetime64Obj = np.datetime64(rates_frame['time'][i])

    year = datetime64Obj.astype(object).year
    # 2002
    day = datetime64Obj.astype(object).day
    # 4

    month = datetime64Obj.astype(object).month

    hour = datetime64Obj.astype(object).hour

    minute = datetime64Obj.astype(object).minute

    second = datetime64Obj.astype(object).second

    dt = datetime.datetime(year, month, day, hour, minute, second)

    test = {
        'timeframe': 'MT5_TIMEFRAME_M1',
        'pairs': pairs,
        'open': rates_frame['open'][i],
        'high': rates_frame['high'][i],
        'low': rates_frame['low'][i],
        'close': rates_frame['close'][i],
        'volume': int(rates_frame['tick_volume'][i]),
        'datetime': time.mktime(dt.timetuple())
    }
    rates.insert_one(test)

# tu dois installer pymongo comme ceci
#pip install pymongo
