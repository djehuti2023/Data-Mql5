#from conda.core.index import get_index
#from pandas.tests.extension.base import dtype
from pymongo import MongoClient
from pandas.io.json import json_normalize
from pymongo import MongoClient
import numpy as np
import pandas as pd
#import pytest
import datetime


def count(collection , nombd):
    client = MongoClient('localhost', 27017)
    db= client[nombd]

    documents = db[collection]

    n_cars = documents.count_documents({})

    print("il y a  {} document dans votre collection".format(n_cars))


def recoverydata(collection , nombd):
    client = MongoClient('localhost', 27017)
    db= client[nombd]
    documents = db[collection]
    tb = []
    data = list(documents.find())
    for i in range(0, len(data)):
        tb.append(data[i])
    data = json_normalize(tb)


    tbl = np.asarray(data)
    close = np.delete(tbl, [0,1,2], axis=1)
    rates_frame = pd.DataFrame(list(close),
                               columns=['open', 'high', 'low', 'close','volume', 'time'])


    #rates_frame = rates_frame.set_index(rates_frame.time)

    rates_frame = rates_frame[['time','low', 'high', 'open', 'close', 'volume']]



    return rates_frame


def recoveryDataClose(collection , nombd):
    client = MongoClient('localhost', 27017)
    db= client[nombd]
    documents = db[collection]
    tb = []
    data = list(documents.find())
    for i in range(0, len(data)):
        tb.append(data[i])
    data = json_normalize(tb)


    tbl = np.asarray(data)

    close = np.delete(tbl, [0,1,2,3,4,5,7,8], axis=1)
    rates_frame = pd.DataFrame(list(close),
                               columns=['close'])

    #rates_frame = rates_frame.set_index(rates_frame.time)
    rates_frame = rates_frame[['close']]


    return rates_frame


