from mt5 import mt5store
import zmq
import threading


from pymongo import MongoClient
client = MongoClient('localhost',27017)
db = client['papillon']
rates = db.AUDJPY_1M


api = mt5store.MTraderAPI()





#Ici on affiche les information qui sont lié au compte par rapport a la configuration effectuer

def _t_livedata():
    socket = api.jpy_socket()
    print(socket)
    while True:

        try:
            last_candle = socket.recv_json()
        except zmq.ZMQError:
            raise zmq.NotDone("Live data ERROR")
        test1 = last_candle
        print("test1 ", test1)
        #decomenter cette partie pour faire une sauvegarde des donnees recu du marche
        test2 = {
            'timeframe': 'MT5_TIMEFRAME_M1',
            'pairs': test1['pair'],
            'open': test1['open'],
            'high': test1['high'],
            'low': test1['low'],
            'close': test1['close'],
            'volume': int(test1['tickvolume']),
            'datetime': test1['time']
        }

        rates.insert_one(test2)

t = threading.Thread(target=_t_livedata(), daemon=True)
t.start()











