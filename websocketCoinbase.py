import threading
import json
import os
import time
import queue
import csv
import sys
import json, hmac, hashlib, time, requests, base64
from websocket import create_connection
from requests.auth import AuthBase

path = os.path.dirname(os.path.realpath(__file__))

class WebsocketClient(threading.Thread):
    def __init__(self,products):

        threading.Thread.__init__(self)

        self.url = "wss://ws-feed.pro.coinbase.com"
        self.timestamp = str(time.time())
        self.path = os.path.dirname(os.path.realpath(__file__))

        with open(self.path+"/pass.json","r") as file:
            passAPI = json.load(file)

        message = self.timestamp + 'GET' + '/users/self/verify'
        message = message.encode('ascii')
        hmac_key = base64.b64decode(passAPI["API_SECRET"])
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode('utf-8')

        self.subParams = {
            "type": "subscribe",
            "product_ids": products,
            "channels": ["full"],
            "signature": signature_b64,
            "key": passAPI["API_KEY"],
            "passphrase": passAPI["API_PASS"],
            "timestamp": self.timestamp
        }
        self.stop = False
        self.ws = None
        self.q = queue.Queue()

    def run(self):

        self.ws = create_connection(self.url)
        self.ws.send(json.dumps(self.subParams))
        data = self.ws.recv()
        while not self.stop:
            data = self.ws.recv()
            self.q.put(json.loads(data))

size = 0
cpt = 0
pathTrades = path+"/data/trades/"

for elem in os.listdir("data/orderBook/"):
    os.remove("data/orderBook"+elem)
for elem in os.listdir("data/trades/"):
    os.remove("data/trades/"+elem)

ws = WebsocketClient(["BTC-EUR"])
ws.start()
while True:
    msg = ws.q.get()
    size+= sys.getsizeof(msg)
    cpt+=1
    fileName = pathTrades+msg["type"]+"-"+msg["product_id"]+".csv"
    if msg["type"]+"-"+msg["product_id"]+".csv" in os.listdir(pathTrades):
        with open(fileName, 'a') as file:
            dict_writer = csv.DictWriter(file,msg.keys())
            dict_writer.writerow(msg)
    else:
        with open(fileName, 'w+', newline='')  as file:
            dict_writer = csv.DictWriter(file, msg.keys())
            dict_writer.writeheader()
            dict_writer.writerow(msg)
