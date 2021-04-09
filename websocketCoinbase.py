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
        self.keepalive = None

    def _keepAlive(self,interval=30):
        while self.ws.connected and not self.stop:
            self.ws.ping("keepalive")
            time.sleep(interval)

    def _connection(self):
        self.stop = False
        while not self.ws:        
            try:
                self.ws = create_connection(self.url)
                self.ws.send(json.dumps(self.subParams) )
                data = self.ws.recv()
                self.keepalive = threading.Thread(target=self._keepAlive)
                self.keepalive.start()
            except:
                self.ws = None
                time.sleep(1)

    def _close(self):
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException:
            pass
        
        self.stop=True
        self.keepalive.join()
        self.keepalive = None

    def _communicate(self):
        while not self.stop:
            try:
                data = self.ws.recv()
                self.q.put(json.loads(data))
            except:
                self.stop = True

    def run(self):
        while True:
            self._connection()
            self._communicate()
            self._close()

if not "data" in os.listdir(path):
    os.mkdir(os.path.join(path,"data"))
if not "orderBook" in os.listdir(path+"/data"):
    os.mkdir(os.path.join(path,"data/orderBook"))
if not "trades" in os.listdir(path+"/data"):
    os.mkdir(os.path.join(path,"data/trades")) 

cpt = 0
pathTrades = path+"/data/trades/"

for elem in os.listdir(path+"/data/orderBook/"):
    os.remove(path+"/data/orderBook/"+elem)
for elem in os.listdir(path+"/data/trades/"):
    os.remove(path+"/data/trades/"+elem)

ws = WebsocketClient(["BTC-EUR"])
ws.start()
while True:
    msg = ws.q.get()
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
