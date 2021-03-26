#!/usr/bin/python3
import threading
import time
import ast
import csv
import os
import numpy as np
import pandas as pd
from queue import Queue
import json, hmac, hashlib, time, requests, base64
from requests.auth import AuthBase
from datetime import datetime, timedelta

api_url = 'https://api.pro.coinbase.com/'
API_KEY = "92d126dade9eb14238c3dc4ee65f47ba"
API_SECRET ="GGWAzjjV/9yjpdvJ0hJZ2ligKgPkCC17rY4jPa4PRd+Dhob7dKbtzooUAsuY5dsGw+8OSzfzQHPv+8S7qEDNCQ=="
API_PASS = "7rdyzp1imh5"

# Create custom authentication for Exchange

class CoinbaseExchangeAuth(AuthBase):
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or b'').decode()
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message.encode(), hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode()

        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        })
        return request

class Client(threading.Thread):
    def __init__(self,auth,dt = 10,plot=False):
        threading.Thread.__init__(self)
        self.auth = auth
        self.dt = dt #dt en second
        with open("products.txt",'r+') as file:
            self.products = [elem for elem in json.loads(file.read()) if elem["quote_currency"]=="EUR" or elem["base_currency"]=="EUR"]
        self.nbProducts = len(self.products)
        self.q = Queue(maxsize=0)
        self.plotLine = []
        self.plotBool = plot
        self.stopRun = False
        self.stopSave = False

    def save(self):
        while not self.stopSave:
            while not self.q.empty():
                r = self.q.get()

                fileName = r["path"]+r["product"]+".csv"
                if r["product"]+".csv" in os.listdir(r["path"]):
                    with open(fileName, 'a') as file:
                        dict_writer = csv.DictWriter(file, r["data"][0].keys())
                        dict_writer.writerows(list(r["data"]))
                else:
                    keys = r["data"][0].keys()
                    with open(fileName, 'w+', newline='')  as file:
                        dict_writer = csv.DictWriter(file, keys)
                        dict_writer.writeheader()
                        dict_writer.writerows(list(r["data"]))

    def run(self):

        saveThread = threading.Thread(target=self.save)
        saveThread.start()

        while not self.stopRun:
            for product in self.products:
                end = datetime.now()
                start = datetime.now() - timedelta(seconds=self.dt)
                #Get trades
                r = requests.get(api_url+"products/{}/trades".format(product['id']), auth=self.auth)
                self.q.put({"product":product['id'],"data":ast.literal_eval(r.text),"path":"data/trades/"})
                #Get book
                #r = requests.get(api_url+"/products/{}/book?level=3".format(product['id']),auth = self.auth)
                #self.q.put({"product":product['id'],"data":[ast.literal_eval(r.text),"path":"data/orderBook/"})
                time.sleep(self.dt/self.nbProducts)

        self.stopSave = True

    def clean(self):
        for elem in os.listdir("data/orderBook/"):
            os.remove("data/orderBook"+elem)
        for elem in os.listdir("data/trades"):
            os.remove("data/trades/"+elem)


if __name__ =="__main__":
    auth = CoinbaseExchangeAuth(API_KEY, API_SECRET, API_PASS)
    client = Client(auth,plot=True)
    client.clean()
    client.start()
    client.join()


# Get accounts
#r = requests.get(api_url + 'accounts', auth=auth)
#print(r.json())
"""
end = datetime.now()
start = datetime.now() - timedelta(days=1)
#r = requests.get(api_url+"products/BTC-USD/candles",params={"start":start.strftime("%Y-%m-%dT%H:%M:%S"),"end":end.strftime("%Y-%m-%dT%H:%M:%S"),"granularity":300}, auth=auth)
r = requests.get(api_url+"products",params={"start":start.strftime("%Y-%m-%dT%H:%M:%S"),"end":end.strftime("%Y-%m-%dT%H:%M:%S"),"granularity":300}, auth=auth)
with open("products.txt",'w+') as file:
	file.write(r.text)"""