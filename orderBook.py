import threading
import bisect
import json
from websocketCoinbase import WebsocketClient

class OrderBook(threading.Thread):

	def __init__(self):
		threading.Thread.__init__(self)

		with open("products.txt",'r+') as file:
			self.products = [elem for elem in json.loads(file.read()) if elem["quote_currency"]=="EUR" or elem["base_currency"]=="EUR"]

		self.book = {"sell":{},"buy":{}}
		for product in self.products:
			self.book["sell"][product["id"]] = []
			self.book["buy"][product["id"]] = []

		self.stop = False
		self.wsClient = None
		
	def run(self):
		self.wsClient = WebsocketClient([product["id"] for product in self.products])
		self.wsClient.start()
		while not self.stop:
			msg = self.wsClient.q.get()
			if msg["type"] == "open":
				self.book[msg["side"]][msg["product_id"]].append(msg)
			elif msg["type"] == "done" :
				result = [elem for elem in self.book[msg["side"]][msg["product_id"]] if  msg["order_id"]==elem["order_id"]]
				if result:
					self.book[msg["side"]][msg["product_id"]].remove(result[0])
		self.wsClient.stop = True



if __name__ == "__main__":
	import time
	book = OrderBook()
	book.start()
	while True:
		print("sell : {}/ buy : {}".format(len(book.book["sell"]["BTC-EUR"]),len(book.book["buy"]["BTC-EUR"])),end="\r")