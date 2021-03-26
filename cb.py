import requests
import json

api_key = "8wlkBw4r7dcm6ywC"
api_secret = "txDhu3ufzgOja1KfVyvjCWpIpxuYTkjk"

api_pro_trade "UagiFY36k+W7TYXXgNTlDIZQuVBAebznqrUZanSeJ9P4ZOe1xqPzrMLm4OOVtbuhql0feMzXJHvmZTEvrJU29Q=="

from coinbase.wallet.client import Client
client = Client(api_key, api_secret)
user = client.get_current_user()

print(json.dumps(user, indent=4, sort_keys=True))
print(client.get_buy_price(currency_pair = 'BTC-EUR'))