import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

df = pd.read_csv("data/trades/open-BTC-EUR.csv",sep=',')

buy = df[df["side"]=="buy"]
sell = df[df["side"]=="sell"]

buy['time'] = buy['time'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ") )
sell['time'] = sell['time'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ"))

match = pd.read_csv("data/trades/match-BTC-EUR.csv",sep=',')
match['time'] = match['time'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ"))

plt.figure()
plt.plot(buy["time"],buy["price"],'-b',label = "buy")
plt.plot(sell["time"],sell["price"],'-r',label = "sell")
plt.plot(match["time"],match["price"],'-g',label = "match")
plt.legend()
plt.show()