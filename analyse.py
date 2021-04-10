import pandas as pd
from datetime import datetime,timedelta
import matplotlib.pyplot as plt

df = pd.read_csv("data/trades/open-BTC-EUR.csv",sep=',')

buy = df[df["side"]=="buy"]
sell = df[df["side"]=="sell"]
quantileValue = .002
buyUpQuantile,buyDownQuantile = buy["price"].quantile(1-quantileValue), buy["price"].quantile(quantileValue)
sellUpQuantile,sellDownQuantile =  sell["price"].quantile(1-quantileValue), sell["price"].quantile(quantileValue)

print(buyUpQuantile,buyDownQuantile)
print(sellUpQuantile,sellDownQuantile)

buy.drop(buy[(buy["price"]>buyUpQuantile) | (buy["price"]<buyDownQuantile)].index,inplace = True)
sell.drop(sell[(sell["price"]>sellUpQuantile) | (sell["price"]<buyDownQuantile)].index,inplace=True)

buy['time'] = buy['time'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ"))
sell['time'] = sell['time'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ"))

match = pd.read_csv("data/trades/match-BTC-EUR.csv",sep=',')
match['time'] = match['time'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ"))

buy["volume"] = [len(buy[(buy['time']<buy['time'].iloc[i]+pd.Timedelta(seconds = .5)) & (buy['time']>buy['time'].iloc[i]+pd.Timedelta(seconds = -.5)) ]) for i in range(len(buy))]
sell["volume"] = [len(sell[(sell['time']<sell['time'].iloc[i]+pd.Timedelta(seconds = .5)) & (sell['time']>sell['time'].iloc[i]+pd.Timedelta(seconds = -.5)) ]) for i in range(len(sell))]
match["volume"] = [len(match[(match['time']<match['time'].iloc[i]+pd.Timedelta(seconds = .5)) & (match['time']>match['time'].iloc[i]+pd.Timedelta(seconds = -.5)) ]) for i in range(len(match))]

plt.subplot(2, 1, 1)
#plt.figure()
plt.plot(buy["time"],buy["price"],'-b',label = "buy")
plt.plot(sell["time"],sell["price"],'-r',label = "sell")
plt.plot(match["time"],match["price"],'-g',label = "match")
plt.plot(match["time"],match["price"]*.995)
plt.plot(match["time"],match["price"]*.990025)
plt.legend()
plt.subplot(2, 1, 2)
plt.plot(buy["time"],buy["volume"],label = "buy")
plt.plot(sell["time"],sell["volume"],label = "sell")
plt.plot(match["time"],match["volume"],label = "match")
plt.legend()
plt.show()