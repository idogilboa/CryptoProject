import os.path
import threading
import requests
import datetime
import pandas as pd
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')
# Pretty print the JSON
import uuid
from IPython.display import display_javascript, display_html, display
import json

#----------------------------------------------------------------
# Configuration
#----------------------------------------------------------------

COINS = ['BTC', 'ETH', 'LTC', 'XRP', 'BCH', 'ADA', 'NEO', 'XLM', 'EOS', 'XEM']


for coin in COINS:
    data_filename = coin + ".csv"
    print(coin)
    if os.path.exists(data_filename):
        fd = pd.read_csv(data_filename)
        fd = fd.set_index('timestamp')
        fd = fd.rename(columns={'volumeto':'volume'})
        #fd.drop(fd.tail(10).index,inplace=True)
        fd.to_csv(data_filename)



