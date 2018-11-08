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


COINS = ['BTC', 'ETH', 'LTC', 'XRP', 'BCH', 'ADA', 'NEO', 'XLM', 'EOS', 'XEM']

def fetch_new_coin_data():
    for coin in COINS:
        print(coin + ": ")
        data_filename = coin + ".csv"
        data = pd.read_csv(data_filename)
        time = data['time']
        range_start = 0
        for i in range(len(time)):
            if i == 0:
                continue
            if time[i] - time[i-1] != 60:
                print(str.format("#(indexes {0}-{1}): {2} - {3}", range_start, i-1, data['timestamp'][range_start], data['timestamp'][i-1]))
                range_start = i     
        print("\n")

fetch_new_coin_data()

