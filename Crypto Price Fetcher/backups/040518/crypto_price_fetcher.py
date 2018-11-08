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
DATA_FETCH_INTERVAL = 3600 * 3 #seconds


#----------------------------------------------------------------
# Galea code from https://github.com/agalea91/cryptocompare-api
#----------------------------------------------------------------
class RenderJSON(object):
    def __init__(self, json_data):
        if isinstance(json_data, dict):
            self.json_str = json.dumps(json_data)
        else:
            self.json_str = json_data
        self.uuid = str(uuid.uuid4())

    def _ipython_display_(self):
        display_html('<div id="{}" style="height: 600px; width:100%;"></div>'.format(self.uuid), raw=True)
        display_javascript("""
        require(["https://rawgit.com/caldwell/renderjson/master/renderjson.js"], function() {
        document.getElementById('%s').appendChild(renderjson(%s))
        });
        """ % (self.uuid, self.json_str), raw=True)

def minute_price_historical(symbol, comparison_symbol, limit, aggregate, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit={}&aggregate={}'\
            .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return df



#----------------------------------------------------------------
# Fetcher Code
#----------------------------------------------------------------

def fetch_new_coin_data():
    for coin in COINS:
        data_filename = coin + ".csv"
        time_delta = 1 # Bar width in minutes
        new_data = minute_price_historical(coin, 'USD', 9999, time_delta)
        if os.path.exists(data_filename):
            old_data = pd.DataFrame.from_csv(data_filename)
            merged_data = pd.concat([old_data, new_data])
            merged_data['timestamp'] = pd.to_datetime(merged_data.timestamp)
            merged_data = merged_data.sort_values(by=['timestamp'])
            merged_data = merged_data.drop_duplicates(['timestamp'], keep='last')
        else:
            print("Creatine new data file " + data_filename)
            old_data = []
            merged_data = new_data
        merged_data.to_csv(data_filename)
        print(coin + ": " + str(len(merged_data) - len(old_data)) + " new entries were added")


def fetch_data_periodically():
  threading.Timer(DATA_FETCH_INTERVAL, fetch_data_periodically).start()
  fetch_new_coin_data()
    

fetch_data_periodically()

