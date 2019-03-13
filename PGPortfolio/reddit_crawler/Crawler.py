#!/usr/bin/env python

import argparse
import requests
import sqlite3
import datetime
import os


SUBREDDITS = {
    "BCN": ["BytecoinBCN"],
    "NAV": ["NavCoin"],
    "XCP": ["counterparty_xcp"],
    "NXT": ["NXT"],
    "LBC": ["lbry"],
    "REP": ["Augur"],
    "PASC": ["pascalcoin"],
    "BCH": ["Bitcoincash"],
    "CVC": ["civicplatform"],
    "NEO": ["NEO"],
    "GAS": ["NEO"],
    "EOS": ["eos"],
    "SNT": ["statusim"],
    "BAT": ["BATProject"],
    "LOOM": ["loomnetwork"],
    "QTUM": ["Qtum"],
    "BNT": ["Bancor"],
    "XRP": ["Ripple,XRP"],
    "LTC": ["litecoin", "LitecoinMarkets"]
    "ALL" : ["CryptoCurrency", "CryptoMarkets"]
}


class CrawlerDB:

    def __init__(self):
        self.database_dir = os.path.join(os.path.dirname(__file__), "../database/Data.db")
        self.create_tables()

    def create_tables(self):
        with sqlite3.connect(self.database_dir) as connection:
            cursor = connection.cursor()

            cursor.execute('CREATE TABLE IF NOT EXISTS Threads (date INTEGER,'
                           ' created_utc datetime,'
                           ' subreddit varchar(100),'
                           ' coin varchar(20),'
                           ' score INTEGER,'
                           ' author varchar(60),'
                           ' num_comments INTEGER,'
                           ' title varchar(500),'
                           ' selftext varchar(4000),'
                           ' full_link varchar(750),'
                           ' id varchar(45),'
                           'PRIMARY KEY (id));')

            cursor.execute('CREATE TABLE IF NOT EXISTS Comments (date INTEGER,'
                           ' created_utc datetime,'
                           ' subreddit varchar(100),'
                           ' coin varchar(20),'
                           ' score INTEGER,'
                           ' author varchar(60),'
                           ' body varchar(1000),'
                           ' sentiment_polarity INTEGER,'
                           ' sentiment_subjectivity INTEGER,'
                           ' parent_id varchar(45),'
                           ' id varchar(45),'
                           'PRIMARY KEY (id));')

            connection.commit()

    def update_threads_table(self, threads_data):
        params = ['author', 'created_utc', 'date', 'full_link', 'num_comments', 'score', 'selftext', 'subreddit',
                  'title', 'id']
        cols_text = ', '.join(params)

        with sqlite3.connect(self.database_dir) as connection:
            cursor = connection.cursor()

            for elem in threads_data:
                try:
                    cols_data = []
                    for param in params:
                        if param not in elem:
                            elem[param] = "NA"
                        if param == 'created_utc':
                            elem['date'] = elem[param]
                            elem[param] = datetime.datetime.fromtimestamp(float(elem[param])).strftime('%d/%m/%y %H:%M:%S')
                        cols_data.append(elem[param])

                    query = "INSERT INTO Threads ({cols_text}) values (" + "".join(
                        ["?, " for i in range(len(params) - 1)]) + "?)"
                    query = query.format(cols_text=cols_text)
                except KeyError as e:
                    print("updateThreadTable::", e)
                    continue

                try:
                    cursor.execute(query, tuple(cols_data))
                except Exception as e:
                    print("updateThreadTable::", e)
                    continue

            connection.commit()

    def update_comments_table(self, comments_data):
        params = ['author', 'created_utc', 'date', 'body', 'score', 'subreddit', 'id', 'parent_id']
        cols_text = ', '.join(params)

        with sqlite3.connect(self.database_dir) as connection:
            cursor = connection.cursor()

            for elem in comments_data:
                for param in params:
                    if param not in elem:
                        elem[param] = "NA"
                if elem['subreddit'] == "NA":
                    continue
                try:
                    cols_data = []
                    for param in params:
                        if param == 'created_utc':
                            elem['date'] = elem[param]
                            elem[param] = datetime.datetime.fromtimestamp(int(elem[param])).strftime('%d/%m/%y %H:%M:%S')
                        cols_data.append(elem[param])
                    query = "INSERT INTO Comments ({cols_text}) values (" + "".join(
                        ["?, " for i in range(len(params) - 1)]) + "?)"
                    query = query.format(cols_text=cols_text)
                except KeyError as e:
                    print("updateCommentsTable::", e)
                    continue

                try:
                    cursor.execute(query, tuple(cols_data))
                except Exception as e:
                    print("updateCommentsTable::", e)
                    continue

            connection.commit()


class Crawler:

    # cur.execute("SELECT * from Comments where subreddit=\"{subreddit}\" and date between (1520908527-33600) and 1520908527").format(subreddit=SUBREDDIT[current_coin])

    def __init__(self):
        # https://github.com/pushshift/api
        self.threads_req_url = "https://api.pushshift.io/reddit/search/submission/?subreddit={subreddit}&after={after}&before={before}&size=500"
        self.comments_req_url = "https://api.pushshift.io/reddit/search/comment/?subreddit={subreddit}&after={after}&before={before}&size=500"
        self.db = CrawlerDB()

    def get_subreddit_threads(self, subreddit, start_time, end_time):
        length = 500
        after = start_time
        threads_data = []
        while length == 500:
            print("Sending Threads request")
            req = requests.get(self.threads_req_url.format(subreddit=subreddit, after=int(after), before=int(end_time)))
            data_slice = req.json()['data']
            length = len(data_slice)
            if length == 0:
                print("No data found in %s" % subreddit)
                return
            after = data_slice[length - 1]['created_utc']
            threads_data.extend(data_slice)
        return threads_data

    def get_subreddit_comments(self, subreddit, start_time, end_time):
        length = 500
        after = start_time
        comments_data = []
        while length == 500:
            print("Sending comments request")
            req = requests.get(self.comments_req_url.format(subreddit=subreddit, after=int(after), before=int(end_time)))
            data_slice = req.json()['data']
            length = len(data_slice)
            if length == 0:
                print("No data found in %s" % subreddit)
                return
            after = data_slice[length - 1]['created_utc']
            comments_data.extend(data_slice)
        return comments_data

    def fetch_all_reddit_data(self, start_time, end_time):
        for coin, subs in SUBREDDITS.items():
            for subreddit in subs:
                print("Fetching {}:{}".format(coin, subreddit))
                threads_data = self.get_subreddit_threads(subreddit, start_time, end_time)
                self.db.update_threads_table(threads_data)
                comments_data = self.get_subreddit_comments(subreddit, start_time, end_time)
                self.db.update_comments_table(comments_data)


def main():
    parser = argparse.ArgumentParser(description='Reddit crawler.')
    parser.add_argument('-start_time', help='epoch start time')
    parser.add_argument('-end_time', help='epoch end time')
    args = parser.parse_args()
    crawler = Crawler()
    crawler.fetch_all_reddit_data(args.start_time, args.end_time)


if __name__ == '__main__':
    main()
