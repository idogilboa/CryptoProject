#!/usr/bin/env python

import sys
import argparse
import urllib2
import json
import sqlite3
import datetime
import os

globals()['DATABASE_DIR'] = os.path.realpath("../database/Data.db")


def init():
    # globals setup
    globals()['commentsDepth'] = 2
    globals()['threadsData'] = []
    globals()['commentsData'] = []

    # arguments setup
    parser = argparse.ArgumentParser(description='Reddit crawler.')
    parser.add_argument('-startTime', help='epoch start time')
    parser.add_argument('-endTime', help='epoch end time')
    globals()['args'] = parser.parse_args()
    createTables()
    # Connection setup
    # globals()['conn'] = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='reddit',charset='utf8')
    # globals()['cur'] = conn.cursor()

def createTables():
    with sqlite3.connect(DATABASE_DIR) as connection:
        cursor = connection.cursor()

        cursor.execute('CREATE TABLE IF NOT EXISTS Threads (date INTEGER,'
                       ' created_utc datetime,'
                       ' subreddit varchar(100),'
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
                       ' score INTEGER,'
                       ' author varchar(60),'
                       ' body varchar(1000),'
                       ' parent_id varchar(45),'
                       ' id varchar(45),'
                       'PRIMARY KEY (id));')

        connection.commit()

def updateThreadsTable():
    params = ['author', 'created_utc','date', 'full_link', 'num_comments', 'score', 'selftext', 'subreddit', 'title', 'id']
    colsText = ', '.join(params)
    for elem in threadsData:
        try:
            colsData = []
            for param in params:
                if param not in elem:
                    elem[param] = "NA"
                if param == 'created_utc':
                    elem['date'] = elem[param]
                    elem[param] = datetime.datetime.fromtimestamp(float(elem[param])).strftime('%d/%m/%y %H:%M:%S')
                colsData.append(elem[param])

            query = "INSERT INTO Threads ({colsText}) values (" + "".join(["?, " for i in range(len(params)-1)]) + "?)"
            query = query.format(colsText=colsText)
        except KeyError as e:
            print "KeyError - updateThreadTable"
            continue

        try:
            cur.execute(query, tuple(colsData))
            conn.commit()
        except Exception as e:
            print "SQLError - updateThreadTable - {}".format(e)
            continue

def updateCommentsTable():
    params = ['author', 'created_utc','date', 'body', 'score', 'subreddit', 'id', 'parent_id']
    colsText = ', '.join(params)
    for elem in commentsData:
        for param in params:
            if param not in elem:
                elem[param] = "NA"
        if elem['subreddit'] == "NA":
            continue
        try:
            colsData = []
            for param in params:
                if param == 'created_utc':
                    elem['date'] = elem[param]
                    elem[param] = datetime.datetime.fromtimestamp(int(elem[param])).strftime('%d/%m/%y %H:%M:%S')
                colsData.append(elem[param])
            query = "INSERT INTO Comments ({colsText}) values (" + "".join(["?, " for i in range(len(params)-1)]) + "?)"
            query = query.format(colsText=colsText)
        except KeyError as e:
            print "KeyError - updateCommentsTable"
            continue

        try:
            cur.execute(query, tuple(colsData))
            conn.commit()
        except Exception as e:
            print "SQLError - updateThreadTable - {}".format(e)
            continue


def getSubredditThreads(subreddit, startTime, endTime):
    length = 500
    after = startTime
    before = endTime
    hdr = {'User-Agent': 'TheCryptoProject:windows10'}

    # https://github.com/pushshift/api
    url = "https://api.pushshift.io/reddit/search/submission/?subreddit={subreddit}&after={after}&before={before}&size=500"
    while (length == 500):
        print "Sending Threads request"
        req = urllib2.Request(url.format(subreddit=subreddit, after=after, before=before), headers=hdr)
        response = urllib2.urlopen(req)
        jsonFile = response.read()
        dataSlice = json.loads(jsonFile)['data']
        length = len(dataSlice)
        if length == 0:
            print "No information from requetsed dates was found"
            exit(0)
        after = dataSlice[length - 1]['created_utc']
        threadsData.extend(dataSlice)

def getSubredditComments(subreddit, startTime, endTime):
    length = 500
    after = startTime
    before = endTime
    hdr = {'User-Agent': 'TheCryptoProject:windows10'}

    # https://github.com/pushshift/api
    url = "https://api.pushshift.io/reddit/search/comment/?subreddit={subreddit}&after={after}&before={before}&size=500"
    while (length == 500):
        print "Sending comments request"
        req = urllib2.Request(url.format(subreddit=subreddit, after=after, before=before), headers=hdr)
        response = urllib2.urlopen(req)
        jsonFile = response.read()
        dataSlice = json.loads(jsonFile)['data']
        length = len(dataSlice)
        after = dataSlice[length - 1]['created_utc']
        commentsData.extend(dataSlice)


SUBREDDITS = {
    "BCN" : ["BytecoinBCN"],
    "NAV" : ["NavCoin"],
    "XCP" : ["counterparty_xcp"],
    "NXT" : ["NXT"],
    "LBC" : ["lbry"],
    "REP" : ["Augur"],
    "PASC": ["pascalcoin"],
    "BCH" : ["Bitcoincash"],
    "CVC" : ["civicplatform"],
    "NEO" : ["NEO"],
    "GAS" : ["NEO"],
    "EOS" : ["eos"],
    "SNT" : ["statusim"],
    "BAT" : ["BATProject"],
    "LOOM": ["loomnetwork"],
    "QTUM": ["Qtum"],
    "BNT" : ["Bancor"],
    "XRP" : ["Ripple,XRP"],
    "LTC" : ["litecoin", "LitecoinMarkets"],
    "ALL" : ["CryptoCurrency", "CryptoMarkets"]
}

def fetchAllRedditData(startTime, endTime):
    for coin, subs in SUBREDDITS.iteritems():
        for subreddit in subs:
            print("Fetching {}:{}".format(coin, subreddit))
            getSubredditThreads(subreddit, startTime, endTime)
            updateThreadsTable()
            getSubredditComments(subreddit, startTime, endTime)
            updateCommentsTable()
            globals()['threadsData'] = []
            globals()['commentsData'] = []


def main():
    globals()['conn'] = sqlite3.connect(DATABASE_DIR)
    globals()['cur'] = conn.cursor()
    init()
    fetchAllRedditData(args.startTime, args.endTime)
    conn.close()


if __name__ == '__main__':
    sys.exit(main())