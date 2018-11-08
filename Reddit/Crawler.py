#!/usr/bin/env python

import sys
import argparse
import pymysql
import urllib2
import json
import datetime


def init():
    # globals setup
    globals()['commentsDepth'] = 2
    globals()['threadsData'] = []
    globals()['commentsData'] = []

    # arguments setup
    parser = argparse.ArgumentParser(description='Reddit crawler.')
    parser.add_argument('-subreddit', help='subreddit name')
    parser.add_argument('-startTime', help='epoch start time')
    parser.add_argument('-endTime', help='epoch end time')
    parser.add_argument('-subreddits', help = 'list of subreddits comma seperated')

    globals()['args'] = parser.parse_args()

    # Connection setup
    globals()['conn'] = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='reddit',
                                        charset='utf8')
    globals()['cur'] = conn.cursor()


def updateThreadsTable():
    params = ['author', 'created_utc', 'full_link', 'num_comments', 'score', 'selftext', 'subreddit', 'title', 'id']
    colsText = ', '.join(params)
    baseQuery = "INSERT INTO reddit.threads ({colsText}) values ({colsData})"
    for elem in threadsData:
        try:
            colsData = []
            for param in params:
                if param not in elem:
                    elem[param] = "NA"
                if param == 'created_utc':
                    elem[param] = datetime.datetime.fromtimestamp(int(elem[param])).strftime('%y/%m/%d %H:%M:%S')
                colsData.append(elem[param])

            colsData = str(colsData).strip('[').strip(']').replace("u'", "'").replace("u\"","\"")
            query = baseQuery.format(colsText=colsText, colsData=colsData)
        except KeyError as e:
            print "KeyError - updateThreadTable"
            continue

        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print "SQLError - updateThreadTable - {}".format(e)
            continue


def updateCommentsTable():
    params = ['author', 'created_utc', 'body', 'score', 'subreddit', 'id', 'parent_id']
    colsText = ', '.join(params)

    baseQuery = "INSERT INTO reddit.comments ({colsText}) values ({colsData})"
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
                    elem[param] = datetime.datetime.fromtimestamp(int(elem[param])).strftime('%y/%m/%d %H:%M:%S')
                # if param == 'thread':
                #     elem['thread'] = elem['permalink'].split("/")[5] if elem['permalink'] != "NA" else "NA"
                #     colsData.append(elem['thread'])
                #     continue
                colsData.append(elem[param])
            colsData = str(colsData).strip('[').strip(']').replace("u'", "'").replace("u\"","\"")
            query = baseQuery.format(colsText=colsText, colsData=colsData)
        except KeyError as e:
            print "KeyError - updateCommentsTable"
            print e
            continue

        try:
            cur.execute(query)
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


def main(arguments):
    init()
    for subreddit in args.subreddits:
        getSubredditThreads(subreddit, args.startTime, args.endTime)
        updateThreadsTable()
        getSubredditComments(subreddit, args.startTime, args.endTime)
        updateCommentsTable()
        threadsData = []
        commentsData = []
    cur.close()
    conn.close()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))