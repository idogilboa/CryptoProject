#!/usr/bin/env python

import sys
import argparse
import pymysql
import urllib2
import json
import datetime

def init():
    #globals setup
    globals()['commentsDepth'] = 2
    globals()['threadsData'] = []
    globals()['commentsData'] = []
    
    #arguments setup
    parser = argparse.ArgumentParser(description='Reddit crawler.')
    parser.add_argument('-subreddit', help='an integer for the accumulator')
    parser.add_argument('-startTime', help='an integer for the accumulator')
    globals()['args'] = parser.parse_args()
    
    #Connection setup
    globals()['conn'] = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='reddit', charset='utf8')
    globals()['cur'] = conn.cursor()

def updateThreadsTable():
    params = ['author', 'created_utc', 'full_link','num_comments','score', 'selftext', 'subreddit', 'title','id']
    colsText = ', '.join(params)
    baseQuery = "INSERT INTO reddit.threads ({colsText}) values ({colsData})"
    for elem in globals()['threadsData']:
        try:
            colsData = []
            elem['title'] = elem['title'].encode('ascii','ignore')
            try:
                elem['selftext'] = elem['selftext'].encode('ascii','ignore')
            except:
                elem['selftext'] = ""
            for param in params:
                if param == 'created_utc':
                    elem[param] = datetime.datetime.fromtimestamp(int(elem[param])).strftime('%y/%m/%d %H:%M:%S')
                colsData.append(elem[param])
    
            colsData = str(colsData).strip('[').strip(']').replace("u'","'")
            query = baseQuery.format(colsText = colsText, colsData = colsData)
        except KeyError as e:
            print "KeyError - updateThreadTable"
            print e
            continue
            
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print "SQLError - updateThreadTable"
            print e
            continue
          
def updateCommentsTable():
    params = ['author', 'created_utc', 'body','score', 'subreddit', 'id','parent_id','thread']
    colsText = ', '.join(params)
    
    baseQuery = "INSERT INTO reddit.comments ({colsText}) values ({colsData})"
    for elem in commentsData:
        try:
            colsData = []
            for param in params:
                if param == 'created_utc':
                    elem[param] = datetime.datetime.fromtimestamp(int(elem[param])).strftime('%y/%m/%d %H:%M:%S')
                if param == 'thread':
                    elem['thread'] = elem['permalink'].split("/")[5]
                    colsData.append(elem['thread'])
                    continue
                colsData.append(elem[param])
            colsData = str(colsData).strip('[').strip(']').replace("u'","'")
            query = baseQuery.format(colsText = colsText, colsData = colsData)
        except KeyError as e:
            print "KeyError - updateCommentsTable"
            print e
            continue
            
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print "SQLError - updateCommentsTable"
            print e
            continue
          


def getSubredditThreads(subreddit, startTime):
    length = 500
    after = startTime
    hdr = {'User-Agent' : 'TheCryptoProject:windows10'}

    #https://github.com/pushshift/api
    url = "https://api.pushshift.io/reddit/search/submission/?subreddit={subreddit}&after={after}&size=500"
    counter = 0
    print counter
    while (length == 500):
        req = urllib2.Request(url.format(subreddit=subreddit, after=after), headers = hdr)
        response = urllib2.urlopen(req)
        jsonFile = response.read()
        dataSlice = json.loads(jsonFile)['data']
        length = len(dataSlice)
        globals()['threadsData'].extend(dataSlice)
        counter += 1
        updateCommentsTable()
        after = dataSlice[length - 1]['created_utc']
        globals()['threadsData'] = []
        print "Chunk number update - {}".format(counter)

def getSubredditComments(subreddit, startTime):
    length = 500
    after = startTime
    hdr = {'User-Agent' : 'TheCryptoProject:windows10'}

    #https://github.com/pushshift/api
    url = "https://api.pushshift.io/reddit/search/comment/?subreddit={subreddit}&after={after}&size=500"
    while (length == 500):
        req = urllib2.Request(url.format(subreddit=subreddit, after=after), headers = hdr)
        response = urllib2.urlopen(req)
        jsonFile = response.read()
        dataSlice = json.loads(jsonFile)['data']
        length = len(dataSlice)
        after = dataSlice[length - 1]['created_utc']
        commentsData.extend(dataSlice)
        
def main(arguments):
    init()
    getSubredditThreads(args.subreddit, args.startTime)
    updateThreadsTable()
#     getSubredditComments(args.subreddit, args.startTime)
#     updateCommentsTable()
    cur.close()
    conn.close()
   



if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))