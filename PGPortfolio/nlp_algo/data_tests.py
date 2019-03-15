import numpy as np
import re
import nltk
from sklearn.datasets import load_files
import pickle
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
import sqlite3
import os


STOP_WORDS = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd",
              'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers',
              'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what',
              'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were',
              'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the',
              'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about',
              'then', 'once', 'here', 'there', 'where', 'why', 'how', 'both', 'each', 'few', 'other', 'some', 'such',
              'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'just', 'don', "don't", 'd', 'll', 'm', 'o', 're',
              've', 'y', 'ain', 'aren', 'ma']

vectorizer = CountVectorizer(max_features=2500, min_df=5, max_df=0.7, ngram_range=(1, 3), stop_words=STOP_WORDS)

DB_DIR = os.path.join(os.path.dirname(__file__), "../database/Data.db")
PRICES_RESOLUTION = 5 * 60


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def query_top_threads(start_date, end_date, num_of_threads):
    with sqlite3.connect(DB_DIR) as connection:
        connection.row_factory = dict_factory
        cursor = connection.cursor()
        threads_query = "SELECT date, coin, num_comments, score, title||selftext as text from Threads " \
                        "WHERE coin != 'Unknown' and coin != 'BTC' and coin != 'NXT' and " \
                        "date BETWEEN %d and %d and title NOT LIKE '%%Daily Discussion%%'" \
                        "ORDER BY num_comments + score DESC LIMIT %d" % \
                        (start_date, end_date, num_of_threads)
        cursor.execute(threads_query)
        threads = cursor.fetchall()
    return threads


def query_top_comments(start_date, end_date, num_of_comments):
    with sqlite3.connect(DB_DIR) as connection:
        connection.row_factory = dict_factory
        cursor = connection.cursor()
        comments_query = "SELECT date, coin, score, body as text from Comments " \
                         "WHERE coin != 'Unknown' AND coin != 'BTC' AND coin != 'NXT' AND " \
                         "date BETWEEN %d AND %d AND " \
                         "(body LIKE '%%should sell%%' OR body LIKE '%%should buy%%' OR body LIKE '%%vitalik%%') "\
                         "ORDER BY score DESC LIMIT %d" % \
                         (start_date, end_date, num_of_comments)
        cursor.execute(comments_query)
        comments = cursor.fetchall()
    return comments


def query_price_changes_for_msgs(msgs, window_length):
    with sqlite3.connect(DB_DIR) as connection:
        connection.row_factory = dict_factory
        cursor = connection.cursor()
        prices_change = [] # up=True, down=False
        msgs_with_price_change = []
        for i, msg in enumerate(msgs):
            start_date = msg['date']
            end_date = start_date + window_length
            coin = msg['coin']
            history_window_query = "SELECT close from History " \
                                   "WHERE coin = '%s' AND " \
                                   "date BETWEEN %d and %d " \
                                   "ORDER BY date ASC" % \
                                   (coin, start_date, end_date)
            cursor.execute(history_window_query)
            prices = cursor.fetchall()
            if len(prices) <= (window_length / PRICES_RESOLUTION) - 1:
                continue
            msgs_with_price_change.append(msg['text'])
            price_value = prices[0]['close'] / prices[-1]['close']
            prices_change.append(price_value >= 1)

    return {"data": msgs_with_price_change, "target": np.array(prices_change)}


def get_one_msg_to_price_window_change_set(start_date, end_date, num_of_msgs, window_length, msg_type='Threads'):
    if msg_type == 'Threads':
        msgs = query_top_threads(start_date, end_date, num_of_msgs)
    elif msg_type == 'Comments':
        msgs = query_top_comments(start_date, end_date, num_of_msgs)
    elif msg_type == 'All':
        msgs = query_top_comments(start_date, end_date, num_of_msgs / 2)
        msgs += query_top_comments(start_date, end_date, num_of_msgs / 2)
    else:
        raise NotImplementedError
    return query_price_changes_for_msgs(msgs, window_length)


def get_avaialble_coins():
    with sqlite3.connect(DB_DIR) as connection:
        connection.row_factory = dict_factory
        cursor = connection.cursor()
        query = "SELECT DISTINCT coin FROM History"
        cursor.execute(query)
        coins = cursor.fetchall()
    return coins


def get_prices_changes_to_msgs_volume_set(start_date, end_date, window_length):
    for coin in ['ETH']:#get_avaialble_coins():
        with sqlite3.connect(DB_DIR) as connection:
            connection.row_factory = dict_factory
            cursor = connection.cursor()
            prices_query = "SELECT date, open, close FROM History " \
                           "WHERE date BETWEEN %d AND %d " \
                           "AND coin = '%s' ORDER BY date ASC" % (start_date, end_date, coin)
            cursor.execute(prices_query)
            prices = cursor.fetchall()

            cursor = connection.cursor()
            comments_query = "SELECT date, coin, score FROM Comments " \
                             "WHERE date between %d AND %d " \
                             "AND coin = '%s' " \
                             "ORDER BY date ASC" % (start_date - window_length, end_date, coin)
            cursor.execute(comments_query)
            comments = cursor.fetchall()

            msgs_volumes = []
            prices_changes = []

            window_start_idx = 0
            window_end_idx = 0
            for price_desc in prices:
                window_end = price_desc['date']
                window_start = window_end - window_length

                while comments[window_start_idx]['date'] < window_start:
                    window_start_idx += 1
                if comments[window_start_idx]['date'] > window_end:
                    window_start_idx -= 1
                while comments[window_end_idx]['date'] < window_end:
                    window_end_idx += 1
                if comments[window_end_idx]['date'] > window_end:
                    window_end_idx -= 1

                msgs_volumes.append(window_end_idx - window_start_idx)
                prices_changes.append((price_desc['close'] / price_desc['open']) >= 1)

            return {"data": np.array(msgs_volumes).reshape((-1, 1)), "target": np.array(prices_changes)}


def classify_reddit_volume(train_dates_range, test_dates_range, window_length):
    train_set = get_prices_changes_to_msgs_volume_set(train_dates_range[0],
                                                       train_dates_range[1],
                                                       window_length)
    test_set = get_prices_changes_to_msgs_volume_set(test_dates_range[0],
                                                      test_dates_range[1],
                                                      window_length)
    classifier = MultinomialNB()
    # classifier = SVC()
    # classifier = RandomForestClassifier(n_estimators=1000, random_state=0)
    classifier.fit(train_set['data'], train_set['target'])

    predicted = classifier.predict(test_set['data'])
    print("Train:: PriceUp-%d PriceDown-%d" % (train_set['target'].sum(), train_set['target'].size - train_set['target'].sum()))
    print("Test:: PriceUp-%d PriceDown-%d" % (test_set['target'].sum(), test_set['target'].size - test_set['target'].sum()))
    print("Predicted Correctly: %.5f" % np.mean(predicted == test_set['target']))


def classify_reddit_text(train_dates_range, test_dates_range, window_length):
    msgs_type = 'All'
    train_set = get_one_msg_to_price_window_change_set(train_dates_range[0],
                                                       train_dates_range[1],
                                                       window_length,
                                                       5000,
                                                       msgs_type)
    test_set = get_one_msg_to_price_window_change_set(test_dates_range[0],
                                                      test_dates_range[1],
                                                      window_length,
                                                      2000,
                                                      msgs_type)

    x = vectorizer.fit_transform(train_set['data'] + test_set['data']).toarray()

    tfidf_transformer = TfidfTransformer()
    x_train_tfidf = tfidf_transformer.fit_transform(x)

    train_set['features'] = x_train_tfidf[:len(train_set['data']), :]
    test_set['features'] = x_train_tfidf[-len(test_set['data']):, :]

    classifier = MultinomialNB()
    # classifier = SVC()
    # classifier = RandomForestClassifier(n_estimators=1000, random_state=0)
    classifier.fit(train_set['features'], train_set['target'])

    predicted = classifier.predict(test_set['features'])
    print("Train:: PriceUp-%d PriceDown-%d" % (train_set['target'].sum(), train_set['target'].size - train_set['target'].sum()))
    print("Test:: PriceUp-%d PriceDown-%d" % (test_set['target'].sum(), test_set['target'].size - test_set['target'].sum()))
    print("Predicted Correctly: %.5f" % np.mean(predicted == test_set['target']))


windows_length = [30*60, 2*3600, 6*3600, 24*3600, 48*3600]
train_dates_range = (1430232149, 1483281060) # 28/04/2015 to 01/01/2017
test_dates_range = (1483281060+windows_length[-1], 1493240000-windows_length[-1]) # 01/01/2017 to 26/04/2017
for window in windows_length:
    print("---Window Length: %.2f Hours---" % (window / 3600))
    classify_reddit_text(train_dates_range, test_dates_range, window)
