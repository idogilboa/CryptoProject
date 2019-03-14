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


def query_threads_set(start_date, end_date, window_length, num_of_threads):
    with sqlite3.connect(DB_DIR) as connection:
        cursor = connection.cursor()
        threads_query = "SELECT date, coin, num_comments, score, title||selftext as text from Threads " \
                        "WHERE coin != 'Unknown' and coin != 'BTC' and coin != 'NXT' and " \
                        "date BETWEEN %d and %d and title NOT LIKE '%%Daily Discussion%%'" \
                        "ORDER BY num_comments + score DESC LIMIT %d" % \
                (start_date, end_date, num_of_threads)
        cursor.execute(threads_query)
        threads = cursor.fetchall()

        tags = []
        threads_texts = []
        for i, thread in enumerate(threads):
            start_date = thread[0]
            end_date = start_date + window_length
            coin = thread[1]
            history_window_query = "SELECT close from History WHERE coin = '%s' AND date BETWEEN %d and %d ORDER BY date ASC" % \
                    (coin, start_date, end_date)
            cursor.execute(history_window_query)
            prices = cursor.fetchall()
            if len(prices) <= (window_length / (5*60)) - 1:
                continue
            threads_texts.append(thread[4])
            price_value = prices[0][0] / prices[-1][0]
            tags.append(price_value >= 1)

    return {"data": threads_texts, "target": np.array(tags)}


def query_comments_set(start_date, end_date, window_length, num_of_comments):
    with sqlite3.connect(DB_DIR) as connection:
        cursor = connection.cursor()
        comments_query = "SELECT date, coin, score, body as text from Comments " \
                        "WHERE coin != 'Unknown' and coin != 'BTC' and coin != 'NXT' and " \
                        "date BETWEEN %d and %d " \
                        "ORDER BY score DESC LIMIT %d" % \
                (start_date, end_date, num_of_comments)
        cursor.execute(comments_query)
        comments = cursor.fetchall()

        tags = []
        comments_texts = []
        for i, comment in enumerate(comments):
            start_date = comment[0]
            end_date = start_date + window_length
            coin = comment[1]
            history_window_query = "SELECT close from History WHERE coin = '%s' AND date BETWEEN %d and %d ORDER BY date ASC" % \
                    (coin, start_date, end_date)
            cursor.execute(history_window_query)
            prices = cursor.fetchall()
            if len(prices) <= (window_length / (5*60)) - 1:
                continue
            comments_texts.append(comment[3])
            price_value = prices[0][0] / prices[-1][0]
            tags.append(price_value >= 1)

    return {"data": comments_texts, "target": np.array(tags)}


def classify_reddit(train_dates_range, test_dates_range, window_length):
    train_set = query_comments_set(train_dates_range[0], train_dates_range[1], window_length, 5000)
    test_set = query_comments_set(test_dates_range[0], test_dates_range[1], window_length, 2000)

    x = vectorizer.fit_transform(train_set['data'] + test_set['data']).toarray()

    tfidf_transformer = TfidfTransformer()
    x_train_tfidf = tfidf_transformer.fit_transform(x)

    train_set['features'] = x_train_tfidf[:len(train_set['data']), :]
    test_set['features'] = x_train_tfidf[-len(test_set['data']):, :]

    classifier = MultinomialNB()
    classifier = SVC()
    # classifier = RandomForestClassifier(n_estimators=1000, random_state=0)
    classifier.fit(train_set['features'], train_set['target'])

    predicted = classifier.predict(test_set['features'])
    print("Train:: PriceUp-%d PriceDown-%d" % (train_set['target'].sum(), train_set['target'].size - train_set['target'].sum()))
    print("Test:: PriceUp-%d PriceDown-%d" % (test_set['target'].sum(), test_set['target'].size - test_set['target'].sum()))
    print("Predicted Correctly: %.5f" % np.mean(predicted == test_set['target']))


windows_length = [30*60, 2*3600, 6*3600, 24*3600, 48*3600]
train_dates_range = (1430232149, 1483281060) # 28/04/2015 to 01/01/2017
test_dates_range = (1483281060+windows_length[-1], 1493240400-windows_length[-1]) # 01/01/2017 to 26/04/2017
for window in windows_length:
    print("---Window Length: %.2f Hours---" % (window / 3600))
    classify_reddit(train_dates_range, test_dates_range, window)
