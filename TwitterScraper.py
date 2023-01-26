import snscrape.modules.twitter as sntwitter
import pandas as pd

import configparser
import json

import mysql.connector

#connect to database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password = "root",
    port = '3306',
    database = 'twitter-data'
)

mycursor = conn.cursor()

query = "elon musk -filter:replies"
tweets = []
limit = 1

for tweet in sntwitter.TwitterSearchScraper(query).get_items():
    if len(tweets) == limit:
        break
    else:
        tweets.append(tweet)
        print(vars(tweet))
        id = tweet.id
        name = tweet.user.username
        text = tweet.rawContent


        query = "INSERT INTO tweets (id, name, text) VALUES (%s, %s, %s)"
        values = (id, name, text)

        mycursor.execute(query, values)
        conn.commit()

