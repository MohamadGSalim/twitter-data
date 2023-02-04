import configparser
import requests
import mysql.connector
import json
from time import sleep


#connect to database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password="root",
    port='3306',
    database='twitter-data'
)

mycursor = conn.cursor()


#create table
# create_table = """CREATE TABLE tweets(
#                         id long,
#                         name varchar(2000)
#
#                         );"""
#
# mycursor.execute(create_table)
# conn.commit()


#read twitter configs
config = configparser.ConfigParser()
config.read('venv/config.ini')
bearer_token = config['twitter']['bearer_token']


#api-endpoint
URL = "https://api.twitter.com/2/tweets/search/recent"

#params
query = "elon musk"
expansions = ['author_id']
media_fields = []
place_fields = []
poll_fields = []
sort_order = ""
tweet_fields = "public_metrics,created_at,lang,entities,referenced_tweets"
user_fields = ['created_at,location,public_metrics,verified']
next_token = None


PARAMS = {'tweet.fields': tweet_fields,
          'user.fields': user_fields,
          'expansions': expansions,
          'query': query,
          'max_results': 10,
          'next_token': next_token
          }

headers = {"Authorization": "Bearer " + bearer_token}


# sending get request and saving the response as response object
response = requests.get(url=URL, params=PARAMS, headers=headers)
json_response = response.json()

#the Request URL
print("URL: \n" + response.url + "\n")

#the tweets
tweets_objects = json_response['data']
print("tweets list: ")
print(tweets_objects)
print()

#the users
users_objects = json_response['includes']['users']
print("users list: ")
print(users_objects)
print()

#meta data
print("meta data: ")
print(json_response['meta'])
print()


#file for everything
with open("json_list.json", "a") as f3:
    f3.write(",\n")
    json.dump(json_response, f3)


def handle_tweets(tweets):

    try:
        f1 = open("json_tweets_list.json", "a")

        #parse tweets save in Json file and save in a DB
        for tweet in tweets:

            #save each tweet in Json File
            f1.write(",\n")
            json.dump(tweet, f1)

            #parse Tweet
            tweet_id = tweet['id']
            tweet_author_id = tweet['author_id']
            tweet_text = tweet['text']
            tweet_lang = tweet['lang']
            tweet_created_at = tweet['created_at']

            # save in Database
            sql_query = "INSERT IGNORE  INTO tweets (tweet_id, author_id,text,lang,created_at) VALUES (%s, %s, %s, %s, %s)"
            values = (tweet_id, tweet_author_id, tweet_text, tweet_lang, tweet_created_at)
            mycursor.execute(sql_query, values)



            #tweet public metrics
            tweet_id = tweet_id
            tweet_retweet_count = tweet['public_metrics']['retweet_count']
            tweet_reply_count = tweet['public_metrics']['reply_count']
            tweet_like_count = tweet['public_metrics']['like_count']
            tweet_quote_count = tweet['public_metrics']['quote_count']
            tweet_impression_count = tweet['public_metrics']['impression_count']

            # save in Database
            sql_query = "INSERT IGNORE  INTO tweet_public_metric (tweet_id, retweet_count, reply_count, like_count, quote_count, impression_count) VALUES (%s, %s, %s, %s, %s, %s)"
            values = (tweet_id, tweet_retweet_count, tweet_reply_count, tweet_like_count, tweet_quote_count, tweet_impression_count)
            mycursor.execute(sql_query, values)

            # referenced tweets
            if 'referenced_tweets' in tweet:
                for tweet_reference in tweet['referenced_tweets']:
                    tweet_reference_type = tweet_reference['type']
                    tweet_reference_id = tweet_reference['id']

                    # save in Database
                    sql_query = "INSERT IGNORE  INTO referenced_tweet (ref_tweet_id, type) VALUES (%s, %s)"
                    values = (tweet_reference_id, tweet_reference_type)
                    mycursor.execute(sql_query, values)

                    # save in Database
                    sql_query = "INSERT IGNORE  INTO tweet_references (tweet_id, ref_tweet_id) VALUES (%s, %s)"
                    values = (tweet_id, tweet_reference_id)
                    mycursor.execute(sql_query, values)

            # hashtags
            if 'entities' in tweet and 'hashtags' in tweet['entities']:
                for tweet_tags in tweet['entities']['hashtags']:
                    tweet_hashtag = tweet_tags['tag']

                    # save in Database
                    sql_query = "INSERT IGNORE  INTO hashtags (tag) VALUES (%s)"
                    values = (tweet_hashtag,)
                    mycursor.execute(sql_query, values)

                    # save in Database
                    sql_query = "INSERT IGNORE  INTO tweet_tags (tweet_id, tag) VALUES (%s,%s)"
                    values = (tweet_id, tweet_hashtag)
                    mycursor.execute(sql_query, values)

            conn.commit()

    finally:
        f1.close()


def handle_users(users):

    try:
        f1 = open("json_users_list.json", "a")

        # parse users save in Json file and save in a DB
        for user in users:
            # save each tweet in Json File

            f1.write(",\n")
            json.dump(user, f1)

            # parse user
            user_id = user['id']
            user_username = user['username']
            user_name = user['name']
            user_verified = user['verified']
            user_created_at = user['created_at']
            user_location = None
            if 'location' in user:
                user_location = user['location']

            # save in Database
            sql_query = "INSERT IGNORE  INTO users (user_id, username, name, verified, location, created_at) VALUES (%s, %s, %s, %s, %s, %s)"
            values = (user_id, user_username, user_name, user_verified, user_location, user_created_at)

            mycursor.execute(sql_query, values)

            #user public metrics
            user_followers_count = user['public_metrics']['followers_count']
            user_following_count = user['public_metrics']['following_count']
            user_tweet_count = user['public_metrics']['tweet_count']
            user_listed_count = user['public_metrics']['listed_count']

            # save in Database
            sql_query = "INSERT IGNORE  INTO user_public_metric (user_id, followers_count, following_count, tweet_count, listed_count) VALUES (%s, %s, %s, %s, %s)"
            values = (user_id, user_followers_count, user_following_count, user_tweet_count, user_listed_count)

            mycursor.execute(sql_query, values)

            conn.commit()

    finally:
        f1.close()



handle_users(users_objects)
handle_tweets(tweets_objects)



page = 1
print(f"page {page}")

# #paginate through results
while json_response['meta']['next_token']:
    sleep(2)
    PARAMS['next_token'] = json_response['meta']['next_token']

    response = requests.get(url = URL, params = PARAMS, headers=headers)
    json_response = response.json()

    print("URL: \n" + response.url + "\n")

    # the tweets
    tweets_objects = json_response['data']

    #the users
    users_objects = json_response['includes']['users']


    # file for everything
    with open("json_list.json", "a") as f3:
        f3.write(",\n")
        json.dump(json_response, f3)

    # parse users save in Json file and save in a DB
    handle_users(users_objects)

    # parse tweets save in Json file and save in a DB
    handle_tweets(tweets_objects)





    page = page + 1
    print(f"page {page}")

    if page == 5:
        exit(0)



