import configparser
import requests
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

# api_key = config['twitter']['api_key']
# api_key_secret = config['twitter']['api_key_secret']
#
# access_token = config['twitter']['access_token']
# access_token_secret = config['twitter']['access_token_secret']

bearer_token =  config['twitter']['bearer_token']


#api-endpoint
URL = "https://api.twitter.com/2/tweets/search/recent"

query = "jordan peterson -is:retweet"
expansions = ['author_id']
media_fields = []
place_fields = []
poll_fields = []
sort_order = ""
tweet_fields = "public_metrics"
user_fields = ['profile_image_url,location']




PARAMS = {'tweet.fields': tweet_fields,
          'user.fields': user_fields,
          'expansions': expansions,
          'query': query,
          'max_results': 10
          }

headers = {"Authorization": "Bearer " + bearer_token}


# sending get request and saving the response as response object
r = requests.get(url = URL, params = PARAMS, headers=headers)

print(r.url)
print()
print(r.json())














