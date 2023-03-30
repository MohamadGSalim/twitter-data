import pymongo
import configparser
import psycopg2

print("Script started0")

# Read Bit.io credentials
config = configparser.ConfigParser()
config.read('venv/config.ini')
bitio_user = config['bitio']['user']
bitio_password = config['bitio']['password']
bitio_host = config['bitio']['host']
bitio_port = config['bitio']['port']
bitio_db = config['bitio']['db']
print("Script started1")

# Connect to Bit.io database
bitio_conn = psycopg2.connect(
    host=bitio_host,
    user=bitio_user,
    password=bitio_password,
    port=bitio_port,
    dbname=bitio_db
)

bitio_cursor = bitio_conn.cursor()
print("Script started2")

# Read MongoDB credentials
config.read('venv/config.ini')
mongodb_connection_string = config['mongodb']['connection_string']
mongodb_db = config['mongodb']['db']
print("Script started3")

# Connect to MongoDB
client = pymongo.MongoClient(mongodb_connection_string)
db = client[mongodb_db]

print("Script started4")

# Fetch and process users
bitio_cursor.execute("SELECT u.*, upm.* FROM users u JOIN user_public_metric upm ON u.user_id = upm.user_id LIMIT 1000000")
user_records = bitio_cursor.fetchall()
user_column_names = [desc[0] for desc in bitio_cursor.description]

user_documents = []
for record in user_records:
    document = {}
    for i in range(len(user_column_names)):
        document[user_column_names[i]] = record[i]
    user_documents.append(document)

user_collection = db['users']
user_collection.insert_many(user_documents)

# Fetch and process tweets
bitio_cursor.execute("SELECT t.*, tpm.* FROM tweets t JOIN tweet_public_metric tpm ON t.tweet_id = tpm.tweet_id LIMIT 1000000")
tweet_records = bitio_cursor.fetchall()
tweet_column_names = [desc[0] for desc in bitio_cursor.description]

tweet_documents = []
for record in tweet_records:
    document = {}
    for i in range(len(tweet_column_names)):
        document[tweet_column_names[i]] = record[i]

    # Add hashtags to the tweet document
    tweet_id = document['tweet_id']
    bitio_cursor.execute(f"SELECT tt.tag FROM tweet_tags tt WHERE tt.tweet_id = {tweet_id}")
    tags = [tag[0] for tag in bitio_cursor.fetchall()]
    document['hashtags'] = tags

    # Add referenced_tweet to the tweet document
    bitio_cursor.execute(f"SELECT tr.ref_tweet_id, rt.type FROM tweet_references tr JOIN referenced_tweet rt ON tr.ref_tweet_id = rt.ref_tweet_id WHERE tr.tweet_id = {tweet_id}")
    refs = [{'ref_tweet_id': ref[0], 'type': ref[1]} for ref in bitio_cursor.fetchall()]
    document['referenced_tweet'] = refs

    tweet_documents.append(document)

tweet_collection = db['tweets']
tweet_collection.insert_many(tweet_documents)

print("Data transfer from Bit.io to MongoDB completed.")
