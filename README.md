# twitter-data
SOEN 363 assignment. Using Twitter API to download tweets about recession and storing them in a remote database data.
The script is writter in python 3, therefore you need to install python 3 to run it.


## Prepare Dev Environment
* install python 3
* apt-get install python3-venv


## Load Dependencies required for Development
Python packages are install with pip.
    
## install pip
    sudo apt installed python3-pip

## Required Packages
* configparser
* requests
* mysql.connector
* bitdotio
* psycopg2

## Install packages
    pip3 install <package_name>
    
## Configuration file 
In the venv directory, we have our config.ini script where we store the environment variables. 
You must create a twitter developer account and get your API keys to fill the variables. 
You also need the DB token, which is available upon request.

## Environment variables in config.ini
    [twitter]

    api_key = your_twitter_api_key
    api_key_secret = your_twitter_api_key_secret
    access_token = your_twitter_access_token
    access_token_secret = your_twitter_access_token
    bearer_token = your_twitter_bearer_token

    [bitio]
    bitio_token = available_upon_request
    
 ## Run the script
    python3 main.py
    
    
## Script Description
1- The script will attempt to connect to the local database using the bitdotio package.

2- It will then read the twitter configs from the config.ini file.

3- The script will use the bearer token to make a GET request to the Twitter API endpoint https://api.twitter.com/2/tweets/search/recent to get recent tweets that match the query "Recession -is:retweet".

4- The script will parse the tweets and users objects in the JSON response and write each tweet to a file called json_tweets_list.json and each user to a file called json_users_list.json. The entire JSON response will be saved to a file called json_list.json.

5- The script will then insert the tweets and tweet public metrics data into the tweets and tweet_public_metric tables in the database.

6- The script will repeat the above steps every 5 seconds.
 
