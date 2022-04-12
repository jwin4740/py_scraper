from driver import MainDriver
from db import start_db
from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.


# Code of your application, which uses environment variables (e.g. from `os.environ` or
# `os.getenv`) as if they came from the actual environment.


# CDC_BASE_URL = 'https://vaers.hhs.gov/data.html'
# d = MainDriver()
#
# d.driver.get(CDC_BASE_URL)

def select():
    return f"SELECT * FROM reports;"


DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

Conn = start_db(DB_HOST, DB_USER, DB_PASSWORD)



def return_text_hashtags():
    cursor = Conn.cursor()
    cursor.execute(select())

    tweet_list = []
    for x in cursor:
        tweet_list.append(x)

    return tweet_list[0]



print(return_text_hashtags())
