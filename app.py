import sys

from driver import MainDriver
from db import start_db
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import os

load_dotenv()  # take environment variables from .env.

# Code of your application, which uses environment variables (e.g. from `os.environ` or
# `os.getenv`) as if they came from the actual environment.


# CDC_BASE_URL = 'https://vaers.hhs.gov/data.html'
# d = MainDriver()
#
# d.driver.get(CDC_BASE_URL)

ROOT_PATH = Path('../../../Nextcloud/PROJECT_RESCUE_OFFENSE/VAERS')
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

Conn = start_db(DB_HOST, DB_USER, DB_PASSWORD)
cursor = Conn.cursor()


def insert(r):
    cursor.execute(
        "INSERT INTO reports"
        "(VAERS_ID, REPORT_RECEIVED_DATE)"
        " VALUES (%s, %s)", (r['VAERS_ID'], r['REPORT_RECEIVED_DATE']))
    Conn.commit()
    print('inserted')

    cursor.close()
    Conn.close()


def read_csv(year, file_name):
    file_path = ROOT_PATH / year / file_name

    with open(file_path, 'r') as f:
        lines = f.readlines()
        # print(lines[0])
        # print(lines[1].split(','))
        # print(lines[2].split(','))

    for x in range(1, 2):
        record = {}
        r = lines[x].split(',')
        record['VAERS_ID'] = int(r[0])
        record['REPORT_RECEIVED_DATE'] = datetime.strptime(r[1], "%m/%d/%Y")
        insert(record)


read_csv(sys.argv[2], sys.argv[3])
# m = '01/02/2020'
#
# d = datetime.strptime(m, "%m/%d/%Y")
# print(d.date())
