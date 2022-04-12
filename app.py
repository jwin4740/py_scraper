import sys

import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
from db import start_db
from dotenv import load_dotenv
from pathlib import Path
import csv
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


def insert(record):
    try:
        cursor.execute(
            "INSERT INTO reports"
            "(VAERS_ID,REPORT_RECEIVED_DATE,STATE,AGE_YEARS,SEX,SYMPTOM_TEXT,DIED,DATE_DIED,LIFE_THREATENING_ILLNESS,"
            "ER_VISIT,HOSPITAL_VISIT,DAYS_IN_HOSPITAL,DISABILITY,VACCINATION_DATE,AE_ONSET_DATE,NUM_DAYS_TO_ONSET,"
            "LAB_DATA,VACCINE_ADMINISTERED_BY,OTHER_MEDICATIONS, CURRENT_ILLNESS, PATIENT_HISTORY,PRIOR_VACCINE,"
            "VAERS_FORM_NUMBER, "
            "DATE_FORM_COMPLETED,BIRTH_DEFECT,OFFICE_VISIT,ER_ED_VISIT,ALLERGIES) "
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (record['VAERS_ID'], record['REPORT_RECEIVED_DATE'], record['STATE'], record['AGE_YEARS'], record['SEX'],
             record['SYMPTOM_TEXT'], record['DIED'], record['DATE_DIED'], record['LIFE_THREATENING_ILLNESS'],
             record['ER_VISIT'], record['HOSPITAL_VISIT'], record['DAYS_IN_HOSPITAL'], record['DISABILITY'],
             record['VACCINATION_DATE'], record['AE_ONSET_DATE'], record['NUM_DAYS_TO_ONSET'], record['LAB_DATA'],
             record['VACCINE_ADMINISTERED_BY'], record['OTHER_MEDICATIONS'], record['CURRENT_ILLNESS'],
             record['PATIENT_HISTORY'],
             record['PRIOR_VACCINE'], record['VAERS_FORM_NUMBER'], record['DATE_FORM_COMPLETED'],
             record['BIRTH_DEFECT'],
             record['OFFICE_VISIT'], record['ER_ED_VISIT'], record['ALLERGIES']))

        Conn.commit()
        print('inserted record')
    except Error as E:
        print(E)


def insert_vaccine(record):
    try:
        cursor.execute(
            "INSERT INTO vaccine"
            "(VAERS_ID,VACCINATION_TYPE,VACCINATION_MANUFACTURER, VACCINATION_LOT, VACCINATION_DOSE_SERIES, "
            "VACCINATION_ROUTE, VACCINATION_SITE, VACCINATION_NAME) "
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (record['VAERS_ID'], record['VACCINATION_TYPE'], record['VACCINATION_MANUFACTURER'],
             record['VACCINATION_LOT'], record['VACCINATION_DOSE_SERIES'], record['VACCINATION_ROUTE'],
             record['VACCINATION_SITE'], record['VACCINATION_NAME']))

        Conn.commit()
        print('inserted record')
    except Error as E:
        print(E)


def read_csv_report(year, file_name):
    file_path = ROOT_PATH / year / file_name

    with open(file_path, 'r') as f:
        rows = csv.reader(f, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
        for r in rows:
            if rows.line_num > 1:
                record = {}

                record['VAERS_ID'] = int(r[0])
                record['REPORT_RECEIVED_DATE'] = datetime.strptime(r[1], "%m/%d/%Y")
                record['STATE'] = r[2]
                if r[3] != '':
                    record['AGE_YEARS'] = float(r[3])
                else:
                    record['AGE_YEARS'] = None
                record['SEX'] = r[6]
                record['SYMPTOM_TEXT'] = r[8]
                if r[9] == 'Y':
                    record['DIED'] = True
                else:
                    record['DIED'] = False

                if r[10] != '':
                    record['DATE_DIED'] = datetime.strptime(r[10], "%m/%d/%Y")
                else:
                    record['DATE_DIED'] = None
                if r[11] == 'Y':
                    record['LIFE_THREATENING_ILLNESS'] = True
                else:
                    record['LIFE_THREATENING_ILLNESS'] = False

                if r[12] == 'Y':
                    record['ER_VISIT'] = True
                else:
                    record['ER_VISIT'] = False
                if r[13] == 'Y':
                    record['HOSPITAL_VISIT'] = True
                else:
                    record['HOSPITAL_VISIT'] = False
                if r[14] != '':
                    record['DAYS_IN_HOSPITAL'] = int(r[14])
                else:
                    record['DAYS_IN_HOSPITAL'] = 0

                if r[16] == 'Y':
                    record['DISABILITY'] = True
                else:
                    record['DISABILITY'] = False

                if r[18] != '':
                    record['VACCINATION_DATE'] = datetime.strptime(r[18], "%m/%d/%Y")
                else:
                    record['VACCINATION_DATE'] = None

                if r[19] != '':
                    record['AE_ONSET_DATE'] = datetime.strptime(r[19], "%m/%d/%Y")
                else:
                    record['AE_ONSET_DATE'] = None

                if r[20] != '':
                    record['NUM_DAYS_TO_ONSET'] = int(r[20])
                else:
                    record['NUM_DAYS_TO_ONSET'] = 0

                record['LAB_DATA'] = r[21]
                record['VACCINE_ADMINISTERED_BY'] = r[22]
                record['OTHER_MEDICATIONS'] = r[24]
                record['CURRENT_ILLNESS'] = r[25]
                record['PATIENT_HISTORY'] = r[26]

                record['PRIOR_VACCINE'] = r[27]
                record['VAERS_FORM_NUMBER'] = r[29]

                if r[30] != '':
                    record['DATE_FORM_COMPLETED'] = datetime.strptime(r[30], "%m/%d/%Y")
                else:
                    record['DATE_FORM_COMPLETED'] = None
                if r[31] == 'Y':
                    record['BIRTH_DEFECT'] = True
                else:
                    record['BIRTH_DEFECT'] = False

                if r[32] == 'Y':
                    record['OFFICE_VISIT'] = True
                else:
                    record['OFFICE_VISIT'] = False

                if r[33] == 'Y':
                    record['ER_ED_VISIT'] = True
                else:
                    record['ER_ED_VISIT'] = False
                record['ALLERGIES'] = r[34]
                insert(record)
        cursor.close()
        Conn.close()
    # lines = f.readlines()
    # print(lines[1])


def read_csv_vaccine(year, file_name):
    file_path = ROOT_PATH / year / file_name

    with open(file_path, 'r') as f:
        rows = csv.reader(f, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
        for r in rows:
            if rows.line_num > 1:
                record = {}
                record['VAERS_ID'] = int(r[0])
                record['VACCINATION_TYPE'] = r[1]
                record['VACCINATION_MANUFACTURER'] = r[2]
                record['VACCINATION_LOT'] = r[3]
                record['VACCINATION_DOSE_SERIES'] = r[4]
                record['VACCINATION_ROUTE'] = r[5]
                record['VACCINATION_SITE'] = r[6]
                record['VACCINATION_NAME'] = r[7]

                insert_vaccine(record)
        cursor.close()
        Conn.close()
    # lines = f.readlines()
    # print(lines[1])


# for x in range(1, 2):
#     record = {}
#     r = lines[x].split(',')
#     print(r)
#     # record['VAERS_ID'] = int(r[0])
#     # record['REPORT_RECEIVED_DATE'] = datetime.strptime(r[1], "%m/%d/%Y")
#     # record['STATE'] = r[2]
#     # record['AGE_YEARS'] = float(r[3])
#     # record['SEX'] = r[6]
#     # record['SYMPTOM_TEXT'] = r[8]
#     # record['DIED'] = r[9]
#     # record['DATE_DIED'] = datetime.strptime(r[10], "%m/%d/%Y")
#     # record['LIFE_THREATENING_ILLNESS'] =
#     # record['ER_VISIT'] =
#     # record['HOSPITAL_VIST'] =
#     # record['DAYS_IN_HOSPITAL'] =
#     # record['DISABILITY'] =
#     # record['VACCINATION_DATE'] =
#     # record['AE_ONSET_DATE'] =
#     # record['NUM_DAYS_TO_ONSET'] =
#     # record['LAB_DATA'] =
#     # record['VACCINE_ADMINISTERED_BY'] =
#     # record['OTHER_MEDICATIONS'] =
#     # record['PATIENT_HISTORY'] =
#     # record['PRIOR_VACCINE'] =
#     # record['VAERS_FORM_NUMBER'] =
#     # record['DATE_FORM_COMPLETED'] =
#     # record['BIRTH_DEFECT'] =
#     # record['OFFICE_VISIT'] =
#     # record['ER_ED_VISIT'] =
#     # record['ALLERGIES'] =
#     # insert(record)


# read_csv_report(sys.argv[2], sys.argv[3])

read_csv_vaccine(sys.argv[2], sys.argv[3])
# m = '01/02/2020'
#
# d = datetime.strptime(m, "%m/%d/%Y")
# print(d.date())
