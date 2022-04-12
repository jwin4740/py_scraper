import sys

from driver import MainDriver
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
    cursor.execute(
        "INSERT INTO reports"
        "(VAERS_ID,REPORT_RECEIVED_DATE,STATE,AGE_YEARS,SEX,SYMPTOM_TEXT,DIED,DATE_DIED,LIFE_THREATENING_ILLNESS,"
        "ER_VISIT,HOSPITAL_VIST,DAYS_IN_HOSPITAL,DISABILITY,VACCINATION_DATE,AE_ONSET_DATE,NUM_DAYS_TO_ONSET,"
        "LAB_DATA,VACCINE_ADMINISTERED_BY,OTHER_MEDICATIONS,PATIENT_HISTORY,PRIOR_VACCINE,VAERS_FORM_NUMBER,"
        "DATE_FORM_COMPLETED,BIRTH_DEFECT,OFFICE_VISIT,ER_ED_VISIT,ALLERGIES) "
        " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (record['VAERS_ID'], record['REPORT_RECEIVED_DATE'], record['STATE'], record['AGE_YEARS'], record['SEX'],
         record['SYMPTOM_TEXT'], record['DIED'], record['DATE_DIED'], record['LIFE_THREATENING_ILLNESS'],
         record['ER_VISIT'], record['HOSPITAL_VIST'], record['DAYS_IN_HOSPITAL'], record['DISABILITY'],
         record['VACCINATION_DATE'], record['AE_ONSET_DATE'], record['NUM_DAYS_TO_ONSET'], record['LAB_DATA'],
         record['VACCINE_ADMINISTERED_BY'], record['OTHER_MEDICATIONS'], record['PATIENT_HISTORY'],
         record['PRIOR_VACCINE'], record['VAERS_FORM_NUMBER'], record['DATE_FORM_COMPLETED'], record['BIRTH_DEFECT'],
         record['OFFICE_VISIT'], record['ER_ED_VISIT'], record['ALLERGIES']))

    Conn.commit()
    print('inserted')

    cursor.close()
    Conn.close()


def read_csv(year, file_name):
    file_path = ROOT_PATH / year / file_name

    with open(file_path, 'r') as f:
        rows = csv.reader(f, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
        for r in rows:
            if rows.line_num < 3 and rows.line_num > 1:
                record = {}
                print(int(r[0]))
                print(r[1])
                print(r[9])

                record['VAERS_ID'] = int(r[0])
                record['REPORT_RECEIVED_DATE'] = datetime.strptime(r[1], "%m/%d/%Y")
                record['STATE'] = r[2]
                record['AGE_YEARS'] = float(r[3])
                record['SEX'] = r[6]
                record['SYMPTOM_TEXT'] = r[8]
                if r[9] == 'Y':
                    record['DIED'] = True
                else:
                    record['DIED'] = False
                print(r[10])
                print(type(r[10]))
                if r[10] != '':
                    print(r[10])
                    record['DATE_DIED'] = datetime.strptime(r[10], "%m/%d/%Y")
                else:
                    record['DATE_DIED'] = None
                record['LIFE_THREATENING_ILLNESS'] = None
                record['ER_VISIT'] = None
                record['HOSPITAL_VIST'] = None
                record['DAYS_IN_HOSPITAL'] = None
                record['DISABILITY'] = None
                record['VACCINATION_DATE'] = None
                record['AE_ONSET_DATE'] = None
                record['NUM_DAYS_TO_ONSET'] = None
                record['LAB_DATA'] = None
                record['VACCINE_ADMINISTERED_BY'] = None
                record['OTHER_MEDICATIONS'] = None
                record['PATIENT_HISTORY'] = None
                record['PRIOR_VACCINE'] = None
                record['VAERS_FORM_NUMBER'] = None
                record['DATE_FORM_COMPLETED'] = None
                record['BIRTH_DEFECT'] = None
                record['OFFICE_VISIT'] = None
                record['ER_ED_VISIT'] = None
                record['ALLERGIES'] = None
                insert(record)

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


read_csv(sys.argv[2], sys.argv[3])
# m = '01/02/2020'
#
# d = datetime.strptime(m, "%m/%d/%Y")
# print(d.date())
