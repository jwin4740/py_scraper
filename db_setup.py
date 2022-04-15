from db import start_db
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import errorcode
import os
import sys

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

Conn = start_db(DB_HOST, DB_USER, DB_PASSWORD)

TABLES = {}
TABLES['reports'] = (
    " CREATE TABLE `reports` ("
    " VAERS_ID INT UNIQUE NOT NULL,"
    " REPORT_RECEIVED_DATE DATE NOT NULL,"
    " STATE VARCHAR(2),"
    " AGE_YEARS INT,"
    " SEX VARCHAR(1),"
    " SYMPTOM_TEXT TEXT,"
    " DIED BOOLEAN,"
    " DATE_DIED DATE,"
    " LIFE_THREATENING_ILLNESS BOOLEAN,"
    " ER_VISIT BOOLEAN,"
    " HOSPITAL_VISIT BOOLEAN,"
    " DAYS_IN_HOSPITAL INT,"
    " DISABILITY BOOLEAN,"
    " VACCINATION_DATE DATE,"
    " AE_ONSET_DATE DATE,"
    " NUM_DAYS_TO_ONSET INT,"
    " LAB_DATA TEXT,"
    " VACCINE_ADMINISTERED_BY VARCHAR(3),"
    " OTHER_MEDICATIONS TEXT,"
    " CURRENT_ILLNESS TEXT,"
    " PATIENT_HISTORY TEXT,"
    " PRIOR_VACCINE VARCHAR(300),"
    " VAERS_FORM_NUMBER INT,"
    " DATE_FORM_COMPLETED DATE,"
    " BIRTH_DEFECT BOOLEAN,"
    " OFFICE_VISIT BOOLEAN,"
    " ER_ED_VISIT BOOLEAN,"
    " ALLERGIES TEXT,"
    " PRIMARY KEY (VAERS_ID)"
    ") ENGINE=InnoDB")

TABLES['vaccine'] = (
    " CREATE TABLE `vaccine` ("
    " ID INT AUTO_INCREMENT UNIQUE NOT NULL,"
    " VAERS_ID INT NOT NULL,"
    " VACCINATION_TYPE VARCHAR(50),"
    " VACCINATION_MANUFACTURER VARCHAR(100),"
    " VACCINATION_LOT VARCHAR(20),"
    " VACCINATION_DOSE_SERIES VARCHAR(10),"
    " VACCINATION_ROUTE VARCHAR(60),"
    " VACCINATION_SITE VARCHAR(100),"
    " VACCINATION_NAME VARCHAR(1000),"
    " PRIMARY KEY (ID),"
    " CONSTRAINT `vaccine_fk_1` FOREIGN KEY (`VAERS_ID`)"
    "    REFERENCES `reports` (`VAERS_ID`) ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)
#
# TABLES['symptoms'] = (
#     " CREATE TABLE `symptoms` ("
#     " ID INT AUTO_INCREMENT UNIQUE NOT NULL,"
#     " VAERS_ID INT NOT NULL,"
#     " SYMPTOM1 VARCHAR(100),"
#     " SYMPTOM_VERSION1 INT,"
#     " SYMPTOM2 VARCHAR(100),"
#     " SYMPTOM_VERSION2 INT,"
#     " SYMPTOM3 VARCHAR(100),"
#     " SYMPTOM_VERSION3 INT,"
#     " SYMPTOM4 VARCHAR(100),"
#     " SYMPTOM_VERSION4 INT,"
#     " SYMPTOM5 VARCHAR(100),"
#     " SYMPTOM_VERSION5 INT,"
#     " PRIMARY KEY (ID),"
#     " CONSTRAINT `symptoms_fk_1` FOREIGN KEY (`VAERS_ID`)"
#     "    REFERENCES `reports` (`VAERS_ID`) ON DELETE CASCADE"
#     ") ENGINE=InnoDB"
# )

cursor = Conn.cursor()


def create_tables():
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

    cursor.close()
    Conn.close()


def drop_table(table_name='reports'):
    print('dropping table')
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.close()
    Conn.close()


if sys.argv[1] == 'create':
    create_tables()
elif sys.argv[1] == 'drop':
    drop_table()
