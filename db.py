import mysql.connector


def start_db(host, user, passwd):
    # Connect to an existing database
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=passwd, database='vaers_tmp')

    return conn

    print(mydb)
