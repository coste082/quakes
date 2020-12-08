import pandas as pd
import config
import mysql.connector 
from mysql.connector import errorcode
from sqlalchemy import create_engine

def create_database(cursor, database):
    """Creates database. if one does not exist."""
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(database))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

# CSV import 
df = pd.read_csv('earthquakes_download.csv')

#Connect to AWS RDS
cnx = mysql.connector.connect(
        host = config.host,
        user = config.user,
        password = config.passwd)
print(cnx)
cursor = cnx.cursor()
db_name = 'okquakes'

#Create database if not already existing.
try:
    cursor.execute("USE {}".format(db_name))
    print("Using existing db")
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(db_name))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor, db_name)
        print("Database {} created successfully.".format(db_name))
        cnx.database = db_name
    else:
        print(err)
        exit(1)

#Create connection point to database
engine = create_engine("mysql+mysqlconnector://{user}:{pw}@{host}/{db}"
         .format(user = config.user,
         pw= config.passwd,
         host = config.host,
         db = config.db_name))

#Push data
try:
    df.to_sql('okquakes', con=engine, if_exists='fail')
except:
    print('data already loaded.')


#Test query
test = pd.read_sql('select * from okquakes;', con=engine) 
print(test.head)
