import mariadb
import csv

mydb = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': "HivewayDB"
}

conn = mariadb.connect(**mydb)

myCursor = conn.cursor()


# while I store the car_id in the search_result, check if there's the same car_id with the same date as the starting date
# avoid using foreign keys for the CAR_ID to avoid restrictions.
# ----------------------------------------------------------------
#                          DB Functions
# ----------------------------------------------------------------
def show_db():
    myCursor.execute("SHOW DATABASES")
    for db in myCursor:
        print(db)


def show_tables():
    # to check the tables in the DB
    myCursor.execute("SHOW TABLES")
    for tb in myCursor:
        print(tb)


def create_database():
    # How to create a database
    myCursor.execute("CREATE DATABASE HivewayDB")

    # To show the DBs created
    show_db()


def create_CAR_table():
    # to create tables in your database, you should add the database attribute in mydb above.
    myCursor.execute("CREATE TABLE CAR (CID int NOT NULL AUTO_INCREMENT,"
                     "CAR_ID varchar(255), "
                     "PLATFORM varchar(255), "
                     "PLATFORM_ID varchar(255), "
                     "MODEL varchar(255), "
                     "MODEL_YEAR varchar(255), "
                     "NO_OF_SEATS varchar(255), "
                     "TRANSMISSION varchar(255), "
                     "ENGINE_TYPE varchar(255), "
                     "MILEAGE varchar(255), "
                     "ADDRESS varchar(255),"
                     "PRIMARY KEY (CID))")
    show_tables()


def create_STATIC_ADDRESSES_table():
    # to create tables in your database, you should add the database attribute in mydb above.
    myCursor.execute("CREATE TABLE STATIC_ADDRESSES (SA_ID int NOT NULL AUTO_INCREMENT, "
                     "LATITUDE varchar(255), "
                     "LONGITUDE varchar(255), "
                     "FULL_ADDRESS varchar(255),"
                     "PRIMARY KEY (SA_ID))")
    show_tables()


def create_SEARCH_RESULT_table():
    # to create tables in your database, you should add the database attribute in mydb above.
    myCursor.execute("CREATE TABLE SEARCH_RESULT (SR_ID int NOT NULL AUTO_INCREMENT, "
                     "CAR_ID varchar(255), "
                     "SA_ID varchar(255), "
                     "DISTANCE varchar(255), "
                     "SR_INDEX varchar(255), "
                     "SCRAPED_DATE date, "
                     "STARTING_DATE date, "
                     "DURATION varchar(255), "
                     "PRICE varchar(255),"
                     "SCRAPED_CAR_URL varchar(255),"
                     "PRIMARY KEY (SR_ID))")
    show_tables()


# def get_CAR_by_ID (CAR_ID, CATEGORY, SCRAPED_CAR_URL):

def insert_car(CAR_ID,
               PLATFORM,
               PLATFORM_ID,
               MODEL,
               MODEL_YEAR,
               NO_OF_SEATS,
               TRANSMISSION,
               ENGINE_TYPE,
               MILEAGE,
               ADDRESS):
    # to insert data into table from database
    # sqlFormula = "INSERT INTO students (name, age) VALUES (%s, %s)"
    # students = [("Rachel", 22), ("Rami", 23), ("Bhavin", 25)]
    # myCursor.executemany(sqlFormula, students)
    myresult = check_duplicate_car_id(CAR_ID)

    if (len(myresult) == 0):
        sqlFormula = "Insert INTO CAR (CAR_ID, " \
                     "PLATFORM, " \
                     "PLATFORM_ID, " \
                     "MODEL, " \
                     "MODEL_YEAR, " \
                     "NO_OF_SEATS, " \
                     "TRANSMISSION, " \
                     "ENGINE_TYPE, " \
                     "MILEAGE," \
                     "ADDRESS)" \
                     "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        theCar = (CAR_ID,
                  PLATFORM,
                  PLATFORM_ID,
                  MODEL,
                  MODEL_YEAR,
                  NO_OF_SEATS,
                  TRANSMISSION,
                  ENGINE_TYPE,
                  MILEAGE,
                  ADDRESS)

        myCursor.execute(sqlFormula, theCar)
        conn.commit()


def check_duplicate_data_by_date_search_result(CAR_ID, STARTING_DATE):
    # query with WHERE
    myQuery = "SELECT CAR_ID, STARTING_DATE FROM SEARCH_RESULT WHERE CAR_ID = %s AND STARTING_DATE = %s"
    myCursor.execute(myQuery, (CAR_ID, STARTING_DATE))
    myresult = myCursor.fetchall()
    return myresult


def check_duplicate_car_id(CAR_ID):
    # query with WHERE
    myQuery = "SELECT CAR_ID FROM CAR WHERE CAR_ID ="+CAR_ID
    myCursor.execute(myQuery)
    myresult = myCursor.fetchall()
    return myresult


def insert_searched_result(CAR_ID,
                           SA_ID,
                           DISTANCE,
                           SR_INDEX,
                           SCRAPED_DATE,
                           STARTING_DATE,
                           DURATION,
                           PRICE,
                           SCRAPED_CAR_URL):
    # # query with WHERE
    # myQuery = "SELECT CAR_ID, STARTING_DATE FROM SEARCH_RESULT WHERE CAR_ID = %s AND STARTING_DATE = %s"
    # myCursor.execute(myQuery, (CAR_ID, STARTING_DATE))
    # myresult = myCursor.fetchall()

    # for row in myresult:
    #     print(row)

    myresult = check_duplicate_data_by_date_search_result(CAR_ID, STARTING_DATE)

    if (len(myresult) == 0):
        sqlFormula = "Insert INTO SEARCH_RESULT (" \
                     "CAR_ID, " \
                     "SA_ID, " \
                     "DISTANCE, " \
                     "SR_INDEX, " \
                     "SCRAPED_DATE, " \
                     "STARTING_DATE, " \
                     "DURATION, " \
                     "PRICE," \
                     "SCRAPED_CAR_URL)" \
                     "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        searched_result = (
            CAR_ID,
            SA_ID,
            DISTANCE,
            SR_INDEX,
            SCRAPED_DATE,
            STARTING_DATE,
            DURATION,
            PRICE,
            SCRAPED_CAR_URL)

        myCursor.execute(sqlFormula, searched_result)
        conn.commit()


def store_STATIC_ADDRESSES(fileName):
    # The Address is ..., the Latitude is ... and the Longitude is ...
    with open(fileName) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                # print(f'[ID: {row[0]}], [Address: {row[1]}], [Latitude :{row[2]}], [Longitude: {row[3]}]')
                sqlFormula = "Insert INTO STATIC_ADDRESSES (" \
                             "LATITUDE, " \
                             "LONGITUDE, " \
                             "FULL_ADDRESS)" \
                             "VALUES (%s, %s, %s)"

                static_address = (
                    row[2],
                    row[3],
                    row[1])

                line_count += 1
                myCursor.execute(sqlFormula, static_address)
                conn.commit()


def get_STATIC_ADDRESSES():
    myQuery = "SELECT LATITUDE, LONGITUDE, FULL_ADDRESS FROM STATIC_ADDRESSES"
    myCursor.execute(myQuery)
    static_address = myCursor.fetchall()
    return static_address
    # print(static_address[0][2])


def get_TODAYs_DISTINCT_CAR_URLs(STARTING_DATE):
    myQuery = "SELECT DISTINCT CAR_ID, SCRAPED_CAR_URL FROM SEARCH_RESULT WHERE STARTING_DATE = %s"
    date = (STARTING_DATE,)
    myCursor.execute(myQuery, date)
    todays_URL = myCursor.fetchall()
    return todays_URL

# from the search result table, i need today's distinct car_id with their respective URLs (Function)
# def get_STATIC_ADDRESS():

#
# def get_CAR_url():


# else:
#     print("The records already exists. No room for duplicates...")


# ----------------------------------------------------------------
#                          DB Execution
# ----------------------------------------------------------------
# myCursor.execute("Use HivewayDB;")
# create_CAR_table()
# create_STATIC_ADDRESSES_table()
# create_SEARCH_RESULT_table()
# create_searched_result('1', '1', '200', '1', '2010-11-11', '2010-1-11', '30', '300', 'https:')
# store_STATIC_ADDRESSES('static_addresses.csv')
# get_STATIC_ADDRESSES()
#insert_car("1", "1", "1", "1", "1", "1", "1", "1", "1", "1")
