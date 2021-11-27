#!/usr/bin/python3
import numpy
import psycopg2
import os
from db_helpers.postgres_config import config


"""
Constant value
Data fields in the UDC table that we are keeping for business use case (there are additional fields we won't use

for now: just usee county code for location
next step: add PLS system primitives (Combination of the county, meridian, township, range and section fields identifies a unique location within the PLS)
"""
UDC_VALID_DATA_KEYS = [
    'prodno',
    'chem_code',
    'lbs_chm_used',
    'applic_dt',
    'county_cd',
    'township'
]

UDC_VALID_DATA_INDECES = numpy.array([2, 3, 5, 14, 16, 18]) - 1
"""
about sql connections: 
https://www.psycopg.org/docs/
* a connection opens up a communication session with the db, and allows access to the program
* a cursor allows python code to execute sql commands within the database session. They are bound to the connection 
and all cursor execution is within the context of the db session/connection
"""
class PostgresInterface:

    def __init__(self, *args):
        self.params = config()
        return

    """
    Connect to the PostgreSQL database server and run a single query
    returns 1 for success, -1 for exception
    """
    def connect_execute_single(self, sql_query):
        return_code = 1
        conn = None
        try:
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**self.params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            prior = cur.rowcount

            cur.execute(sql_query)

            updated_rows = cur.rowcount

            print((updated_rows - prior), 'rows added by query. ')

            # commit db changes
            conn.commit()

            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return_code = -1
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

        return return_code


    """
    Adds columns to table from UDC_VALID_DATA_KEYS. This operation is only additive, not subtractive
    https://www.psycopg.org/docs/usage.html#adaptation-of-python-values-to-sql-types
    """
    def add_key_columns(self):
        self.connect_execute_single("""
        CREATE TABLE IF NOT EXISTS ca_udc (
            id serial PRIMARY KEY,
            prodno integer,
            chem_code integer,
            lbs_chm_used numeric,
            applic_dt varchar(10),  
            county_cd varchar(4), 
            township varchar(4)
        ); 
        """)

    """
    run a batch of requests
    returns 1 for success, -1 for exception
    
    Could just use .execute() in a loop, or could use `https://www.psycopg.org/docs/extras.html#fast-exec`
    for now will use .execute in a loop. 
    
    db is the cleaned rows of the text file
    """
    def connect_add_pur_entry(self, db):

        return_code = 1 # success code by default
        db_session = None # initialize to avoid NPE if .connect returns null

        try:

            print('Connecting to the PostgreSQL database...')
            db_session = psycopg2.connect(**self.params)

            cursor = db_session.cursor()
            prior = cursor.rowcount
            for row in range(len(db)): # 15,XXX x 35 table

                items = []
                for column in range(len(db[0])):
                    if column in UDC_VALID_DATA_INDECES:
                        value = db[row][column]

                        items.append(value)

                print(items)
                prodno, chem_code, lbs_chm_used, applic_dt, county_cd, township = items
                cursor.execute(cursor.mogrify("""INSERT INTO ca_udc(prodno, chem_code, lbs_chm_used, applic_dt, county_cd, township) values (%s, %s, %s, %s, %s, %s);""", (prodno, chem_code, lbs_chm_used, applic_dt, county_cd, township)))

            # https://www.psycopg.org/docs/usage.html#constants-adaptation

            updated_row = cursor.rowcount

            print((updated_row - prior), 'rows updated')

            cursor.close()
            db_session.commit()
            db_session.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return_code = -1 # failure code returned

        finally:
            if db_session is not None:
                db_session.close()
                print('Database connection closed.')

        return return_code



# helper functions

"""
expected directory structure:

"Downloads"
        |---"pur_data_uncompressed"
                    |---"pur2007"
                            |-- {...} 
                    |---"pur2008"
                            |-- {...} 
                    |---"pur2009"
                            |-- {...} 
                        ...
                    |---"pur2018"
                            |-- {...} 
"""
def read_from_download_folder(download_directory='/Users/denbanek/Downloads/pur_data_uncompressed'):

    pur_by_year = []

    for filename in os.listdir(download_directory):
        if filename.startswith("pur"):
            print(os.path.join(download_directory, filename))
            # try to walk files
            pur_by_year.append(os.path.join(download_directory, filename))
            continue
        else:
            continue

    return pur_by_year;

"""
for a specific year import all the file contents of that year 

expected directory structure: 
"pur20XX"
    |-- ...
      ...
    |-- ...
    |--udcXX_01.txt
    |--udcXX_02.txt
      ...
    |--udcXX_YY.txt     

XX is the last two digits of the year
YY is the last highest number the udc text file is split up into (I don't know how high it goes)
"""
def read_year(year_directory='/Users/denbanek/Downloads/pur_data_uncompressed/pur2018'):

    udc_components = []

    for filename in os.listdir(year_directory):
        if filename.startswith("udc"):
            # print(os.path.join(year_directory, filename))
            # try to walk files
            # read_text(os.path.join(year_directory, filename))
            udc_components.append(os.path.join(year_directory, filename))
            continue
        else:
            continue

    return udc_components

"""
reads and processes all the text of a single file
"""
def read_text(file='/Users/denbanek/Downloads/pur_data_uncompressed/pur2018/udc18_01.txt'):

    with open(file, mode='r') as f:
        lines = f.readlines()
        for line in lines:
            if ",," not in line and "use_no" not in line:
                print(line)
            else:
                continue
    return lines