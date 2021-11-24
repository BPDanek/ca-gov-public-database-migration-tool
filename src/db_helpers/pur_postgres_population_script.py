#!/usr/bin/python3
import psycopg2
import os
from db_helpers.postgres_config import config

class PostgresInterface:
    """
    open connection, run one command, close connection
    todo: for bigger operations, lets run multiple commands at once.
    """

    def __init__(self):
        self.connect_execute() # test

    def __init__(self):
        self.connect_execute() # test

    def connect_execute(sql_query='SELECT version()'):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            print('PostgreSQL database version:')
            cur.execute(sql_query)

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)

            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
            return "success"


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

def read_line():

    # send's line to sql
    return

# if __name__ == '__main__':
#     read_from_download_folder()
#     read_year()
#     read_text()