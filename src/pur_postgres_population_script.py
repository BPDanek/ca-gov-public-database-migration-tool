#!/usr/bin/python3
import psycopg2
import os
from postgres_config import config

"""
open connection, run one command, close connection
todo: for bigger operations, lets run multiple commands at once.
"""
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

    # folder with years
    for filename in os.listdir(download_directory):
        if filename.startswith("pur"):
            print(os.path.join(download_directory, filename))
            # try to walk files
            read_year(os.path.join(download_directory, filename))
            continue
        else:
            continue

    return


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
def read_year(year_directory):

    return

def read_year_component():

    return

def read_line():

    # send's line to sql
    return

if __name__ == '__main__':
    read_year()