#!/usr/bin/python3
import psycopg2
from db_helpers.postgres_config import config

"""
DEPRACATED FEATURE WE MAY BRING BACK
Constant value Data fields in the UDC table that we are keeping for business use case (there are additional fields we won't use

for now: just use county code for location
next step: add PLS system primitives (Combination of the county, meridian, township, range and section fields identifies a unique location within the PLS)

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

"""
Pesticidue Usage Report Migrator. Takes PUR folders and pushes them to a postgres instance. 

about sql connections: 
https://www.psycopg.org/docs/
* a connection opens up a communication session with the db, and allows access to the program
* a cursor allows python code to execute sql commands within the database session. They are bound to the connection 
and all cursor execution is within the context of the db session/connection
"""
class PURMigrator:

    def __init__(self, *args):

        # get postgres config parameters
        self.params = config()['connectionstring'] # abstract local development vs prod development.
        return

    """
    Connect to the PostgreSQL database server and run a single query
    returns 1 for success, -1 for exception
    """
    def connect_execute_single(self, sql_query, params=None):
        return_code = 1
        conn = None
        try:
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(self.params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            prior = cur.rowcount

            if params is not None:
                cur.execute(cur.mogrify(sql_query, params))
            else:
                cur.execute(sql_query)

            updated_rows = cur.rowcount

            # commit db changes
            conn.commit()

            print((updated_rows - prior), 'rows added by query.', sql_query)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return_code = -1
        finally:
            if cur is not None:
                cur.close()
                print('Cursor closed.')
            if conn is not None:
                conn.close()
                print('Database connection closed.')

        return return_code

    """
    Define some columns in table for partial (but not reduced) UDC table. Partial because we use 7/25 fields.
    Complies with UDC_VALID_DATA_KEYS defined at the beggining of this file. 
    """
    def udc_add_key_columns(self):
        return (self.connect_execute_single("""
        CREATE TABLE IF NOT EXISTS ca_udc (
            id serial PRIMARY KEY,
            prodno integer,
            chem_code integer,
            lbs_chm_used numeric,
            applic_dt varchar(10),  
            county_cd varchar(4), 
            township varchar(4)
        ); 
        """))

    """
    Define the reduced (precomputed) UDC table fields.
    Here we track the pesticide count for each county.
    
    Future work: do pesticide count per year, so we can track more than one year. 
    """
    def reduced_udc_add_key_columns(self):
        return (self.connect_execute_single("""
        CREATE TABLE IF NOT EXISTS ca_reduced_udc (
            id serial PRIMARY KEY,
            county_cd varchar(4), 
            pesticide_count integer
        ); 
        """))
