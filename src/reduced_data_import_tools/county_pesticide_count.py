"""
this script defines and populates a db table which contains the pesticide count per county for a single year.

the pesticide counts are all for a single year (specified in the file)
 id | county_cd | pesticide_count
----+-----------+-----------------
  1 | 03        |            6181
  ....
"""

import time
from db_helpers.pur_helper_functions import read_year, read_text, read_from_download_folder
from db_helpers.pur_postgres_population_script import PURMigrator
from raw_data_import_tools.udc_table_import import import_year

"""
pgAccess: PostgresIntrface from db_helpers
year_path: string pointing to a year directory from the pur database. 

return result_codes: ...
"""
def import_year_reduced(pgAccess, year_path):
    files = read_year(year_path)
    print("Importing year documents with [{}] files. -- {}".format(len(files), year_path))
    for file in files:
        county_code = file[-6] + file[-5] # anticipate all files are in format of "[path]/udcYY_(C1)(C2).txt" where C1 and C2 are the two digits that denote county code.
        pesticide_count = get_page_count(file)

        query = """INSERT INTO ca_reduced_udc(county_cd, pesticide_count) values (%s, %s);"""
        params = county_code, pesticide_count

        start = time.time()
        result_code = pgAccess.connect_execute_single(sql_query=query, params=params)
        end = time.time()

        print("Time for file [{}] -- [{}] -- result: [{}] ".format(file, end - start, result_code))


"""
file_path: string pointing to a text file from a years PUR. Each year has 58 files. 

return the number of pesticides available in each county
"""
def get_page_count(file_path):

    # get text file as one big array of strings (each line in the file is a new array element)
    # type -- str[] (approximately 15000 x 35),
    text = read_text(file_path)

    # process labels (not that it really matters
    # type -- str
    labels = text[0].replace('\n', '').split(',')  # remove end of line character
    print("Importing file with [{}] lines and [{}] labels. -- {}".format(len(text) - 1, len(labels), file_path))

    # loop through the content lines and clean them.
    # type -- str[]
    content_lines = text[1:]

    return len(content_lines)

"""
build table like 
 id | county_cd | pesticide_count 
----+-----------+-----------------  
"""
def migrateUDCTable(download_path):

    # make a Postgres Interface
    pgAccess = PURMigrator()

    # generate full udc table if it doesn't exist
    print("Declaring table result: [{}]".format(pgAccess.udc_add_key_columns()))

    # get all the years (path) available for the Pesticide Usage Report
    years = read_from_download_folder(download_path)
    for year in years:
        print("\nYear import: [{}]".format(year))
        start = time.time()
        # import year and log the result codes for each year import
        result_codes = import_year(pgAccess=pgAccess, year_path=year)
        end = time.time()
        print("Year import time: [{}]".format(end - start))
