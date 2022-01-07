"""
this script defines and populates a db table which contains the pesticide count per county over several years
 id | county_cd | count_2018 | count_2017 |...|count_YYYY
----+-----------+------------+------------+---+-----------
"""

import time
from functools import reduce

from db_helpers.pur_helper_functions import read_year, read_text, read_from_download_folder
from db_helpers.pur_postgres_population_script import PURMigrator

"""
pgAccess: PostgresInterface from db_helpers
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
Define a table in the postgres database which is (key, county_cd, count_2018, count_2017, ..., count_YYYY)
where years: str[] like ['2018', '2017', ..., 'YYYY'] 
"""
def buildYearlyPesticideCountTable(years):

    # generate the sql for table definition
    query_prefix = """CREATE TABLE IF NOT EXISTS ca_yearly_reduced_udc (id serial PRIMARY KEY, county_cd varchar(4)"""
    query_middle = reduce(lambda body, append: body + ", " + "count_" + append + " integer", years, "")
    query_postfix = """); """
    query = query_prefix + query_middle + query_postfix

    pgAccess = PURMigrator()
    return pgAccess.connect_execute_single(query)

"""
reads and processes all the text of a single file
determines the number of pur reports per file, since each line is a pur, 
except the first first which is the column definitions
"""
def getLineCount(file):
    with open(file, mode='r') as f:
        lines = f.readlines()
    return len(lines) - 1

"""
num is a number like type int in other languages
numb is str(num), so it can be normalizd/formattd
1 --> "01"
0 --> "00"
12 --> "12"
"""
normalize = lambda num: (lambda numb: ("0" + numb) if len(numb) == 1 else (numb))(str(num))

def migrateReducedUDCTable(download_path):

    print("~~~~~Reduced Dataset Run~~~~~")

    # make a Postgres Interface
    pgAccess = PURMigrator()

    # generate reduced table if it doesn't exist
    print("Declaring table result: [{}]".format(pgAccess.reduced_udc_add_key_columns()))

    # get all the years (path) available for the Pesticide Usage Report
    years = read_from_download_folder(download_path)
    for year in years:
        print("\nYear import: [{}]".format(year))
        start = time.time()
        # import year and log the result codes for each year import
        result_codes = import_year_reduced(pgAccess=pgAccess, year_path=year)
        end = time.time()
        print("Year import time: [{}]".format(end - start))

def migrateYearlyReducedTable(download_path):

    yearPaths = read_from_download_folder(download_directory=download_path)

    # get the set of years from download path
    # the last 4 digits of the yearPath is the year in human readable form. We will use this to define the columns.
    years = [yp[-4:] for yp in yearPaths]
    print(years)

    buildYearlyPesticideCountTable(years=years)

    counties = range(1, 59)
    for county in counties:

        # get rowcount for each year
        pur_counts = []
        for yp in yearPaths:

            # get the path to the udc file defines the pesticide report in the county
            countyYearPath = f"{yp}/udc{yp[-2:]}_{normalize(county)}.txt"

            # get pesticide count for county @ year
            pur_counts.append(getLineCount(file=countyYearPath))

        # (this is pointless to have in the code, but it should be true)
        # assert len(counts) == len(yearPaths), "DEV ERROR: Insufficient counts generated"

        # query = """INSERT INTO ca_reduced_udc(county_cd, pesticide_count) values (%s, %s);"""

        query_prefix = """INSERT INTO ca_yearly_reduced_udc(county_cd, """
        query_middle = reduce(lambda body, append: body + "count_" + append + ", ", years, "")
        query_postfix = f""") VALUES ({county}, """
        query_postpostfix = reduce(lambda body, _: body  + "%s ,", years, "")
        # todo: compleete building this insertion string, this one sucks tits holes

        query = query_prefix + query_middle[:-2] + query_postfix + query_postpostfix[:-2] + ");"
        params = pur_counts

        pgAccess = PURMigrator()
        start = time.time()
        result_code = pgAccess.connect_execute_single(sql_query=query, params=params)
        end = time.time()

        print("county:", county, "-\t", end - start, "-", "success" if result_code == 1 else "failed")
