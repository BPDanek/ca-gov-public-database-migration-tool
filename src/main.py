import time
from functools import reduce

from db_helpers.pur_postgres_population_script import PURMigrator
from db_helpers.pur_helper_functions import read_from_download_folder

from raw_data_import_tools.udc_table_import import import_year

from reduced_data_import_tools.county_pesticide_count import import_year_reduced

from reduced_data_import_tools.yearly_county_pesticide_count import buildYearlyPesticideCountTable, getLineCount, normalize


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
        counts = []
        for yp in yearPaths:

            # get the path to the udc file defines the pesticide report in the area

            countyYearPath = f"{yp}/udc{yp[-2:]}_{normalize(county)}.txt"
            counts.append(getLineCount(file=countyYearPath))

        # (this is pointless to have in the code, but it should be true)
        # assert len(counts) == len(yearPaths), "DEV ERROR: Insufficient counts generated"

        query = """INSERT INTO ca_reduced_udc(county_cd, pesticide_count) values (%s, %s);"""

        query_prefix = """INSERT INTO ca_reduced_udc("""
        query_middle = reduce(lambda body, append: body + ", " + "count_" + append + " integer", years, "")
        query_postfix = """) VALUES ("""
        query_postpostfix = reduce(lambda body, append: body + ", " + "count_" + append + " integer", years, "")
        # todo: compleete building this insertion string, this one sucks tits holes

        params = county_code, pesticide_count

        pgAccess = PURMigrator()
        start = time.time()
        result_code = pgAccess.connect_execute_single(sql_query=query, params=params)
        end = time.time()

        pgAccess.connect_execute_single(query)





if __name__ == '__main__':
    # migrateReducedUDCTable(download_path='/Users/denbanek/Downloads/REDUCED_pur_data_uncompressed/');
    migrateYearlyReducedTable(download_path='/Users/denbanek/Downloads/yearly_pur_data_uncompressed/')