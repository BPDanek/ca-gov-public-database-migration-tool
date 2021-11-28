import time
from db_helpers.pur_postgres_population_script import PostgresInterface
from raw_data_import_tools.udc_table_import import import_page, import_year
from db_helpers.pur_postgres_population_script import read_from_download_folder

def migrateUDCTable(download_path):

    # make a Postgres Interface
    pgAccess = PostgresInterface()

    # generate table if it doesn't exist
    print("Declaring table result: [{}]".format(pgAccess.add_key_columns()))

    # get all the years (path) available for the Pesticide Usage Report
    years = read_from_download_folder(download_path)
    for year in years:
        print("\nYear import: [{}]".format(year))
        start = time.time()
        # import year and log the result codes for each year import
        result_codes = import_year(pgAccess=pgAccess, year_path=year)
        end = time.time()
        print("Year import time: [{}]".format(end - start))

if __name__ == '__main__':
    migrateUDCTable(download_path='/Users/denbanek/Downloads/pur_data_uncompressed/');