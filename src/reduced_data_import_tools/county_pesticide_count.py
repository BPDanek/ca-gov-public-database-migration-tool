import time

from psycopg2.psycopg1 import cursor

from db_helpers.pur_postgres_population_script import read_year, read_text

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

