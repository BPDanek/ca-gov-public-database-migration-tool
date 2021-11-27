import numpy
from db_helpers.pur_postgres_population_script import PostgresInterface, read_from_download_folder, read_year, read_text

"""
currently only does import for single file within a year
"""
def do_import():

    # columns that need special string casting
    # column 23 and 24 would need it too, but they are logged in some special way
    # column 14 is date, we will store it as string for now.
    # indexed from 1 based on documents, made too index by 0 for python manipulation
    CHAR_COLUMNS = numpy.array([8, 10, 12, 14, 16, 17, 18, 19, 20, 21, 22, 25, 27, 31, 33]) - 1

    download_path = '/Users/denbanek/Downloads/pur_data_uncompressed/'

    years = read_from_download_folder(download_path)
    year = years[1]

    files = read_year(year)
    file_ = files[1]

    text = read_text(file_)

    labels = text[0]
    labels = labels.replace('\n', '')  # remove end of line character
    labels = labels.split(',')  # split by comma into a list

    content_lines = text[1:]

    # for m content lines, n labels O(m * (len(lines) + n)) ~O(15,000 * (160 + 35)) computations
    # loop through the lines and clean them
    for line_index in range(len(content_lines)):

        # O(len(line)) + O(len(line))
        # remove artifact from .txt file
        content_lines[line_index] = content_lines[line_index].replace('\n', '').split(',')

        # labels is n, runs in O(n)
        # three different null/empty types occur in the data
        for column_index in range(len(labels)):
            current_value = content_lines[line_index][column_index]
            if current_value is ('' or '""' or ',,') or '""' in current_value or current_value.__len__() is 0:
                content_lines[line_index][column_index] = None
            # elif column_index in CHAR_COLUMNS:
            #     content_lines[line_index][column_index] = """'""" + current_value + """'"""

    return content_lines

if __name__ == '__main__':
    pgAccess = PostgresInterface()
    raw_db_as_lines = do_import()
    print(pgAccess.add_key_columns())  # test!
    print(pgAccess.connect_add_pur_entry(db=raw_db_as_lines))
