import time
from db_helpers.pur_helper_functions import read_year, read_text

"""
pgAccess: PostgresIntrface from db_helpers
year_path: string pointing to a year directory from the pur database. 

return result_codes: ...
"""
def import_year(pgAccess, year_path):
    files = read_year(year_path)
    print("Importing year documents with [{}] files. -- {}".format(len(files), year_path))
    for file in files:
        cleaned_page_lines = import_page(file)
        start = time.time()
        result_code = pgAccess.connect_execute_batch(db_component=cleaned_page_lines)
        end = time.time()
        print("Time for file [{}] -- [{}] -- result: [{}] ".format(file, end - start, result_code))


"""
file_path: string pointing to a text file from a years PUR. Each year tends to have > 50 files. 

return content_lines: a str[] 
"""
def import_page(file_path):

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

    # time, O(): outer loop is run O(n). Inner loop is run O(m), as is .split and .replace. --> O(n(m + m + m)) == O(nm)
    # what is complxity of .replace() and .split()? (assume its linear in worst case)
    # let n be length of str[], ~15000 in practice
    # let m be number of characters in each line, ~150 in practice
    for line_index in range(len(content_lines)):

        # clean line from artifacts
        content_lines[line_index] = content_lines[line_index].replace('\n', '').split(',')

        # three different null/empty types occur in the data, these need to be standardized.
        for column_index in range(len(labels)):
            current_value = content_lines[line_index][column_index]
            if current_value is ('' or '""' or ',,') or '""' in current_value or current_value.__len__() is 0:
                content_lines[line_index][column_index] = None

    return content_lines