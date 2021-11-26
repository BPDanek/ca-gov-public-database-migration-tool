from db_helpers.pur_postgres_population_script import PostgresInterface, read_from_download_folder, read_year, read_text

def do_import():

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
            if content_lines[line_index][column_index] is '':
                content_lines[line_index][column_index] = None
            elif content_lines[line_index][column_index] is '""':
                content_lines[line_index][column_index] = None
            elif content_lines[line_index][column_index] is ',,':
                content_lines[line_index][column_index] = None
            elif content_lines[line_index][column_index].__len__() is 0:
                content_lines[line_index][column_index] = None
            elif '""' in content_lines[line_index][column_index]: # some weird edge case in the data?
                content_lines[line_index][column_index] = None

    # declare the text file data structure
    file_text_as_dictionary = {}

    # initialize the text file data structure with the two lines
    for col_index in range(len(labels)):
        file_text_as_dictionary[labels[col_index]] = [content_lines[0][col_index], content_lines[1][col_index]]

    print(len(content_lines))
    print(len(content_lines[:][0]))
    print(len(labels))

    for col_index in range(len(labels)):
        existing_values = file_text_as_dictionary[labels[col_index]]

        for line in content_lines[2:]:
            existing_values.append(line[col_index])

    print("done")
    return file_text_as_dictionary


if __name__ == '__main__':
    pgAccess = PostgresInterface()

    raw_db_as_text = do_import()

    print(pgAccess.add_key_columns())  # test!
    print(pgAccess.connect_add_pur_entry(db=raw_db_as_text))
