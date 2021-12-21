#!/usr/bin/python3
import os

# some PUR specific helper functions
"""
get pur files by year
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

    pur_by_year = []

    for filename in os.listdir(download_directory):
        if filename.startswith("pur"):
            print(os.path.join(download_directory, filename))
            pur_by_year.append(os.path.join(download_directory, filename))
            continue
        else:
            continue

    return pur_by_year;

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
def read_year(year_directory='/Users/denbanek/Downloads/pur_data_uncompressed/pur2018'):

    udc_components = []

    for filename in os.listdir(year_directory):
        if filename.startswith("udc"):
            udc_components.append(os.path.join(year_directory, filename))
            continue
        else:
            continue

    return udc_components

"""
reads and processes all the text of a single file
"""
def read_text(file='/Users/denbanek/Downloads/pur_data_uncompressed/pur2018/udc18_01.txt'):

    with open(file, mode='r') as f:
        lines = f.readlines()
    return lines