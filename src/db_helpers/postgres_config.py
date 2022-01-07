#!/usr/bin/python3
from configparser import ConfigParser

def config(filename='/Users/denbanek/PycharmProjects/ca-gov-public-database-migration-tool/src/config.ini', section='postgresql'):

    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    db_config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_config[param[0]] = param[1]
        # db['password'] = os.getenv()
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
        db_config = None

    # print(db_config)
    return db_config
