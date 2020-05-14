# Taken from https://www.postgresqltutorial.com/postgresql-python/connect/
#!/usr/bin/python
from configparser import ConfigParser
import sys
env = sys.argv[1]

def config(filename='common_files/database.ini'):
    if env == "prod":
        section='postgresql_prod'
    elif env == "stage":
        section='postgresql_stage'
    else:
        print("please include environment arguement (e.g. $ python data_pipeline.py prod)")
    
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db