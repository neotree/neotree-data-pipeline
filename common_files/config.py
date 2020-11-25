# Taken from https://www.postgresqltutorial.com/postgresql-python/connect/
#!/usr/bin/python
from configparser import ConfigParser
#import libraries
import sys
import logging
import os,stat
# Configuration for Global Logger
filepath = "/var/log/"
#Change file Permissions
os.chmod(filepath, stat.S_IWOTH | stat.S_IROTH)

logging.basicConfig(level=logging.INFO
,filename =filepath+'data_pipeline.log'
,filemode="w",format='%(asctime)s - %(levelname)s - %(message)s'
,datefmt='%d-%b-%y %H:%M:%S')
# set up logging to console
console = logging.StreamHandler();
# set a format which is simpler for console use 
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter) 
# add the handler to the root logger 
logging.getLogger('').addHandler(console) 


if len(sys.argv) > 1:
    env = sys.argv[1]
    def config(filename='common_files/database.ini'):
        if env == "prod":
            section='postgresql_prod'
        elif env == "stage":
            section='postgresql_stage'
        elif env == "dev":
            section ='postgresql_dev'
        
        else:
            logging.error(env,"is not a valid arguement: Valid arguements are (dev stage or prod)");
            sys.exit()
    
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
            logging.error('Section {0} not found in the {1} file'.format(section, filename))
            sys.exit()
            
        logging.info("Ready To Run Data Pipeline in {0} mode".format(env))
        return db
else:
    logging.error("Please include environment arguement (e.g. $ python data_pipeline.py prod)")
    sys.exit()
    