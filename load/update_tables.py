# 
# update_tables.py
# Parses config file to load all data and build updated aggregates
# config file must be local and named 'config.ini'
# 

#import requests
#import datetime
#import json
#import psycopg2
import configparser as cp
import sys
from load_data import *
import aggregates as aggs

#
# still to do
#
# should "URL encode" GET parameters - look in SODA docs
# 
# get an "app token" to pass in with requests?
# 

# config file strings
CONFIG_FILE_NAME = 'config.ini'
DATASOURCE_SECTION = "DATASOURCE"
AGGREGATES_SECTION = "AGGREGATES"
URL_KEY = 'url'
TABLE_NAME_KEY= 'table_name'
LAST_UPDATE_COL_KEY = 'last_update_col'
LAST_UPDATE_VAL_KEY = 'last_update_val'
TYPE_KEY = 'type'
TRUNCATE_KEY = 'truncate'


config = cp.configparser()

# this line keeps the config parser from converting option names to lowercase
config.optionxform = str

try:
    config.read(CONFIG_FILE_NAME)
    
except Exception as e:
    sys.exit("Cannot read config file:\n" + str(e))

# as of now, database info is hardcoded
# we will read it here if we decide to put in config file
dict_db_connect = { "database" : "finalproject",
                    "user" : "postgres",
                    "password" : "pass",
                    "host" : "localhost",
                    "port" : "5432"}

# semi-crude but very simple way to validate DATASOURCE config file sections
# first pass verifies there is enough info, on success, second pass loads data
no_errors = True
config_ok = True
for i in range(2):
    for s in config.sections():
        if s.startswith(DATASOURCE_SECTION):
            if URL_KEY in config.options(s):
                url_value = config.get(s, URL_KEY)
            else:
                url_value = ""
            
            if TABLE_NAME_KEY in config.options(s):
                table_name_value = config.get(s, TABLE_NAME_KEY)
            else:
                table_name_value = ""
            
            if LAST_UPDATE_COL_KEY in config.options(s):
                last_update_col_value = config.get(s, LAST_UPDATE_COL_KEY)
            else:
                last_update_col_value = ""
            
            if LAST_UPDATE_VAL_KEY in config.options(s):
                last_update_val_value = config.get(s, LAST_UPDATE_VAL_KEY)
            else:
                last_update_val_value = ""
            
            if TYPE_KEY in config.options(s):
                type_value = config.get(s, TYPE_KEY)
            else:
                type_value = ""
            
            if TRUNCATE_KEY in config.options(s):
                truncate_value = True if config.get(s, TRUNCATE_KEY)[0].lower() == 't' else False
            else:
                truncate_value = False
            
            # at a minimum, we need table name, url, and type
            # if any are missing, error out so it can be fixed
            if len(table_name_value) == 0 or len(url_value) == 0 or len(type_value) == 0:
                print("Config file error:")            
                print("In " + s + "table_name, url, and type are required")
                config_ok = False
                
            # everything is read in for this source, on verify pass, load the data
            if i == 1:
                if type_value == "SODA":
                    ret = load_data_SODA( dict_db_connect, url_value, table_name_value, 
                                          truncate_value, last_update_col_value, last_update_val_value)
                    
                    #check ret - it will be either a boolean or a string
                    # here is where we'd write back to the config file if we are not truncating
                    # and storing the last_update_val
                    if isinstance(ret, bool):
                        if ret == False:
                            no_errors = False
                    else:
                        config.set(s, LAST_UPDATE_VAL_KEY, ret)
                        config.write(CONFIG_FILE_NAME)
                        
                else:
                    # unknown type - don't error out, just skip
                    # this could be a convenient way to add sources that aren't quite ready yet to the config
                    print("Unknown source type: " + type_value + "\nSkipping source at " + url_value)
    
    if not config_ok:
        sys.exit("Cannot proceed until errors are fixed\nexiting...")

# do we want to run aggregations even if there were errors?
if no_errors:
    
    # run aggregations
    try:
        # make sure the config file contains aggregates
        if not config.has_section( AGGREGATES_SECTION):
            sys.exit("Error: No sections named 'AGGREGATES' in config file")
    
        # parse AGGREGATES section of config file and call each function
        for function in config.options( AGGREGATES_SECTION):
            #read any parameters to pass to the function
            opts = config.get( AGGREGATES_SECTION, function)
    
            # call the function        
            print("Calling: " + function)
            if len(opts) == 0: 
                result = getattr(aggs, function)()
            else:
                params = [p.strip('"\' ') for p in opts.strip('"\'').split(',')]
                print("With parameter list: " + str(params))
                result = getattr(aggs, function)(params)
    
            # do something with the result
    except Exception as e:
            print(e)



