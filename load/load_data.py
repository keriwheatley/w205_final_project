# 
# load_data.py
# Provides functions to download data from various sources and insert into database
# 
# 

import requests
import datetime
import json
import psycopg2

def load_data_SODA( dict_db_connect, url, table_name, 
                    truncate_table = False, last_update_field = "", last_update_value =  ""):
    """Loads all data since last_update_value (if provided) from table_name 
    at url using the SODA API""" 
    try:
        # Start runtime
        start_time = datetime.datetime.now()
        print("Starting data extract for data source (" + table_name + ") at time (" + str(start_time) + ").")
        
        # Connect to database
        conn = psycopg2.connect( database = dict_db_connect["database"], 
                                 user = dict_db_connect["user"],
                                 password = dict_db_connect["password"],
                                 host = dict_db_connect["host"],
                                 port = dict_db_connect["port"])
        cur = conn.cursor()
        
        request = url
        chunk_size = 50000
        
        if request[-1] != '?': request += '?'

        # Note: we have to chunk the transfer into blocks of 50000 rows                
        request += "$limit=" + str(chunk_size)
        
        # order results by last_update_field
        if len(last_update_field) > 0:
            request += "&$order=" + last_update_field
        
        # get only the rows since the last data load (if options permit)
        if not truncate_table and len(last_update_field) > 0 and len(last_update_value) > 0:
            # get latest values
            # NOTE: last_update_value must be wrapped in quotes or it is seen as an int
            request += "&$where=" + last_update_field + ">\'" + last_update_value + "'"
        else:
            # if we don't have last update info, or we aren't doing incremental updates,
            # truncate the table before insertion
            cur.execute("TRUNCATE TABLE " + table_name + ";");
            #print("Truncated data table.")
        
        # loop until all data is received
        offset = 0
        all_data_loaded = False
        #counter = 0
        while not all_data_loaded:
            requestFull = request + "&$offset=" + str(offset)
            print(requestFull)
            response = requests.get(requestFull, verify=False)
            data = response.json()
            if response.status_code != 200:
                print("Error: Did not complete call to API (status code: " + str(response.status_code) + ")")
                print("Check API call: " + request)
                print(data)
                return False

            # Write each row to data table
            for row in data:
                values = ""
                columns = ""
                for i in row:
                    columns += str(i) + ","                
                    values += "'" + str(row[i]).replace("'","") + "',"
                columns = columns[:-1]
                values = values[:-1]
                #counter += 1
                #print(counter, end=" ")
                #print("INSERT INTO " + table_name + " (" + columns + ") VALUES (" + values + ");");
                cur.execute("INSERT INTO " + table_name + " (" + columns + ") VALUES (" + values + ");");
            
            # determine if there is more data
            if len(data) == chunk_size:
                offset += chunk_size
            else:
                offset += len(data)
                all_data_loaded = True
        
        # Commit changes to table and close connection
        conn.commit()
        conn.close()
        
        print ("Loaded " + str(offset) + " records to data source (" + table_name + ").")
        print ("Ended data extract for data source (" + table_name + ") at time (" + 
                str(datetime.datetime.now()) + ").")
    
        # if doing incremental updates, save last value inserted, if there is one
        if len(data) > 0 and not truncate_table and len(last_update_field) > 0:
            last_update_value = row[last_update_field]
    
    except Exception as inst:
        print(inst.args)
        print(inst)
        return False
    
    # if doing incremental updates, return last value inserted to use next time
    if not truncate_table and len(last_update_field) > 0:
        return str(last_update_value)
    else:
        return True
            
load_data_SODA( url = "https://data.austintexas.gov/resource/awym-tx82.json",
                table_name = "commercial_water_consumption")
