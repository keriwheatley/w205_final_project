# 
# load_data.py
# Provides functions to download data from various sources and insert into database
# 
# 

import requests
import datetime
import json
import psycopg2
import pandas as pd

#############################################################################
# Helper Functions
#############################################################################
def isNewer(strDate1, strDate2):
    """Simple utility function for comparing a particular string date format
    strDate1 and strDate2 are date strings in 'YYYY-MM' format
    returns True if strDate2 is newer than strDate1, False otherwise
    NOTE: interprets empty string as "Beginning of time" meaning:
        if strDate1 is empty, returns True
        if strDate1 is not empty but strDate2 IS empty, return False"""
    if len(strDate1) == 0:
        return True
    elif len(strDate2) == 0:
        return False
    else:
        return (int(strDate2[0:4]) > int(strDate1[0:4]) or \
               (int(strDate2[0:4]) == int(strDate1[0:4]) and int(strDate2[-2:]) > int(strDate1[-2:])))


#############################################################################
# Data Load Functions
#############################################################################

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
                try:
                    values = ""
                    columns = ""
                    for i in row:
                        columns += '"' + str(i) + '",'
                        values += "'" + str(row[i]).replace("'","") + "',"
                    columns = columns[:-1]
                    values = values[:-1]
                    #counter += 1
                    #print(counter, end=" ")
                    #print("INSERT INTO " + table_name + " (" + columns + ") VALUES (" + values + ");");
                
                    cur.execute("INSERT INTO " + table_name + " (" + columns + ") VALUES (" + values + ");");
                except Exception as e:
                    # placeholder - sometimes there are weird characters that the db won't take
                    # MUST SORT THIS OUT RATHER THAN SKIPPING THEM!
                    print "exception encountered:\n" +  str(e)
            
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
            

# You will need to implement truncate_table as well
# Basically, if that value is true, you need to truncate the db table and reload all the data
# it is mostly for testing, but needs to be implemented for consistency
# 
# Also, you need to modify slightly so that it doesn't just check the last column(date) because
# on the first run it will need to load data for all dates thus far.
# Another scenario is if somehow it doesn't run one month (who knows how), it should be able to 
# see that there are 2 new date columns (or however many new columns there are) and load them both
# shouldn't be too tough, do something like start at the last column and move towards the first.
# Compare each date to "last_update_value" with isNewer, if it returns true, add that column to a 
# list, and when isNewer returns false you have all the new dates. 
# 
def load_data_Zillow( dict_db_connect, url, table_name, 
                      truncate_table = False, last_update_value =  ""):
    """return True if no errors and no new data to load
    return the newest date string that was loaded into the DB to be stored for next time
    return False if errors occurred"""
    
    print "Loading Zillow data from (" + url + ")"
    
    try:
        #Read the CSV from the url directly into a pandas dataframe
        df = pd.read_csv(url)
        
        # PUT THIS WHEREVER IT SHOULD LOGICALLY GO
        #    - replace newDate with the actual date extracted from the csv file
        #    - last_update_value will contain what this function returned last time it updated the DB
        # check if there is newer data
        newDate = list(df)[-1]  # (this is a placeholder for my own testing, do this part however is best)
        if not isNewer( last_update_value, newDate):
            return True
        
        print "loaded: " + str(df.shape[0]) + " rows, with " + str(df.shape[1]) + " columns."
        
        #Drop all rows that aren't for Austin
        df = df.drop(df[df.City != "Austin"].index)
        
        #Drop all rows that aren't for Texas
        df = df.drop(df[df.State != "TX"].index)
        
        #Get the Last Columns
        last_col_index = len(df.columns)-1
        
        #List of columns to drop. 
        #First 6 cols are regionid (ZIP), region name, city, metro area and county
        drop_cols = list(range(6,last_col_index))
        
        #Drop all columns after the 6th until the last
        df.drop(df.columns[drop_cols], axis = 1, inplace = True)
        
        #Get the date of the latest data
        date = list(df.columns.values)[6]
        
        #Add new columns with date values
        df['date'] = date
        
        # REMOVE? I DON'T SEE ANY NEED TO STORE FILES LOCALLY ONCE THEY ARE IN THE DB
        #Save to .csv
        #df.to_csv('updated_%s' % csv, header = False)

        # Connect to database
        conn = psycopg2.connect( database = dict_db_connect["database"], 
                                 user = dict_db_connect["user"],
                                 password = dict_db_connect["password"],
                                 host = dict_db_connect["host"],
                                 port = dict_db_connect["port"])
        cur = conn.cursor()
        
        # this is where the data needs to be added to the DB
        # assume that the table is created before the initial load in a bash script
        count = 0
        for index, row in df.iterrows():
            # for testing
            if count > 10:
                break
            
            #
            #insert values into DB
            #
            # for testing, let's print a couple of column headers and some row values
            print list(df)[0], list(df)[6]
            print str(row['RegionID']), str(row[date])
            
            count += 1
        
        return date
    
    except Exception as e:
        print("There was an exception while updating the Zillow data:")
        print(e.args)
        print(e)
        return False





