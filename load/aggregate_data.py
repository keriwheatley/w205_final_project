# 
# aggregates.py
# Provides functions to download data from various sources and insert into database
# 
# 

import requests
import datetime
import json
import psycopg2
import pandas as pd

# Will hide urrlib3 warnings for the purposes of this project.
# Currently receiving this error message because an older version of Python is loaded to AMI.
# InsecurePlatformWarning /usr/lib/python2.7/site-packages/requests-2.10.0-py2.7.egg/requests/
# packages/urllib3/connectionpool.py:821: InsecureRequestWarning: Unverified HTTPS request is 
# being made. Adding certificate verification is strongly advised. 
# See: https://urllib3.readthedocs.org/en/latest/security.html 
# InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification 
# is strongly advised. See: https://urllib3.readthedocs.org/en/latest/security.html
# SNIMissingWarning: An HTTPS request has been made, but the SNI (Subject Name Indication) 
# extension to TLS is not available on this platform. This may cause the server to present an 
# incorrect TLS certificate, which can cause validation failures. You can upgrade to a newer 
# version of Python to solve this. For more information, see 
# https://urllib3.readthedocs.org/en/latest/security.html#snimissingwarning.
from requests.packages.urllib3.exceptions import InsecurePlatformWarning
requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from requests.packages.urllib3.exceptions import SNIMissingWarning
requests.packages.urllib3.disable_warnings(SNIMissingWarning)


#############################################################################
# Data Aggregate Functions
#############################################################################

def aggregate_data_SODA( dict_db_connect, source_table, target_table, truncate_table = False, last_update_field = "", last_update_value =  ""):
    """Loads all data since last_update_value (if provided) from source_table 
    at url using the SODA API""" 
    try:
        # Start runtime
        start_time = datetime.datetime.now()
        print("Starting data aggregation for data source (" + source_table + ") at time (" + str(start_time) + ").")
        
        # Connect to database
        conn = psycopg2.connect( database = dict_db_connect["database"], 
                                 user = dict_db_connect["user"],
                                 password = dict_db_connect["password"],
                                 host = dict_db_connect["host"],
                                 port = dict_db_connect["port"])
        cur = conn.cursor()

        cur.execute("SELECT * FROM transform_map WHERE target_table = '" + target_table + "';")

        data = cur.fetchall()
        
        insert_columns = ""
        select_columns = ""
        group_by = ""
        for row in data:
            try:
                insert_columns += str(row[4]) + ","
                if row[5]==1: select_columns += str(row[3]) + " AS " + str(row[4]) + ","
                if row[6]==1: select_columns += "SUM(" + str(row[3]) + ") AS " + str(row[4]) + ","
                if row[7]==1: select_columns += "COUNT(" + str(row[3]) + ") AS " + str(row[4]) + ","
                if row[5]==1: group_by += str(row[3]) + ","
            except Exception as e:
                print "exception encountered:\n" +  str(e)
                return False
        
        select_columns = select_columns[:-1]
        insert_columns = insert_columns[:-1]
        group_by = group_by[:-1]

        sql = "INSERT INTO " + target_table + " ("+ insert_columns + ") SELECT "
        sql += select_columns + " FROM " + source_table
        sql += " GROUP BY " + group_by + ";"

        print sql

#                 #counter += 1
#                 #print(counter, end=" ")
#                 #print("INSERT INTO " + table_name + " (" + columns + ") VALUES (" + values + ");");
#                 cur.execute("INSERT INTO " + table_name + " (" + columns + ") VALUES (" + values + ");");
#                 conn.commit()

#         if truncate_table:
#             # if we don't have last update info, or we aren't doing incremental updates,
#             # truncate the table before insertion
#             cur.execute("TRUNCATE TABLE " + table_name + ";");
#             print("Truncated data table.")

#         # get only the rows since the last data load (if options permit)
#         if not truncate_table and len(last_update_field) > 0 and len(last_update_value) > 0:

# source_table    target_table    source_field    target_field    group_by    sum_of  count_of
# construction_permits    construction_permit_aggregate   TO_CHAR(TO_DATE(issue_date, 'YYYY-MM-DD'),'YYYYMMDD')   date_number 1   0   0
# construction_permits    construction_permit_aggregate   original_zip    zip_code    1   0   0
# construction_permits    construction_permit_aggregate   permit_class_mapped residential_or_commercial   1   0   0
# construction_permits    construction_permit_aggregate   permit_type_desc    permit_type 1   0   0
# construction_permits    construction_permit_aggregate   work_class  work_class  1   0   0
# construction_permits    construction_permit_aggregate   COALESCE(CAST(total_valuation_remodel AS decimal(16,2)) , COALESCE(CAST(total_job_valuation AS decimal(16,2)) , (CAST(building_valuation AS decimal(16,2)) + CAST(building_valuation_remodel AS decimal(16,2)) + CAST(electrical_valuation AS decimal(16,2)) + CAST(electrical_valuation_remodel AS decimal(16,2)) + CAST(mechanical_valuation AS decimal(16,2)) + CAST(mechanical_valuation_remodel AS decimal(16,2)) + CAST(plumbing_valuation AS decimal(16,2)) + CAST(plumbing_valuation_remodel AS decimal(16,2)) + CAST(medgas_valuation AS decimal(16,2)) + CAST(medgas_valuation_remodel AS decimal(16,2))) ))  sum_project_valuation   0   1   0
# construction_permits    construction_permit_aggregate   *   total_num_permits   0   0   1


#         # loop until all data is received
#         offset = 0
#         all_data_loaded = False
#         #counter = 0
#         while not all_data_loaded:
#             requestFull = request + "&$offset=" + str(offset)
#             #print(requestFull)
#             response = requests.get(requestFull, verify=False)
#             data = response.json()
#             if response.status_code != 200:
#                 print("Error: Did not complete call to API (status code: " + str(response.status_code) + ")")
#                 print("Check API call: " + request)
#                 print(data)
#                 return False

#             # Write each row to data table
#             for row in data:
#                 try:
#                     values = ""
#                     columns = ""
#                     for i in row:
#                         columns += '"' + str(i) + '",'
                        
#                         if isinstance(row[i],dict):
#                             values += "'" + str(row[i]).replace("'","").replace("\\","") + "',"
#                         else:
#                             values += "'" + str(row[i].encode('ascii','ignore').replace("'","").replace("\\","")) + "',"
                            
#                     columns = columns[:-1]
#                     values = values[:-1]
#                     #counter += 1
#                     #print(counter, end=" ")
#                     #print("INSERT INTO " + table_name + " (" + columns + ") VALUES (" + values + ");");
#                     cur.execute("INSERT INTO " + table_name + " (" + columns + ") VALUES (" + values + ");");
#                     conn.commit()
            
#             print "Added " + str(len(data)) + " records at time (" + str(start_time) + ")."

#             # determine if there is more data
#             if len(data) == chunk_size:
#                 offset += chunk_size
#             else:
#                 offset += len(data)
#                 all_data_loaded = True
        
#         # Commit changes to table and close connection
#         conn.close()
        
#         print ("Loaded " + str(offset) + " records to data source (" + table_name + ").")
#         print ("Ended data extract for data source (" + table_name + ") at time (" + 
#                 str(datetime.datetime.now()) + ").")
    
#         # if doing incremental updates, save last value inserted, if there is one
#         if len(data) > 0 and not truncate_table and len(last_update_field) > 0:
#             last_update_value = row[last_update_field]
    
    except Exception as inst:
        print(inst.args)
        print(inst)
        return False
    
    # if doing incremental updates, return last value inserted to use next time
    if not truncate_table and len(last_update_field) > 0:
        return str(last_update_value)
    else:
        return True
            

dict_db_connect = { "database" : "finalproject",
                    "user" : "postgres",
                    "password" : "pass",
                    "host" : "localhost",
                    "port" : "5432"}
source_table = "construction_permits"
target_table = "construction_permits_aggregate"

aggregate_data_SODA( dict_db_connect, source_table, target_table, truncate_table = False)
