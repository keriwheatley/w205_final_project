import requests
import datetime
import json
import psycopg2

def data_extract():
    try:        
        # Start runtime
        start_time = datetime.datetime.now()
        print "Starting data aggregation into (racial_profiling_citations_aggregate) for data source (racial_profiling_citations) at time (" + str(start_time) + ")."
        
        # Connect to database
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()

        # Empty data table
        cur.execute("TRUNCATE TABLE racial_profiling_citations_aggregate;");
        print "Truncated aggregate table."

        cur.execute("SELECT off_from_date, vl_street_name, case_party_sex, race_origin_code, reason_for_stop, msearch_type, msearch_found FROM racial_profiling_citations")        
#         data = cur.fetchall()
        data = cur.fetchone()
                    
#         print data
        for row in data:
            print row

        columns = cur.description
                    
        for row in columns:
            print row

        result_set = cur.fetchone()
        for row in result_set:
            print row.vl_street_name
    
#         "SELECT 
#             rep_date AS date_number
#             , zip_code AS zip_code
#             , sex AS gender
#             , age_at_offense AS age_at_offense
#             , apd_race_desc AS race
#             , reason_for_stop, AS reason_for_stop
#         FROM 
#             "
        
#         # Write each row to data table
#         for row in data:
#             values = ""
#             columns = ""
#             for i in row:
#                 columns += str(i) + ","                
#                 values += "'" + str(row[i]).replace("'","") + "',"
#             columns = columns[:-1]
#             values = values[:-1]
#             cur.execute("INSERT INTO residential_water_consumption (" + columns + ") VALUES (" + values + ");");

#         # Commit changes to table and close connection
#         conn.commit()
#         conn.close()
#         print "Loaded " + str(len(data)) + " records to data source (residential_water_consumption)."
#         print "Ended data extract for data source (residential_water_consumption) at time (" + str(datetime.datetime.now()) + ")."
    
    # Error logging
    except Exception as inst:
        print(inst.args)
        print(inst)

data_extract() 
