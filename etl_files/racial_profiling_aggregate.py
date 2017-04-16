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

        cur.execute("SELECT 
            rep_date
            , location
            , sex
            , age_at_offense
            , apd_race_desc
            , reason_for_stop
        FROM racial_profiling_citations")        
        data = cur.fetchall()
                    
        print data
        for row in data:
            print row
                    
            
        
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
