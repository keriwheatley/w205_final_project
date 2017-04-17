import requests
import datetime
import json
import psycopg2
from googlemaps import Client

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

        cur.execute("SELECT COALESCE(vl_street_name,'NONE') AS vl_street_name\
            , TO_CHAR(TO_DATE(off_from_date, 'YYYY-MM-DD'),'YYYYMMDD') AS date_number\
            , COALESCE(case_party_sex,'NONE') AS case_party_sex\
            , COALESCE(race_origin_code,'NONE') AS race_origin_code\
            , COALESCE(reason_for_stop,'NONE') AS reason_for_stop\
            , COALESCE(msearch_type,'NONE') AS msearch_type\
            , COALESCE(msearch_found,'NONE') AS msearch_found\
            FROM racial_profiling_citations LIMIT 5;")        
        data = cur.fetchall()
        
        api_key = 'AIzaSyAEiOrh_qZFJBTzEVRKLKYQ3cYFBAvcScs'
        c = Client(key=api_key)
        
        for row in data:
            
            print "ROW: " + str(row)

            vl_street_name = row[0]
            geocode_result = c.geocode(vl_street_name)
            for i in xrange(len(geocode_result[0]['address_components'])):
                if geocode_result[0]['address_components'][i]['types'][0] == 'postal_code':
                    zip_code = geocode_result[0]['address_components'][i]['long_name']

            date_number = row[1]
            case_party_sex = row[2]
            race_origin_code = row[3]
            reason_for_stop = row[4]
            msearch_type = row[5]
            msearch_found = row[6]
            
            sql = "INSERT INTO racial_profiling_citations_temp (zip_code, date_number, case_party_sex, race_origin_code, reason_for_stop, msearch_type, msearch_found) VALUES ("+zip_code+","+date_number+","+case_party_sex+","+race_origin_code+","+reason_for_stop+","+msearch_type+","+msearch_found+");"
             print sql
            cur.execute(sql)              
    
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
