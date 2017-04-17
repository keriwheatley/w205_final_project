import requests
import datetime
import json
import psycopg2
from googlemaps import Client
from eventlet.timeout import Timeout

def data_extract():
    try:        
        
        # Start runtime
        start_time = datetime.datetime.now()
        print "Starting data transformation into (racial_profiling_citations_transformed) for data source (racial_profiling_citations) at time (" + str(start_time) + ")."
        
        # Connect to database
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()

        sql = "SELECT COALESCE(vl_street_name,'NONE') AS vl_street_name"
        sql += " , TO_CHAR(TO_DATE(off_from_date, 'YYYY-MM-DD'),'YYYYMMDD') AS date_number"
        sql += " , COALESCE(case_party_sex,'NONE') AS case_party_sex"
        sql += " , COALESCE(race_origin_code,'NONE') AS race_origin_code"
        sql += " , COALESCE(reason_for_stop,'NONE') AS reason_for_stop"
        sql += " , COALESCE(msearch_type,'NONE') AS msearch_type"
        sql += " , COALESCE(msearch_found,'NONE') AS msearch_found"
        sql += " FROM racial_profiling_citations;"
        
        cur.execute(sql)        
        data = cur.fetchall()
        
        api_key = 'AIzaSyAEiOrh_qZFJBTzEVRKLKYQ3cYFBAvcScs'
        c = Client(key=api_key)
        
        for row in data:
            
            print "ROW: " + str(row)

            vl_street_name = row[0] + ",Austin,TX"
            geocode_result = c.geocode(vl_street_name)
                
            if len(geocode_result)==0:
                zip_code = '99999'
            else:
                for i in xrange(len(geocode_result[0]['address_components'])):
                    if geocode_result[0]['address_components'][i]['types'][0] == 'postal_code':
                        zip_code = geocode_result[0]['address_components'][i]['long_name']

            date_number = row[1]
            case_party_sex = row[2]
            race_origin_code = row[3]
            reason_for_stop = row[4]
            msearch_type = row[5]
            msearch_found = row[6]
            
            sql = "INSERT INTO racial_profiling_citations_transformed"
            sql += " (zip_code, date_number, case_party_sex,race_origin_code, reason_for_stop, msearch_type, msearch_found)"
            sql += " VALUES ("+zip_code+","+date_number+",'"+case_party_sex+"','"+race_origin_code+"','"
            sql += reason_for_stop+"','"+msearch_type+"','"+msearch_found+"');"
            print sql
            cur.execute(sql)              
    
        # Commit changes to table and close connection
        conn.commit()
        conn.close()
        print "Loaded " + str(len(data)) + " records to data source (racial_profiling_citations_transformed)."
        print "Ended data extract for data source (racial_profiling_citations_transformed) at time (" + str(datetime.datetime.now()) + ")."
    
    # Error logging
    except Exception as inst:
        print(inst.args)
        print(inst)

data_extract() 
