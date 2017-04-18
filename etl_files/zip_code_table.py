import requests
import datetime
import json
import psycopg2
from googlemaps import Client
from eventlet.timeout import Timeout

# CREATE TABLE zip_codes AS (
# SELECT row_number() OVER () AS row_number, 99999 AS zip_code, location 
# FROM (SELECT DISTINCT location AS location FROM racial_profiling_arrests UNION SELECT DISTINCT vl_street_name AS location FROM racial_profiling_citations) loc);

def data_extract():
    try:        
        
        # Start runtime
        start_time = datetime.datetime.now()
        print "Starting zip codes table load at time (" + str(start_time) + ")."
        
        # Connect to database
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()

        sql = "SELECT location, row_number FROM zip_codes WHERE row_number BETWEEN 1 and 2500;"
        
        cur.execute(sql)        
        data = cur.fetchall()
        
        api_key = 'AIzaSyABv-P4wOVxfTFW4_T-654exQXSsnZO0z0'
        c = Client(key=api_key)
        
        for row in data:
            
            print "ROW: " + str(row)

            location = row[0] + ",Austin,TX"
            row_number = row[1]
            print location
            print row_number
            geocode_result = c.geocode(location)
                
            if len(geocode_result)==0:
                zip_code = '99999'
            else:
                for i in xrange(len(geocode_result[0]['address_components'])):
                    if geocode_result[0]['address_components'][i]['types'][0] == 'postal_code':
                        zip_code = geocode_result[0]['address_components'][i]['long_name']
            
            sql = "UPDATE zip_codes SET zip_code = "+zip_code+" WHERE row_number = "+str(row_number)+";"
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
