import requests
import datetime
import json
import psycopg2

def data_extract():
    try:        
        # Start runtime
        start_time = datetime.datetime.now()
        print "Starting data extract for data source (service_alerts) at time (" + str(start_time) + ")."
        
        # Connect to database
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()

        # Empty data table
        cur.execute("TRUNCATE TABLE service_alerts;");
        print "Truncated data table."
                        
        # Make API call to data source
        url = "https://data.texas.gov/resource/cpwm-8vvp.json?$limit=50000"
        response = requests.get(url, verify=False)
        data = response.json()
        if response.status_code <> 200:
            print "Error: Did not complete call to API. Check API call: " + url
            return data

        # Write each row to data table
        for row in data:
            values = ""
            columns = ""
            for i in row:
                columns += str(i) + ","                
                values += "'" + str(row[i]).replace("'","") + "',"
            columns = columns[:-1]
            values = values[:-1]
            cur.execute("INSERT INTO service_alerts (" + columns + ") VALUES (" + values + ");");

        # Commit changes to table and close connection
        conn.commit()
        conn.close()
        print "Loaded " + str(len(data)) + " records to data source (service_alerts)."
        print "Ended data extract for data source (service_alerts) at time (" + str(datetime.datetime.now()) + ")."
    
    # Error logging
    except Exception as inst:
        print(inst.args)
        print(inst)
            
data_extract()
