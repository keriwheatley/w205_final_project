import requests
import datetime
import json
import psycopg2

def data_extract():
    try:        
        # Start runtime
        start_time = datetime.datetime.now()
        print "Starting data extract for data source (residential_water_consumption) at time (" + str(start_time) + ")."
        
        # Connect to database
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()

        # Empty data table
        cur.execute("TRUNCATE TABLE residential_water_consumption;");
        print "Truncated data table."
                        
        # Make API call to data source
        url = "https://data.austintexas.gov/resource/9vdn-n87u.json?$limit=50000"
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
            cur.execute("INSERT INTO residential_water_consumption (" + columns + ") VALUES (" + values + ");");

        # Commit changes to table and close connection
        conn.commit()
        conn.close()
        print "Loaded " + str(len(data)) + " records to data source (residential_water_consumption)."
        print "Ended data extract for data source (residential_water_consumption) at time (" + str(datetime.datetime.now()) + ")."
    
    # Error logging
    except Exception as inst:
        print(inst.args)
        print(inst)
            
data_extract()
