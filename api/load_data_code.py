import requests
import datetime
import json
import psycopg2

# Get date range for inputs incremented by day
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

# This function makes API calls and writes results to data lake tables
def data_extract(data_source, initial_start_date, end_date, date_format, api_url):
    try:        
        # Start runtime
        start_time = datetime.datetime.now()
        print "Starting data extract for data source (" + data_source + ") at time (" + str(start_time) + ")."
        
        # Connect to database
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()
        
        # Find last run date for data source. If no run date exists, use input initial_start_date.
        cur.execute("SELECT MAX(match_key) FROM "+data_source+"_counts;");
        last_run = cur.fetchall()[0][0]
        if last_run is None: start_date = initial_start_date
        else: start_date = last_run+datetime.timedelta(days=1)
                
        # Iterate through all days from last run date to end date
        for day in daterange(start_date, end_date):
            
            # Reformat single date
            single_date=eval("str(day."+date_format+")")
            
            # Make API call to data source
            url = api_url+single_date
            response = requests.get(url, verify=False)
            data = response.json()
            if response.status_code <> 200:
                print "Error: Did not complete call to API. Check API call: " + url
                print data
                break

            # Print row count for single date
            num_rows = len(data)
            row_format = "{:>20}" *(6)
            print row_format.format('Date:', single_date,'Row_Count:',str(num_rows),
                'Runtime:',str((datetime.datetime.now() - start_time)))
            
            # Write each row for single date to data lake table
            for row in data:
                values = ""
                columns = ""
                for i in row:
                    columns += str(i) + ","                
                    values += "'" + str(row[i]).replace("'","") + "',"
                columns = columns[:-1]
                values = values[:-1]
                cur.execute("INSERT INTO " + data_source + " (" + columns + ") VALUES (" + values + ");");
            
            # Record row count for single date to counts table
            cur.execute("INSERT INTO " + data_source + "_counts VALUES('"+single_date+"',"+str(num_rows)+");")

            # Commit changes to tables for single date
            conn.commit()
            print "Loaded records to data source ("+data_source+") for day ("+single_date+")."

        # Close connection after all single dates have been processed
        conn.close()
        print "Ended data extract for data source (" + data_source + ") at time (" + str(datetime.datetime.now()) + ")."
    
    # Error logging
    except Exception as inst:
        print(inst.args)
        print(inst)
