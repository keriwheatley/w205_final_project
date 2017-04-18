import requests
import datetime
import json
import psycopg2

def data_extract():
    try:        
        # Start runtime
        start_time = datetime.datetime.now()
        print "Starting data extract for data source (racial_profiling_arrests) at time (" + str(start_time) + ")."
        
        # Connect to database
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()

        # Empty data table
        cur.execute("TRUNCATE TABLE racial_profiling_arrests;");
        print "Truncated data table."

        # 2015 data API link
        api_2015 = "https://data.austintexas.gov/resource/dzyc-fhgt.json?$limit=50000"            
        # 2016 data API link
        api_2016 = "https://data.austintexas.gov/resource/s7xq-bupu.json?$limit=50000"
        
        api_links=[api_2015, api_2016]
        years=['2015','2016']

        # Iterate through all years
        for year_index in [0,1]:
            # Make API call to data source
            url = api_links[year_index]
            response = requests.get(url, verify=False)
            data = response.json()
            if response.status_code <> 200:
                print "Error: Did not complete call to API. Check API call: " + url
                print data
                break

            # Print row count for single year
            num_rows = len(data)
            row_format = "{:>20}" *(6)
            print row_format.format('Year:', years[year_index], 'Row_Count:',str(num_rows),
                'Runtime:',str((datetime.datetime.now() - start_time)))

            # Write each row for single year to data table
            for row in data:
                values = ""
                columns = ""
                for i in row:
                    columns += str(i) + ","                
                    values += "'" + str(row[i]).replace("'","") + "',"
                columns = columns[:-1]
                values = values[:-1]
                cur.execute("INSERT INTO racial_profiling_arrests (" + columns + ") VALUES (" + values + ");");

            # Commit changes to tables for single year
            conn.commit()
            print "Loaded records to data source (racial_profiling_arrests) for year ("+years[year_index]+")."

        # Close connection after all single year have been processed
        conn.close()
        print "Ended data extract for data source (racial_profiling_arrests) at time ("\
            + str(datetime.datetime.now()) + ")."
    
    # Error logging
    except Exception as inst:
        print(inst.args)
        print(inst)
            
data_extract()
