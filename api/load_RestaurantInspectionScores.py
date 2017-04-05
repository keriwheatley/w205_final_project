import requests
import datetime
import json
import psycopg2

# This function makes API calls and writes results to data lake tables
def data_extract():
    try:        
        # Start runtime
        start_time = datetime.datetime.now()
        print "Starting data extract for data source (restaurant_inspection_scores) at time (" + str(start_time) + ")."
        
        # Connect to database
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()

        # Empty data tables
        cur.execute("DELETE FROM restaurant_inspection_scores;");
        cur.execute("DELETE FROM restaurant_inspection_scores_counts;");
                
        # Iterate through all zip codes
        for zip in ['78610', '78613', '78617', '78641', '78652', '78653', '78660', '78664', '78681', '78701', '78702', 
            '78703', '78704', '78705', '78712', '78717', '78719', '78721', '78722', '78723', '78724', '78725',
            '78726', '78727', '78728', '78729', '78730', '78731', '78732', '78733', '78734', '78735', '78736',
            '78737', '78738', '78739', '78741', '78742', '78744', '78745', '78746', '78747', '78748', '78749',
            '78750', '78751', '78752', '78753', '78754', '78756', '78757', '78758', '78759']:
        
            # Make API call to data source
            url = "https://data.austintexas.gov/resource/nguv-n54k.json?$limit=50000&zip_code="+zip
            response = requests.get(url, verify=False)
            data = response.json()
            if response.status_code <> 200:
                print "Error: Did not complete call to API. Check API call: " + url
                print data
                break

            # Print row count for single zip code
            num_rows = len(data)
            row_format = "{:>20}" *(6)
            print row_format.format('Zip_Code:', zip, 'Row_Count:',str(num_rows),
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
                cur.execute("INSERT INTO restaurant_inspection_scores (" + columns + ") VALUES (" + values + ");");

            # Record row count for single zip code to counts table
            cur.execute("INSERT INTO restaurant_inspection_scores_counts VALUES('"+zip+"',"+str(num_rows)+");")

            # Commit changes to tables for single zip code
            conn.commit()
            print "Loaded records to data source (restaurant_inspection_scores) for zip code ("+zip+")."

        # Close connection after all single dates have been processed
        conn.close()
        print "Ended data extract for data source (restaurant_inspection_scores) at time (" + str(datetime.datetime.now()) + ")."
    
    # Error logging
    except Exception as inst:
        print(inst.args)
        print(inst)
            
data_extract()
