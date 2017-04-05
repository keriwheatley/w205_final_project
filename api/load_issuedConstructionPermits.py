import requests
import datetime
import json
import psycopg2


# This function makes API calls and writes results to data lake tables
def data_extract():
    try:        
        # Start runtime
        start_time = datetime.datetime.now()
        print "Starting data extract for data source (issued_construction_permits) at time (" + str(start_time) + ")."
        
        # Connect to database
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()
        
        print int(datetime.datetime.now().year())
        
        # Iterate through all zip codes and years
        for year in range(1990,2017):
            for zip in ['78610', '78613', '78617', '78641', '78652', '78653', '78660', '78664', '78681', '78701', '78702', 
                    '78703', '78704', '78705', '78712', '78717', '78719', '78721', '78722', '78723', '78724', '78725',
                    '78726', '78727', '78728', '78729', '78730', '78731', '78732', '78733', '78734', '78735', '78736',
                    '78737', '78738', '78739', '78741', '78742', '78744', '78745', '78746', '78747', '78748', '78749',
                    '78750', '78751', '78752', '78753', '78754', '78756', '78757', '78758', '78759']:
            
                # Make API call to data source
                url = "https://data.austintexas.gov/resource/x9yh-78fz.json?$limit=50000&original_zip="+zip+"&calendar_year_issued="+str(year)
#                 response = requests.get(url, verify=False)
#                 data = response.json()
#                 if response.status_code <> 200:
#                     print "Error: Did not complete call to API. Check API call: " + url
#                     print data
#                     break

#                 # Print row count for single date
#                 num_rows = len(data)
#                 row_format = "{:>20}" *(8)
#                 print row_format.format('Zip_Code:', zip,'Year:', year, 'Row_Count:',str(num_rows),
#                     'Runtime:',str((datetime.datetime.now() - start_time)))

#                 # Write each row for single date to data lake table
#                 for row in data:
#                     values = ""
#                     columns = ""
#                     for i in row:
#                         columns += str(i) + ","                
#                         values += "'" + str(row[i]).replace("'","") + "',"
#                     columns = columns[:-1]
#                     values = values[:-1]
#                     cur.execute("INSERT INTO issued_construction_permits (" + columns + ") VALUES (" + values + ");");

#             # Record row count for single date to counts table
#             cur.execute("INSERT INTO issued_construction_permits_counts VALUES('"+zip+"',"+str(num_rows)+");")

#             # Commit changes to tables for single date
#             conn.commit()
#             print "Loaded records to data source (issued_construction_permits) for zip code ("+zip+")."

#         # Close connection after all single dates have been processed
#         conn.close()
#         print "Ended data extract for data source (issued_construction_permits) at time (" + str(datetime.datetime.now()) + ")."
    
    # Error logging
    except Exception as inst:
        print(inst.args)
        print(inst)
            
data_extract()
