import requests
import datetime
import json
import psycopg2

def data_extract():
    try:        
        # Start runtime
        start_time = datetime.datetime.now()
        print "Starting data extract for data source (racial_profiling_citations) at time (" + str(start_time) + ")."
        
        # Connect to database
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()

        # Empty data tables
        cur.execute("TRUNCATE TABLE racial_profiling_citations;");
        cur.execute("TRUNCATE TABLE racial_profiling_citations_counts;");

        # 2014 data API link
        api_2014 = "https://data.austintexas.gov/resource/rkqq-yay6.json?$limit=50000&race_origin_code="
        # 2015 data API link
        api_2015 = "https://data.austintexas.gov/resource/5tcj-brxc.json?$limit=50000&race_origin_code="            
        # 2016 data API link
        api_2016 = "https://data.austintexas.gov/resource/gmmi-p5zw.json?$limit=50000&race_origin_code="
        
        api_links=[api_2014, api_2015, api_2016]
        years=['2014','2015','2016']

        # Iterate through all years and races
        for year_index in xrange(2):
            for race in ['A','B','H','ME','N','O','W']:
                # Make API call to data source
                url = api_links[year_index]+race
                response = requests.get(url, verify=False)
                data = response.json()
                if response.status_code <> 200:
                    print "Error: Did not complete call to API. Check API call: " + url
                    print data
                    break

                # Print row count for single zip code
                num_rows = len(data)
                row_format = "{:>20}" *(8)
                print row_format.format('Year:', years[year_index], 'Race:', race, 'Row_Count:',str(num_rows),
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
                    cur.execute("INSERT INTO racial_profiling_citations (" + columns + ") VALUES (" + values + ");");

                # Record row count for single zip code to counts table
                cur.execute("INSERT INTO racial_profiling_citations_counts \
                    VALUES("+years[year_index]+",'"+race+"',"+str(num_rows)+");")

                # Commit changes to tables for single zip code
                conn.commit()
                print "Loaded records to data source (racial_profiling_citations) \
                    for year ("+years[year_index]+") and race (" +race+"."

        # Close connection after all single dates have been processed
        conn.close()
        print "Ended data extract for data source (racial_profiling_citations) at time ("\
            + str(datetime.datetime.now()) + ")."
    
    # Error logging
    except Exception as inst:
        print(inst.args)
        print(inst)
            
data_extract()
