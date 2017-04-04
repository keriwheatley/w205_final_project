import requests
import datetime
import json
import psycopg2

# Get date range for inputs incremented by day
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

# This function makes API calls and writes results to data lake tables
def data_extract(data_source, initial_start_date, date_format, api_url):
    try:        
        # Start runtime
        start_time = datetime.datetime.now()
        print "Starting data extract for data source (" + data_source + ") at (" + str(start_time) + ")."
        
        # Connect to database
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()
        
        # Find last run date for data source. If no run date exists, use 01-01-1990.
        cur.execute("SELECT MAX(match_key) FROM "+data_source+"_counts;");
        last_run = cur.fetchall()[0][0]
        if last_run is None: start_date = initial_start_date
        else: start_date = last_run
        
        # Iterate through all days from last run date to current date - 1 day
        for day in daterange(start_date+datetime.timedelta(days=1), (datetime.date.today()-datetime.timedelta(days=1))):
            
            # Reformat single date
            single_date=str(day.strftime(date_format))
            
            # Make API call to data source
            url = api_url+single_date
            response = requests.get(url, verify=False)
            data = response.json()
            if response.status_code <> 200:
                print "Error: Did not complete call to API. Check url call: "
                print url
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
            print "Loaded " + single_date + " records."

        # Close connection after all single dates have been processed
        conn.close()
        print "Ended data extract for data source (" + data_source + ") at (" + str(datetime.datetime.now()) + ")."
    
    # Error logging
    except Exception as inst:
        print(inst.args)
        print(inst)

# psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS issued_construction_permits_counts;"
# psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS issued_construction_permits;"        
# psql -U postgres -d finalproject -c "CREATE TABLE issued_construction_permits_counts (match_key DATE, row_count INT);"
# psql -U postgres -d finalproject -c "CREATE TABLE issued_construction_permits (permittype TEXT,permit_type_desc TEXT,
# permit_number TEXT,permit_class_mapped TEXT,permit_class TEXT,work_class TEXT,condominium TEXT,permit_location TEXT,
# description TEXT,tcad_id TEXT,legal_description TEXT,applieddate TEXT,issue_date TEXT,day_issued TEXT,
# calendar_year_issued TEXT,fiscal_year_issued TEXT,issued_in_last_30_days TEXT,issue_method TEXT,status_current TEXT,
# statusdate TEXT,expiresdate TEXT,completed_date TEXT,total_existing_bldg_sqft TEXT,remodel_repair_sqft TEXT,
# total_new_add_sqft TEXT,total_valuation_remodel TEXT,total_job_valuation TEXT,number_of_floors TEXT,housing_units TEXT,
# building_valuation TEXT,building_valuation_remodel TEXT,electrical_valuation TEXT,electrical_valuation_remodel TEXT,
# mechanical_valuation TEXT,mechanical_valuation_remodel TEXT,plumbing_valuation TEXT,plumbing_valuation_remodel TEXT,
# medgas_valuation TEXT,medgas_valuation_remodel TEXT,original_address1 TEXT,original_city TEXT,original_state TEXT,
# original_zip TEXT,council_district TEXT,jurisdiction TEXT,link TEXT,project_id TEXT,masterpermitnum TEXT,
# latitude TEXT,longitude TEXT,location TEXT,contractor_trade TEXT,contractor_company_name TEXT,contractor_full_name TEXT,
# contractor_phone TEXT,contractor_address1 TEXT,contractor_address2 TEXT,contractor_city TEXT,contractor_zip TEXT,
# applicant_full_name TEXT,applicant_org TEXT,applicant_phone TEXT,applicant_address1 TEXT,applicant_address2 TEXT,
# applicant_city TEXT,applicantzip TEXT);"
table_name = "issued_construction_permits"
initial_start_date = datetime.date(1990, 1, 1)
date_format = "%Y-%m-%d"
api_url = "https://data.austintexas.gov/resource/x9yh-78fz.json?$limit=50000&applieddate="
data_extract(table_name,initial_start_date,date_format,api_url) #Initial runtime ~30 minutes

# psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS restaurant_inspection_scores_counts;"
# psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS restaurant_inspection_scores;"
# psql -U postgres -d finalproject -c "CREATE TABLE restaurant_inspection_scores_counts (match_key DATE, row_count INT);"
# psql -U postgres -d finalproject -c "CREATE TABLE restaurant_inspection_scores (restaurant_name TEXT,
# zip_code TEXT, inspection_date TEXT, score TEXT, address TEXT, facility_id TEXT, process_description TEXT);"
table_name = "restaurant_inspection_scores"
initial_start_date = datetime.date(2014, 3, 1)
date_format = "%m/%d/%Y"
api_url = "https://data.austintexas.gov/resource/nguv-n54k.json?$limit=50000&inspection_date="
data_extract(table_name,initial_start_date,date_format,api_url)

# table_name = "issued_construction_permits"
# initial_start_date = datetime.date(1990, 1, 1)
# api_url = "https://data.austintexas.gov/resource/x9yh-78fz.json?$limit=50000&applieddate="
# data_extract(table_name,initial_start_date,date_format,api_url)

