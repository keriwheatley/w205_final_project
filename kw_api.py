import requests
import datetime
import json
import psycopg2

# psql -U postgres -d finalproject -c "CREATE TABLE last_run (table_name TEXT, row_count INT, run_date TIMESTAMP);"
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

def data_extract():
    try:
        table_name = 'issued_construction_permits'
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()
        current_day = datetime.date.today()
        last_run = cur.execute("SELECT MAX(run_date) FROM last_run WHERE table_name = '"+table_name+"';");
        last_run = current_day if last_run is None else last_run

        print current_day
        print last_run
        
        url = 'https://data.austintexas.gov/resource/x9yh-78fz.json?$statusdate>='+\
            str(last_run)+'andstatusdate<'+str(current_day) #2011-12-28T10:56:53.000
        print url
#         response = requests.get(url, verify=False)
#         if response.status_code == 200:
#             data = response.json()
#             num_rows = len(data)
            
#         for row in data:
#             values = ""
#             columns = ""
#             for i in row:
#                 columns += str(i) + ","                
#                 values += "'" + str(row[i]).replace("'","") + "',"
#             columns = columns[:-1]
#             values = values[:-1]
#             sql = 'INSERT INTO '+table_name+' (' + columns + ') VALUES (' + values + ');'
#             cur.execute(sql);
#             print "Loaded row "+ str(num_rows)
        
#         conn.commit()
        
#         cur.execute("INSERT INTO last_run VALUES('"+table_name+"',"+str(num_rows)+","+str(current_time)+");")
        
#         conn.close()
        
    except Exception as inst:
        print(inst.args)
        print(inst)

data_extract()

