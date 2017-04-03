import requests
import datetime
import json
import psycopg2

# psql -U postgres -d finalproject -c "CREATE TABLE issued_construction_permits_counts (applied_date TIMESTAMP, row_count INT);"
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

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

def data_extract():
    try:
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()
        current_day = datetime.date.today()
        last_run = cur.execute("SELECT MAX(applied_date) FROM issued_construction_permits_counts;");
        print last_run
        if last_run is None:
            start_date = datetime.date(1990, 1, 1)
            end_date = datetime.date(1990, 5, 1)
#             end_date = date(2017, 5, 1)
            for single_date in daterange(start_date, end_date):
                applied_date=str(single_date.strftime("%Y-%m-%d"))
                url = "https://data.austintexas.gov/resource/x9yh-78fz.json?$limit=50000&applied_date = "+applied_date
                response = requests.get(url, verify=False)
                data = response.json()
                num_rows = len(data)
                print "Date: " + str(single_date.strftime("%Y-%m-%d")) + " Row_Count: " + str(num_rows)

                for row in data:
                    values = ""
                    columns = ""
                    for i in row:
                        columns += str(i) + ","                
                        values += "'" + str(row[i]).replace("'","") + "',"
                    columns = columns[:-1]
                    values = values[:-1]
                    sql = 'INSERT INTO issued_construction_permits (' + columns + ') VALUES (' + values + ');'
                    cur.execute(sql);
                conn.commit()
                cur.execute("INSERT INTO issued_construction_permits_counts VALUES('"+str(num_rows)+","+applied_date+");")

        else:
            url = "https://data.austintexas.gov/resource/x9yh-78fz.json?$limit=50000&statusdate BETWEEN '"+\
            str(last_run)+"' AND '"+str(current_day) +"'"#2011-12-28T10:56:53.000

#         print current_day
#         print last_run        
#         print url
#         response = requests.get(url, verify=False)
#         if response.status_code == 200:
#             data = response.json()
#             num_rows = len(data)

        
#         conn.commit()
        
#         cur.execute("INSERT INTO last_run VALUES('"+table_name+"',"+str(num_rows)+","+str(current_time)+");")
        
#         conn.close()
        
    except Exception as inst:
        print(inst.args)
        print(inst)

data_extract()

