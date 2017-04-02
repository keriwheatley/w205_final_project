import requests
import datetime
import json
import psycopg2

psql -U postgres -d finalproject -c "CREATE TABLE issued_construction_permits (permit_type TEXT,permit_type_desc TEXT,
permit_num TEXT,permit_class_mapped TEXT,permit_class TEXT,work_class TEXT,condominium TEXT,project_name TEXT,
description TEXT,tcad_id TEXT,property_legal_description TEXT,applied_date TEXT,issued_date TEXT,day_issued TEXT,
calendar_year_issued TEXT,fiscal_year_issued TEXT,issued_in_last_30_days TEXT,issuance_method TEXT,status_current TEXT,
status_date TEXT,expires_date TEXT,completed_date TEXT,total_existing_bldg_sqft TEXT,remodel_repair_sqft TEXT,
total_wew_add_sqft TEXT,total_valuation_remodel TEXT,total_job_valuation TEXT,number_of_floors TEXT,housing_units TEXT,
building_valuation TEXT,building_valuation_remodel TEXT,electrical_valuation TEXT,electrical_valuation_remodel TEXT,
mechanical_valuation TEXT,mechanical_valuation_remodel TEXT,plumbing_valuation TEXT,plumbing_valuation_remodel TEXT,
med_gas_valuation TEXT,med_gas_valuation_remodel TEXT,original_address_1 TEXT,original_city TEXT,original_state TEXT,
original_zip TEXT,council_district TEXT,jurisdiction TEXT,link TEXT,project_id TEXT,master_permit_num TEXT,latitude TEXT,
longitude TEXT,location TEXT,contractor_trade TEXT,contractor_company_name TEXT,contractor_full_name TEXT,
contractor_phone TEXT,contractor_address_1 TEXT,contractor_address_2 TEXT,contractor_city TEXT,contractor_zip TEXT,
applicant_full_name TEXT,applicant_organization TEXT,applicant_phone TEXT,applicant_address_1 TEXT,
applicant_address_2 TEXT,applicant_city TEXT,applicant_zip TEXT);"


def data_extract():
    try:
        url = 'https://data.austintexas.gov/resource/x9yh-78fz.json?permittype=EP'
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            data = response.json()
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()

        for row in data:
            values = ""
            columns = ""
            for i in row:
                columns += str(i) + ","                
                values += "'" + str(row[i]).replace("'","") + "',"
            columns = columns[:-1]
            values = values[:-1]
            print columns
            print values
            sql = 'INSERT INTO issued_construction_permits('+columns+') VALUES ('+values+');'
            print sql
            cur.execute(sql);
    except Exception as inst:
        print(inst.args)
        print(inst)

data_extract()

