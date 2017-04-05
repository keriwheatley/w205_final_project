# Issued Construction Permits
# https://data.austintexas.gov/Permitting/Issued-Construction-Permits/3syk-w9eu
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS issued_construction_permits_counts;"
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS issued_construction_permits;"        
psql -U postgres -d finalproject -c "CREATE TABLE issued_construction_permits_counts (zip_code INT,year INT,row_count INT);"
psql -U postgres -d finalproject -c "CREATE TABLE issued_construction_permits (permittype TEXT,permit_type_desc TEXT,
permit_number TEXT,permit_class_mapped TEXT,permit_class TEXT,work_class TEXT,condominium TEXT,permit_location TEXT,
description TEXT,tcad_id TEXT,legal_description TEXT,applieddate TEXT,issue_date TEXT,day_issued TEXT,
calendar_year_issued TEXT,fiscal_year_issued TEXT,issued_in_last_30_days TEXT,issue_method TEXT,status_current TEXT,
statusdate TEXT,expiresdate TEXT,completed_date TEXT,total_existing_bldg_sqft TEXT,remodel_repair_sqft TEXT,
total_new_add_sqft TEXT,total_valuation_remodel TEXT,total_job_valuation TEXT,number_of_floors TEXT,housing_units TEXT,
building_valuation TEXT,building_valuation_remodel TEXT,electrical_valuation TEXT,electrical_valuation_remodel TEXT,
mechanical_valuation TEXT,mechanical_valuation_remodel TEXT,plumbing_valuation TEXT,plumbing_valuation_remodel TEXT,
medgas_valuation TEXT,medgas_valuation_remodel TEXT,original_address1 TEXT,original_city TEXT,original_state TEXT,
original_zip TEXT,council_district TEXT,jurisdiction TEXT,link TEXT,project_id TEXT,masterpermitnum TEXT,
latitude TEXT,longitude TEXT,location TEXT,contractor_trade TEXT,contractor_company_name TEXT,contractor_full_name TEXT,
contractor_phone TEXT,contractor_address1 TEXT,contractor_address2 TEXT,contractor_city TEXT,contractor_zip TEXT,
applicant_full_name TEXT,applicant_org TEXT,applicant_phone TEXT,applicant_address1 TEXT,applicant_address2 TEXT,
applicant_city TEXT,applicantzip TEXT);"

# Restaurant Inspection Scores
# https://data.austintexas.gov/dataset/Restaurant-Inspection-Scores/ecmv-9xxi
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS restaurant_inspection_scores_counts;"
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS restaurant_inspection_scores;"
psql -U postgres -d finalproject -c "CREATE TABLE restaurant_inspection_scores_counts (zip_code INT, row_count INT);"
psql -U postgres -d finalproject -c "CREATE TABLE restaurant_inspection_scores (restaurant_name TEXT,zip_code TEXT,
inspection_date TEXT,score TEXT,address_city TEXT,address_state TEXT,address TEXT,facility_id TEXT,
process_description TEXT,address_address TEXT,address_zip TEXT);" 

# Code Complaint Cases
# https://data.austintexas.gov/Government/Austin-Code-Complaint-Cases/6wtj-zbtb
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS code_complaint_cases_counts;"
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS code_complaint_cases;"
psql -U postgres -d finalproject -c "CREATE TABLE code_complaint_cases_counts (zip_code INT, row_count INT);"
psql -U postgres -d finalproject -c "CREATE TABLE code_complaint_cases (case_id TEXT,address TEXT,house_number TEXT,
street_name TEXT,city TEXT,state TEXT,zip_code TEXT,x TEXT,y TEXT,opened_date TEXT,closed_date TEXT,department TEXT,
case_type TEXT,description TEXT,case_contact TEXT,case_manager TEXT,date_updated TEXT,latitude TEXT,longitude TEXT,
location_city TEXT,location TEXT,location_address TEXT,location_zip TEXT,location_state TEXT);" 

# 2014-2016 Racial Profiling Dataset Citations
# https://data.austintexas.gov/dataset/2014-Racial-Profiling-Dataset-Citations/mw6q-k5gy
# https://data.austintexas.gov/Public-Safety/Racial-Profiling-Dataset-2015-Citations/sc6h-qr9f
# https://data.austintexas.gov/Public-Safety/2016-Racial-Profiling-Dataset-Citations/gcpe-gehi
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS racial_profiling_citations_counts;"
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS racial_profiling_citations;"
psql -U postgres -d finalproject -c "CREATE TABLE racial_profiling_citations_counts (year INT, race VARCHAR(2), row_count INT);"
psql -U postgres -d finalproject -c "CREATE TABLE racial_profiling_citations (case_id TEXT,address TEXT,house_number TEXT,
street_name TEXT,city TEXT,state TEXT,zip_code TEXT,x TEXT,y TEXT,opened_date TEXT,closed_date TEXT,department TEXT,
case_type TEXT,description TEXT,case_contact TEXT,case_manager TEXT,date_updated TEXT,latitude TEXT,longitude TEXT,
location_city TEXT,location TEXT,location_address TEXT,location_zip TEXT,location_state TEXT);" 



citation_number TEXT,off_from_date TEXT,off_time TEXT,race_origin_code TEXT,case_party_sex TEXT,reason_for_stop TEXT,race_known TEXT,
vl_street_name TEXT,msearch_y_n TEXT,msearch_type TEXT,msearch_found TEXT,
