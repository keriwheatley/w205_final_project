# Issued Construction Permits
# https://data.austintexas.gov/Permitting/Issued-Construction-Permits/3syk-w9eu
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS construction_permit_aggregate;"
psql -U postgres -d finalproject -c "CREATE TABLE construction_permit_aggregate AS(
  SELECT 
    TO_CHAR(TO_DATE(issue_date, 'YYYY-MM-DD'),'YYYYMMDD') AS date_number
    , original_zip AS zip_code
    , permit_class_mapped AS residential_or_commercial
    , permit_type_desc AS permit_type
    , work_class AS work_class
    , SUM(COALESCE(CAST(total_valuation_remodel AS decimal(16,2))
        , COALESCE(CAST(total_job_valuation AS decimal(16,2))
        ,  (CAST(building_valuation AS decimal(16,2)) + 
            CAST(building_valuation_remodel AS decimal(16,2)) + 
            CAST(electrical_valuation AS decimal(16,2)) + 
            CAST(electrical_valuation_remodel AS decimal(16,2)) +
            CAST(mechanical_valuation AS decimal(16,2)) +
            CAST(mechanical_valuation_remodel AS decimal(16,2)) +
            CAST(plumbing_valuation AS decimal(16,2)) +
            CAST(plumbing_valuation_remodel AS decimal(16,2)) +
            CAST(medgas_valuation AS decimal(16,2)) +
            CAST(medgas_valuation_remodel AS decimal(16,2)))
        ))) AS sum_project_valuation
    , COUNT(*) AS total_num_permits
  FROM construction_permits
  GROUP BY
    TO_CHAR(TO_DATE(issue_date, 'YYYY-MM-DD'),'YYYYMMDD')
    , original_zip
    , permit_class_mapped
    , permit_type_desc
    , work_class
  );"


# Restaurant Inspection Scores
# https://data.austintexas.gov/dataset/Restaurant-Inspection-Scores/ecmv-9xxi
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS restaurant_inspection_aggregate;"
psql -U postgres -d finalproject -c "CREATE TABLE restaurant_inspection_aggregate AS (
  SELECT
    TO_CHAR(TO_DATE(inspection_date, 'YYYY-MM-DD'),'YYYYMMDD') AS date_number
    , zip_code AS zip_code
    , SUM(CAST(score AS decimal(16,2))) AS sum_score
    , COUNT(*) AS total_num_scores
    , MIN(CAST(score AS decimal(16,2))) AS min_score
    , MAX(CAST(score AS decimal(16,2))) AS max_score
  FROM restaurant_inspection_scores
  GROUP BY
    TO_CHAR(TO_DATE(inspection_date, 'YYYY-MM-DD'),'YYYYMMDD')
    , zip_code
  );"

# Code Complaint Cases
# https://data.austintexas.gov/Government/Austin-Code-Complaint-Cases/6wtj-zbtb
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS code_complaint_cases_aggregate;"
psql -U postgres -d finalproject -c "CREATE TABLE code_complaint_cases_aggregate AS (
  SELECT
    TO_CHAR(TO_DATE(opened_date, 'YYYY-MM-DD'),'YYYYMMDD') AS date_number
    , zip_code AS zip_code
    , case_type AS complaint_type
    , COUNT(*) AS total_num_cases
  FROM code_complaint_cases
  GROUP BY 
    TO_CHAR(TO_DATE(opened_date, 'YYYY-MM-DD'),'YYYYMMDD')
    , zip_code
    , case_type
  );"

# Residential Water Consumption
# Commercial Water Consumption
# https://data.austintexas.gov/Utility/Austin-Water-Residential-Water-Consumption/sxk7-7k6z
# https://data.austintexas.gov/Utility/Austin-Water-Commercial-Water-Consumption/5h9c-wmds
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS water_consumption_aggregate;"
psql -U postgres -d finalproject -c "CREATE TABLE water_consumption_aggregate AS (
  SELECT
    year_month AS month_number
    , postal_code AS zip_code
    , 'Residential' AS residential_or_commercial
    , customer_class AS customer_class
    , total_gallons AS total_gallons
  FROM residential_water_consumption
  UNION ALL
  SELECT
    year_month AS month_number
    , postal_code AS zip_code
    , 'Commercial' AS residential_or_commercial
    , customer_class AS customer_class
    , total_gallons AS total_gallons
  FROM commercial_water_consumption
  );"


# Pothole Repair
# https://data.austintexas.gov/Government/Pothole-Repair/fmm2-ytyt
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS pothole_repair_aggregate;"
psql -U postgres -d finalproject -c "CREATE TABLE pothole_repair_aggregate AS (
  SELECT
    TO_CHAR(TO_DATE(sr_created_date, 'YYYY-MM-DD'),'YYYYMMDD') AS created_date_number
    , sr_location_zip_code AS zip_code
    , TO_CHAR(TO_DATE(sr_updated_date, 'YYYY-MM-DD'),'YYYYMMDD') AS status_change_date_number
    , sr_status_desc AS status
    , COUNT(*) AS total_num_cases
  FROM pothole_repair
  GROUP BY
    TO_CHAR(TO_DATE(sr_created_date, 'YYYY-MM-DD'),'YYYYMMDD')
    , sr_location_zip_code
    , TO_CHAR(TO_DATE(sr_updated_date, 'YYYY-MM-DD'),'YYYYMMDD')
    , sr_status_desc
  );"
