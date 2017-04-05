import load_data_code
import datetime

table_name = "issued_construction_permits"
api_url = "https://data.austintexas.gov/resource/x9yh-78fz.json?$limit=50000&original_zip="
load_data_code.data_extract(table_name,api_url)
