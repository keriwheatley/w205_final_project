import load_data_code
import datetime

table_name = "issued_construction_permits"
initial_start_date = datetime.date(1990, 1, 1) # Start date for initial load; initial load runtime ~30 minutes
end_date = datetime.date.today()-datetime.timedelta(days=1) # End time is yesterday
date_format = "strftime('%Y-%m-%d')" # Format date for API call
api_url = "https://data.austintexas.gov/resource/x9yh-78fz.json?$limit=50000&applieddate="
load_data_code.data_extract(table_name,initial_start_date,end_date,date_format,api_url)
