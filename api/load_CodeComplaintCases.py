import data_extract
import datetime

table_name = "code_complaint_cases"
initial_start_date = datetime.date(1990, 1, 1) # Start date for initial load
end_date = datetime.date.today()-datetime.timedelta(days=1) # End time is yesterday
date_format = "strftime('%Y-%m-%d')" # Format date for API call
api_url = "https://data.austintexas.gov/resource/cgku-nb4s.json?$limit=50000&opened_date="
data_extract(table_name,initial_start_date,end_date,date_format,api_url)
