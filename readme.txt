HOW TO SETUP ETL

1. Make sure you have Postgres loaded to your AMI.

2. Load these Python packages
		pip install requests
		pip install datetime
		pip install json
		pip install psycopg2
		pip install geopy

3. Load Postgres
		mount -t ext4 /dev/xvdf /data
		/data/start_postgres.sh

4. Log into w205 user
		su - w205

5. Clone repository
		git clone https://github.com/keriwheatley/w205_final_project.git

6. Go to repository and run setup script
		cd w205_final_project
		chmod +x ./etl_process.sh
		./etl_process.sh
