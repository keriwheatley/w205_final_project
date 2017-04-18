HOW TO SETUP ETL

1. Create an EC2 instance using this AMI and attach 100GB EBS volume.
		AMI Name: UCB MIDS W205 Ex2-FULL
		AMI ID: ami-d4dd4ec3
		
2. Configure these security groups:
		<<<<<<<<add this>>>>>>>

3. Connect to instance and run setup scripts
		ls /
		chmod a+rwx /data
		wget https://s3.amazonaws.com/ucbdatasciencew205/setup_ucb_complete_plus_postgres.sh
		chmod +x ./setup_ucb_complete_plus_postgres.sh
		./setup_ucb_complete_plus_postgres.sh <input EBS volume directoy>

2. Load these Python packages
		pip install requests
		pip install datetime
		pip install json
		pip install psycopg2
		pip install googlemaps

3. Start Postgres
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
