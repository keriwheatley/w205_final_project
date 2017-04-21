HOW TO SETUP ETL

1. Create an EC2 instance using this AMI and attach 100GB EBS volume.
	AMI Name: UCB MIDS W205 Ex2-FULL
	AMI ID: ami-d4dd4ec3
		
2. Configure inbound security rule:
	Type: All TCP
	Source: Anywhere

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
	pip install pandas
	pip install sqlalchemy

3. Start Postgres
	mount -t ext4 <input EBS volume directoy> /data
	/data/start_postgres.sh

4. Log into w205 user
	su - w205

5. Clone repository
	git clone https://github.com/keriwheatley/w205_final_project.git

6. Run tables setup script
	cd w205_final_project
	chmod +x ./setup/table_setup.sh
	./setup/table_setup.sh

7. Run initial load of data
	cd load
	python update_tables.py

8. Setup password for database (to enable connection to Tableau)
	psql -U postgres -d finalproject
	\password
	Enter new password: pass
	
	To test connection, type this into your local command line:
	psql -U postgres -d finalproject -h <public DNS from AWS>
