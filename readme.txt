HOW TO SETUP ETL

1. Make sure you have Postgres loaded to your AMI.

2. Mount EBS volume
        mount -t ext4 /dev/xvdf /data

3. Start Postgres
        /data/start_postgres.sh

4. Log into user
        su - w205

5. Run application setup script

chmod +x ./load_tables_setup.sh
./load_tables_setup.sh

python load_CodeComplaintCases.py
python load_CommercialWaterConsumption.py
python load_IssuedConstructionPermits.py
python load_PotholeRepair.py
python load_RacialProfilingCitations.py
python load_ResidentialWaterConsumption.py
python load_RestaurantInspectionScores.py
python load_ServiceAlerts.py

#### Need to load 2014 data. This is in CSV format not API.
python load_RacialProfilingArrests.py
