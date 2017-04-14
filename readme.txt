HOW TO SETUP ETL

1. Make sure you have Postgres loaded to your AMI.

2. Mount EBS volume
        mount -t ext4 /dev/xvdf /data

3. Start Postgres
        /data/start_postgres.sh

4. Log into user
        su - w205

5. Run application setup script
