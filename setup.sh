#!/bin/bash

#
# setup script for w205 final project
#

# load Python packages
#pip install requests
#pip install datetime
#pip install json
#pip install psycopg2
#pip install googlemaps
#pip install pandas
#pip install sqlalchemy


# make log folder
mkdir -p log

# schedule task to run once a day at noon and pass in the install directory (pwd)
#    (to unschedule the task, run "crontab -e" and remove the entry for this program.)
((crontab -l && echo "* * * * * sh $(pwd)/runApp.sh $(pwd)") | sort | uniq) | crontab



# load tables







