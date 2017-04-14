cd extract

chmod +x ./extract_tables_setup.sh
./extract_load_tables_setup.sh

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

cd ..

