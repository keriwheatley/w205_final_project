chmod +x ./etl_files/extract_TablesSetup.sh
./etl_files/extract_TablesSetup.sh

python etl_files/load_CodeComplaintCases.py
python etl_files/load_CommercialWaterConsumption.py
python load_IssuedConstructionPermits.py
python load_PotholeRepair.py
python load_RacialProfilingCitations.py
python load_ResidentialWaterConsumption.py
python load_RestaurantInspectionScores.py
python load_ServiceAlerts.py

#### Need to load 2014 data. This is in CSV format not API.
python load_RacialProfilingArrests.py

chmod +x ./transform_DataAggregates.sh
./transform_DataAggregates.sh

