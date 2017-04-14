chmod +x ./etl_files/extract_TablesSetup.sh
./etl_files/extract_TablesSetup.sh

python ./etl_files/load_CodeComplaintCases.py
python ./etl_files/load_CommercialWaterConsumption.py
python ./etl_files/load_IssuedConstructionPermits.py
python ./etl_files/load_PotholeRepair.py
python ./etl_files/load_RacialProfilingCitations.py
python ./etl_files/load_ResidentialWaterConsumption.py
python ./etl_files/load_RestaurantInspectionScores.py
python ./etl_files/load_ServiceAlerts.py

#### Need to load 2014 data. This is in CSV format not API.
python ./etl_files/load_RacialProfilingArrests.py

chmod +x ./etl_files/transform_DataAggregates.sh
./etl_files/transform_DataAggregates.sh

