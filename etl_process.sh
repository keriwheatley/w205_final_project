chmod +x ./etl_files/extract_TablesSetup.sh
./etl_files/extract_TablesSetup.sh

python ./etl_files/extract_CodeComplaintCases.py
python ./etl_files/extract_CommercialWaterConsumption.py
python ./etl_files/extract_IssuedConstructionPermits.py
python ./etl_files/extract_PotholeRepair.py
python ./etl_files/extract_RacialProfilingCitations.py
python ./etl_files/extract_ResidentialWaterConsumption.py
python ./etl_files/extract_RestaurantInspectionScores.py
python ./etl_files/extract_ServiceAlerts.py

#### Need to load 2014 data. This is in CSV format not API.
python ./etl_files/extract_RacialProfilingArrests.py

chmod +x ./etl_files/transform_DataAggregates.sh
./etl_files/transform_DataAggregates.sh

