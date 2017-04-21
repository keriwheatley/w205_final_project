#Zillow absolute home value
#http://files.zillowstatic.com/research/public/Zip/Zip_MedianValuePerSqft_AllHomes.csv
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS \"Zip_MedianValuePerSqft_AllHomes\";"
psql -U postgres -d finalproject -c "CREATE TABLE \"Zip_MedianValuePerSqft_AllHomes\" (index TEXT, city TEXT, date TEXT, metro TEXT, state TEXT, value INT, zip_code INT);"

#Zillow home value per square foot
#http://files.zillowstatic.com/research/public/Zip/Zip_Zhvi_AllHomes.csv
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS \"Zip_Zhvi_AllHomes\";"
psql -U postgres -d finalproject -c "CREATE TABLE \"Zip_Zhvi_AllHomes\" (index TEXT, city TEXT, date TEXT, metro TEXT, state TEXT, value INT, zip_code INT);"

#Zillow absolute rent
#http://files.zillowstatic.com/research/public/Zip/Zip_Zri_AllHomesPlusMultifamily.csv
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS \"Zip_Zri_AllHomesPlusMultifamily\";"
psql -U postgres -d finalproject -c "CREATE TABLE \"Zip_Zri_AllHomesPlusMultifamily\" (index TEXT, city TEXT, date TEXT, metro TEXT, state TEXT, value INT, zip_code INT);"

#Zillow rent per sq foot
#http://files.zillowstatic.com/research/public/Zip/Zip_ZriPerSqft_AllHomes.csv
psql -U postgres -d finalproject -c "DROP TABLE IF EXISTS \"Zip_ZriPerSqft_AllHomes\";"
psql -U postgres -d finalproject -c "CREATE TABLE \"Zip_ZriPerSqft_AllHomes\" (index TEXT, city TEXT, date TEXT, metro TEXT, state TEXT, value INT, zip_code INT);"
