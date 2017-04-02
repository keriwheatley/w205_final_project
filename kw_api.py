import requests
import datetime
import json
import psycopg2

# psql -U postgres -d finalproject -c "CREATE TABLE issued_construction_permits (PermitType TEXT,PermitTypeDesc TEXT,
# PermitNum TEXT,PermitClassMapped TEXT,PermitClass TEXT,WorkClass TEXT,Condominium TEXT,ProjectName TEXT,
# Description TEXT,TCAD_ID TEXT,PropertyLegalDescription TEXT,AppliedDate TEXT,IssuedDate TEXT,DayIssued TEXT,
# CalendarYearIssued TEXT,FiscalYearIssued TEXT,IssuedInLast30Days TEXT,IssuanceMethod TEXT,StatusCurrent TEXT,
# StatusDate TEXT,ExpiresDate TEXT,CompletedDate TEXT,TotalExistingBldgSQFT TEXT,RemodelRepairSQFT TEXT,TotalNewAddSQFT TEXT,
# TotalValuationRemodel TEXT,TotalJobValuation TEXT,NumberOfFloors TEXT,HousingUnits TEXT,BuildingValuation TEXT,
# BuildingValuationRemodel TEXT,ElectricalValuation TEXT,ElectricalValuationRemodel TEXT,MechanicalValuation TEXT,
# MechanicalValuationRemodel TEXT,PlumbingValuation TEXT,PlumbingValuationRemodel TEXT,MedGasValuation TEXT,
# MedGasValuationRemodel TEXT,OriginalAddress1 TEXT,OriginalCity TEXT,OriginalState TEXT,OriginalZip TEXT,CouncilDistrict TEXT,
# Jurisdiction TEXT,Link TEXT,ProjectID TEXT,MasterPermitNum TEXT,Latitude TEXT,Longitude TEXT,Location TEXT,
# ContractorTrade TEXT,ContractorCompanyName TEXT,ContractorFullName TEXT,ContractorPhone TEXT,ContractorAddress1 TEXT,
# ContractorAddress2 TEXT,ContractorCity TEXT,ContractorZip TEXT,ApplicantFullName TEXT,ApplicantOrganization TEXT,
# ApplicantPhone TEXT,ApplicantAddress1 TEXT,ApplicantAddress2 TEXT,ApplicantCity TEXT,ApplicantZip TEXT);"


def data_extract():
    try:
        url = 'https://data.austintexas.gov/resource/x9yh-78fz.json?permittype=EP'
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            data = response.json()
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()

        for row in data:
            values = ""
            columns = ""
            for i in row:
                columns += str(i) + ","                
                values += "'" + str(row[i]).replace("'","") + "',"
            columns = columns[:-1]
            values = values[:-1]
            print columns
            print values
            sql = 'INSERT INTO issued_construction_permits('+columns+') VALUES ('+values+');'
            print sql
            cur.execute(sql);
    except Exception as inst:
        print(inst.args)
        print(inst)

data_extract()

