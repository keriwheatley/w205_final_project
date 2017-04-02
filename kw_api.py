import requests
import datetime
import json
import psycopg2

def create_table():
    url = 'https://data.austintexas.gov/resource/x9yh-78fz.json?permittype=EP'
    response = requests.get(url, verify=False)
    if response.status_code == 200:
        data = response.json()
    for row in data:
        sql = 'CREATE TABLE issued_construction_permits ('
        for i in row:
            sql += i + " text,"
        sql = sql[:-1] + ');'
        break
    print sql

create_table()

def data_extract():
    try:
        url = 'https://data.austintexas.gov/resource/x9yh-78fz.json?permittype=EP'
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            data = response.json()
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
        cur = conn.cursor()

        for row in data:
            sql = 'INSERT INTO issued_construction_permits VALUES ('
            for i in row:
                print i + ' ' + str(row[i])
                sql += "'" + str(row[i]).replace("'","") + "',"
            sql = sql[:-1] + ');'
            print sql
            cur.execute(sql);
    except Exception as inst:
        print(inst.args)
        print(inst)

data_extract()

