import requests
import datetime
import json
import psycopg2

def data_extract():
    try:
        conn = psycopg2.connect(database="finalproject",user="postgres",password="pass",host="localhost",port="5432")
    sql = "INSERT INTO issued_construction_permits VALUES (1,%s);" %(row)
    cur.execute(sql);
    except Exception as inst:
        print(inst.args)
        print(inst)

data_extract()

