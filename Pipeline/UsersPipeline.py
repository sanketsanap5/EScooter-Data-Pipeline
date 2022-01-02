import datetime as dt

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd
from pandas import json_normalize
import psycopg2 as db
import os,sys

from faker import Faker

from elasticsearch import Elasticsearch
from elasticsearch import helpers

deafult_args = {
    'owner' : 'sanket',
    'start_date' : dt.datetime(2022,1,1),
    'retries' : 1,
    'retry_delay' : dt.timedelta(minutes=2)
}

with DAG(
    'MyDBDAG',
    default_args = deafult_args,
    schedule_interval = '@once'
    # '0****',
) as dag:

    baseDir = '/Users/Sanket/Documents/GitHub/Real-Time-Data-Pipeline'
    userDataDir = baseDir + '/output/userData/'
    stageJSON = userDataDir + 'stageJSON.json'
    userDF = pd.DataFrame()
    conn_string = "dbname='test' host='localhost' user='postgres' password='postgres'"
    conn = db.connect(conn_string)

    def createDataLake():
        if os.path.exists(userDataDir):
            fileNames = [x for x in os.listdir(userDataDir) if x.endswith('.json')]
            for name in fileNames:
                if os.path.exists(userDataDir+name):
                    os.remove(userDataDir+name)
        else:
            os.mkdir(userDataDir)
        
        fake = Faker()
        for i in range(1,10001):
            data = {
                'name':fake.name(),
                'id':i,
                'street':fake.street_address(),
                'city':fake.city(),
                'zip':fake.zipcode()
            }
            df = pd.DataFrame([data])
            df.to_json(userDataDir+str(data['id'])+'.json')

    def readDataLake():
        if not os.path.exists(userDataDir):
            print('------------------------!!! Directory Does Not Exists For Data !!!------------------------')
            sys.exit(1)

        fileNames = [x for x in os.listdir(userDataDir) if x.endswith('.json')]
        for name in fileNames:
            global userDF
            df = pd.read_json(userDataDir+name)
            userDF = userDF.append(other=df,ignore_index=True)

        print('Found {} Records In Data Lake '.format(userDF.id.count()))
        if os.path.exists(stageJSON):
            os.remove(stageJSON)
        
        userDF.to_json(stageJSON)
        print('-------------------- Read Completed From Data Lake ---------------------')


    def insertIntoStatging():
        if not os.path.exists(stageJSON):
            print('----------------------!!! No Stage File Found !!!--------------------')
            sys.exit(1)
        userDF = pd.read_json(stageJSON)
        cur = conn.cursor()
        print('Inserting {} Records In Staging DB'.format(userDF.id.count()))
        try:
            query = "insert into users (name,id,street,city,zip) values(%s,%s,%s,%s,%s)"
            cur.executemany(query,userDF.values)
            conn.commit()
        except:
            conn.rollback()
            if not pd.read_sql('select count(*) from users;',conn).values == 0:
                print('-----------------------!!! Staging DB Has Stail Data !!!----------------------')
                cur.execute('truncate table users;')
                conn.commit()
            
            print('--------------------------- Retrying Insert Into Staging DB --------------------------')
            query = "insert into users (name,id,street,city,zip) values(%s,%s,%s,%s,%s)"
            cur.executemany(query,userDF.values)
            conn.commit()
        if pd.read_sql('select count(*) from users;',conn).values > 0 :
            print('-------------- Data Staging Completed ---------------')
        else:
            print('--------------!!! Data Staging Failed !!!---------------')
            sys.exit(1)


    def validateStaging():
        if not pd.read_sql('select count(*) from users;',conn).values > 0:
            print('------------------------!!! No Data Found In Staging Db !!!--------------------')
            sys.exit(1)
        print('------------------------- Data Validated At Staging -------------------------')


    def insertElasticSearch():
        es = Elasticsearch()
        df = pd.read_sql('select * from users;',conn)
        dataSource=[
            {
                "_index":"users",
                "_source":df.loc[i].to_dict()
            }
            for i in range(df['id'].count())
        ]
        result = helpers.bulk(es,dataSource)
        if result[0] !=0:
            print('-------------- Data Loaded --------------')
        else:
            print('-------------------!!! Data Load Failed !!!--------------------')
            sys.exit(1)


    def cleanDirectories():
        doc = {
            "query":{
                "match_all":{}
            }
        }
        es = Elasticsearch()
        res = es.search(index="users",body=doc,size=1)
        df = json_normalize(res['hits']['hits'])
    
        if df['_source.id'].value_counts().values==1:
            fileNames = [x for x in os.listdir(userDataDir) if x.endswith('.json')]
            for name in fileNames:
                if os.path.exists(userDataDir+name):
                    os.remove(userDataDir+name)
            print('--------------------- User Data Directory Cleaned ----------------------')
        else:
            print('-----------------------!!! Could Not Clean Data Directories !!!----------------------')
            sys.exit(1)


    CreateDataLake = PythonOperator(
        task_id = 'Create_Data_Lake',
        python_callable=createDataLake
    )

    GetData = PythonOperator(
        task_id='Read_From_Data_Lake',
        python_callable=readDataLake
    )

    InsertIntoStagingDB = PythonOperator(
        task_id = 'Insert_Into_Staging_DB',
        python_callable=insertIntoStatging
    )

    ValidateStagingDB = PythonOperator(
        task_id = 'Validate_Staging_DB',
        python_callable=validateStaging
    )

    LoadData = PythonOperator(
        task_id = 'Load_Into_Warehouse',
        python_callable=insertElasticSearch
    )

    CleanStuff = PythonOperator(
        task_id = 'Clean_File_Stuff',
        python_callable=cleanDirectories
    )

    CreateDataLake >> GetData >> InsertIntoStagingDB >> ValidateStagingDB >> LoadData >> CleanStuff