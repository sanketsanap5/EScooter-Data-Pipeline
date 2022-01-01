import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, task

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

deafult_args = {
    'owner' : 'sanket',
    'start_date' : dt.datetime(2021,12,28),
    'retries' : 1,
    'retry_delay' : dt.timedelta(minutes=2)
}

with DAG(
    'MyDBDAG',
    default_args = deafult_args,
    schedule_interval = "@once"
) as dag:

    def queryPostgres():
        conn_string = "dbname='test' host='localhost' user='postgres' password='postgres'"
        conn = db.connect(conn_string)
        df = pd.read_sql("select name, city from users",conn)
        df.to_csv('postgresData.csv')
        print('-------------- Data Saved ---------------')

    def insertElasticSearch():
        es = Elasticsearch()
        df = pd.read_csv('postgresData.csv')
        for i,r in df.iterrows():
            doc = r.to_json()
            res = es.index(index="postgresusers",document=doc)
            print(res['result'])
        print('-------------- Data Loaded --------------')
        
    getData = PythonOperator(
        task_id='QueryPostgres',
        python_callable=queryPostgres
    )

    loadData = PythonOperator(
        task_id = 'LoadDataInElasticSearch',
        python_callable=insertElasticSearch
    )

    getData >> loadData