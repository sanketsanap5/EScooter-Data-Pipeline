import datetime as dt
import os, sys

from airflow import DAG
from airflow.operators.python import PythonOperator, task

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time

#Setting Environment variable for Spark
os.system('export SPARK_HOME=/Users/Sanket/Spark/spark3')
os.system('export PATH=$SPARK_HOME/bin/:$PATH')
os.system('export PYSPARK_PYTHON=python3')


import findspark
findspark.init('/Users/Sanket/Spark/spark3')

from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


#os.system('cd /Users/Sanket/Spark/spark3/sbin/')
#os.system('./start-master.sh')
#time.sleep(3)
#os.system('cd /Users/Sanket/Spark/spark-node/sbin')
#os.system('./start-slave.sh spark://Sankets-MacBook-Air.local:7077 -p 9911')
#time.sleep(3)


deafult_args = {
    'owner' : 'sanket',
    'start_date' : dt.datetime(2022,1,1),
    'retries' : 1,
    'retry_delay' : dt.timedelta(minutes=2)
}

with DAG(
    'Spark_DataPipeline',
    default_args = deafult_args,
    schedule_interval = '@once'
) as dag:

    baseDir = '/Users/Sanket/Documents/GitHub/Real-Time-Data-Pipeline'
    outputDir = baseDir + '/output'
    escooter = baseDir + '/dataset/scooter.csv'
    cleanedScooter = 'cleanedScooterData.json'
    mayScooter = 'MayScooterData.csv'
    juneScooter = 'JuneScooterData.csv'
    julyScooter = 'JulyScooterData.csv'

    conn_string = "dbname='test' host='localhost' user='postgres' password='postgres'"
    conn = db.connect(conn_string)

    def connectSpark():
        spark = SparkSession \
            .builder \
            .master('spark://Sankets-MacBook-Air.local:7077') \
            .appName("Spark-Pipeline") \
            .config("spark.jars", "/Users/Sanket/nifi-1.15.2/drivers/postgresql-42.3.1.jar") \
            .getOrCreate()

        df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/test") \
            .option("dbtable", "escooter") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df,spark

    def readAndClean():
        if not os.path.exists(escooter):
            print('File Not Found !!!')
            sys.exit()
        #read
        df = pd.read_csv(escooter)
        #lower case all columns
        df.columns = [x.lower() for x in df.columns]
        #Fill all NaN for start and end location names
        startValues = {'start_location_name':'Unknown'}
        endValues = {'end_location_name':'Unknown'}
        newdf=df[df.start_location_name.isnull()].fillna(value=startValues)
        df.loc[newdf.index]=newdf
        newdf=df[df.end_location_name.isnull()].fillna(value=endValues)
        df.loc[newdf.index]=newdf
        newdf = df[df.duration.isnull()].fillna(value="00:00:00")
        df.loc[newdf.index] = newdf
        #change data type
        df.started_at = pd.to_datetime(df.started_at,format='%m/%d/%Y %H:%M')
        df.ended_at = pd.to_datetime(df.ended_at,format='%m/%d/%Y %H:%M')

        if os.path.exists(cleanedScooter):
            os.remove(cleanedScooter)

        df.to_json(cleanedScooter)
        print('---------------------- Data Cleaned and Saved --------------------')

    def insertIntoStatging():
        if not os.path.exists(cleanedScooter):
            print('----------------------!!! No Stage File Found !!!--------------------')
            sys.exit(1)
        escooterDF = pd.read_json(cleanedScooter)
        cur = conn.cursor()
        print('Inserting {} Records with {} columns In Staging DB'.format(escooterDF.trip_id.count(),escooterDF.columns))
        try:
            query = "insert into escooter (month,trip_id,region_id,vehicle_id,started_at,ended_at,  \
            duration,start_location_name,end_location_name,user_id,trip_ledger_id)  \
            values(,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cur.executemany(query,escooterDF.values)
            conn.commit()
        except:
            if not pd.read_sql('select count(*) from escooter',conn).values == 0:
                conn.rollback()
                print('-----------------------!!! Staging DB Has Stail Data !!!----------------------')
                cur.execute('truncate table escooter')
                conn.commit()
                print('-----------------------!!! Stail Data Truncated !!!------------------------')
            
            print('--------------------------- Retrying Insert Into Staging DB --------------------------')
            query = "insert into escooter (month,trip_id,region_id,vehicle_id,started_at,ended_at,  \
            duration,start_location_name,end_location_name,user_id,trip_ledger_id)  \
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cur.executemany(query,escooterDF.values)
            conn.commit()
        if pd.read_sql('select count(*) from escooter',conn).values == escooterDF.trip_id.count() :
            conn.close()
            print('-------------- Data Staging Completed ---------------')
        else:
            print('--------------!!! Data Staging Failed !!!---------------')
            sys.exit(1)

    def processScooterData():
        try:
            df,spark = connectSpark()
        except:
            os.system('cd /Users/Sanket/Spark/spark-node/sbin')
            os.system('./stop-slave.sh')
            os.system('cd /Users/Sanket/Spark/spark3/sbin')
            os.system('./stop-master.sh ')
            os.system('./start-master.sh ')
            os.system('cd /Users/Sanket/Spark/spark-node/sbin')
            os.system('./start-slave.sh spark://Sankets-MacBook-Air.local:7077 -p 9911')
            time.sleep(2)

            df,spark = connectSpark()

        #drop unnecessary columns
        df = df.drop('region_id')

        if os.path.exists(mayScooter):
            os.remove(mayScooter)
        if os.path.exists(juneScooter):
            os.remove(juneScooter)
        if os.path.exists(julyScooter):
            os.remove(julyScooter)

        address_Arr = split(df.start_location_name,',')
        zipCode = split(regexp_extract(df.start_location_name, "(NM \\d{5})" ,1 ),' ')
        df = df.withColumn('start_street',address_Arr.getItem(0))
        df = df.withColumn('start_zip',zipCode.getItem(1))

        endaddress_Arr = split(df.end_location_name,',')
        endzipCode = split(regexp_extract(df.end_location_name, "(NM \\d{5})" ,1 ),' ')
        df = df.withColumn('end_street',endaddress_Arr.getItem(0))
        df = df.withColumn('end_zip',endzipCode.getItem(1))

        df = df.fillna(value="Unknown",subset=['start_zip','end_zip','start_street','end_street'])
        df = df.withColumn('start_zip',df.started_at.cast(StringType())) \
            .withColumn('end_zip',df.ended_at.cast(StringType()))
        
        mayData = df.filter(df.month == "May")
        mayData.toPandas().to_csv(mayScooter)
        juneData = df.filter(df.month == "June")
        juneData.toPandas().to_csv(juneScooter)
        julyData = df.filter(df.month == "July")
        julyData.toPandas().to_csv(julyScooter)

        spark.stop()
        print('---------------------- Monthly Data Saved --------------------------')

    def loadIntoWarehouse():
        es = Elasticsearch()
        dfMay = pd.read_csv(mayScooter)
        dfJune = pd.read_csv(juneScooter)
        dfJuly = pd.read_csv(julyScooter)
        dataSourceMay =[
            {
                "_index":"mayscooterdata",
                "_source":dfMay.loc[i].to_dict()
            }
            for i in range(dfMay['trip_id'].count())
        ]
        dataSourceJune =[
            {
                "_index":"junescooterdata",
                "_source":dfJune.loc[i].to_dict()
            }
            for i in range(dfJune['trip_id'].count())
        ]
        dataSourceJuly =[
            {
                "_index":"julyscooterdata",
                "_source":dfJuly.loc[i].to_dict()
            }
            for i in range(dfJuly['trip_id'].count())
        ]
        result1 = helpers.bulk(es,dataSourceMay)
        result2 = helpers.bulk(es,dataSourceJune)
        result3 = helpers.bulk(es,dataSourceJuly)

        time.sleep(2)

        if result1[0] !=0 and result2[0] !=0 and result3[0] !=0:
            print('-------------- Data Loaded --------------')
        else:
            print('-------------------!!! Data Load Failed !!!--------------------')
            sys.exit(1)


    def loadFilesIntoDirectory():
        monthlyDataDir = outputDir + '/monthlyData'
        if not os.path.exists(outputDir):
            os.mkdir(outputDir)
        if not os.path.exists(monthlyDataDir):
            os.mkdir(monthlyDataDir)
        if os.path.exists(mayScooter):
            os.system('cp '+ mayScooter + ' ' + monthlyDataDir + '/' + mayScooter)
        if os.path.exists(juneScooter):
            os.system('cp '+ juneScooter + ' ' + monthlyDataDir + '/' + juneScooter)
        if os.path.exists(julyScooter):
            os.system('cp '+ julyScooter + ' ' + monthlyDataDir + '/' + julyScooter)
        
        if os.path.exists(monthlyDataDir + '/' + julyScooter):
            os.remove(cleanedScooter)
            os.remove(mayScooter)
            os.remove(juneScooter)
            os.remove(julyScooter)
            print('-------------------------- Files Loaded ----------------------------')
        else:
            print('------------------!!! Files Transfer Failed !!!---------------------')
        
    
    ReadAndCleanScooterData = PythonOperator(
        task_id='Read_And_Clean_Scooter_Data',
        python_callable=readAndClean
    )

    InsertIntoStatgingDB = PythonOperator(
        task_id = 'Insert_Into_Staging_DB',
        python_callable=insertIntoStatging
    )

    ProcessScooterData = PythonOperator(
        task_id='Process_Scooter_Data_In_Spark',
        python_callable=processScooterData
    )

    LoadIntoWarehouse = PythonOperator(
        task_id = 'Load_Into_Warehouse',
        python_callable=loadIntoWarehouse
    )

    LoadFilesIntoDirectory = PythonOperator(
        task_id='LoadFilesIntoDirectory',
        python_callable=loadFilesIntoDirectory
    )

    ReadAndCleanScooterData >> InsertIntoStatgingDB >> ProcessScooterData >> LoadIntoWarehouse >>  LoadFilesIntoDirectory



