import datetime as dt
import os, sys

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd

deafult_args = {
    'owner' : 'sanket',
    'start_date' : dt.datetime(2022,1,1),
    'retries' : 1,
    'retry_delay' : dt.timedelta(minutes=2)
}

with DAG(
    'E-ScooterDataPipeline',
    default_args = deafult_args,
    schedule_interval = '@once'
) as dag:

    baseDir = '/Users/Sanket/Documents/GitHub/Real-Time-Data-Pipeline'
    outputDir = baseDir + '/output'
    escooter = baseDir + '/dataset/scooter.csv'
    cleanedScooter = 'cleanedScooterData.csv'
    mayScooter = 'MayScooterData.csv'
    juneScooter = 'JuneScooterData.csv'
    julyScooter = 'JulyScooterData.csv'

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
        newdf=df[df['start_location_name'].isnull()].fillna(value=startValues)
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

        df.to_csv(cleanedScooter)
        print('---------------------- Data Cleaned and Saved --------------------')


    def filterScooterData():
        if not os.path.exists(cleanedScooter):
            print('File Not Found !!!')
            sys.exit()
        df = pd.read_csv(cleanedScooter)
        #drop unnecessary columns
        df.drop(columns='region_id',inplace=True)

        if os.path.exists(mayScooter):
            os.remove(mayScooter)
        if os.path.exists(juneScooter):
            os.remove(juneScooter)
        if os.path.exists(julyScooter):
            os.remove(julyScooter)
        mayData = df[df.month=='May']
        mayData.to_csv(mayScooter)
        juneData = df[df.month=='June']
        juneData.to_csv(juneScooter)
        julyData = df[df.month=='July']
        julyData.to_csv(julyScooter)
        print('---------------------- Monthly Data Saved --------------------------')


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
        task_id='ReadAndCleanScooterData',
        python_callable=readAndClean
    )

    FilterScooterData = PythonOperator(
        task_id='FilterScooterData',
        python_callable=filterScooterData
    )

    LoadFilesIntoDirectory = PythonOperator(
        task_id='LoadFilesIntoDirectory',
        python_callable=loadFilesIntoDirectory
    )

    ReadAndCleanScooterData >> FilterScooterData >> LoadFilesIntoDirectory


