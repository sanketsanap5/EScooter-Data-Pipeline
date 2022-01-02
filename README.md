# EScooter-Data-Pipeline

## Technology Stack:

  - Apache Airflow
  - Python
  - Pandas
  - Spark
  - PostgreSQL (Relational)
  - ElasticSearch, Kibana (NoSQL)
  - Files (csv, json)
  - OS (MacOS)

## Summary

  This is a automated data pipeline workflow performing **ETL** (Extract, Transform, Load) operations using **Python** programming language with the help of **Pandas** library where data is processed parallely in disributed envionment using **Spark** data processing engine

  Relational database **PostgreSQL** acting as _Staging Database_ and NoSQL database **ElasticSearch** as _Data Warehouse_, along with that 
  _csv_ and _json_ data files acting as multiple data sources

  Data pipeline managemet tool **Apache Airflow** is used to create and manage automated data workflow efficiently with help of **DAG** Python, Bash Operators

## Problem

  We have the transactional RAW data for *Escooter* organization, where huge number of records are available in the datalake. This transaction needs to be
  stored in a data warehose for Business Intelligence. Since, this is RAW data, it might be dirty and flawed. We need to make sure useful data lands in warehouse
  via data pipeline. Pipeline should be fully automated which can run daily and load data into warehouse
  
## Solution

  > In order to achieve automated data pipeline which will load transactional data into warehouse
  1. First of all we need to clean the data by performing **ETL** operations with the help of **Pandas** library and put cleaned data in **staging** database 
      which would be **PostgreSQL** in this case.
  3. Once data is loaded successfully in staging db, we will process and validate data in distributed way and parallely with the help of **PySpark** library with
      single master and slave node respectively, we will trandsorm and enrich data by adding new useful fields and removing duplicate/unnecessary data
  4. After that we will start loading this new processed data into the warehouse, which is **ElasticSearch** in this case, where we will be creating different
      index as per requirement
  4. Once all done, we will clean the data directory and copy the staging files i.e. CSV, JSON into final directory

 All above steps will be executed one after another efficiently with the help of **Apache Airflow** tool, we will create a **DAG** and respective python process, 
 where each process will perform single task in ordered manner one after another with the help of **PythonOperator**
 
 The final result is we have a automated pipeline which perfoms **ETL** operations with help of **Pandas** and load data into staging database **PostgreSQL** and
 then distributed processing using **Spark** and finally load clean, useful data into the warehouse **ElasticSearch**

 
  
## Execution Steps

  1. Install Apache Airflow, PostgreSQL, pgAdmin 4, ElasticSearch, Kibana, Python3, Pandas, PySpark, psycopg2
 
  2. Move source code .py file in ~/Airflow/dags directory

          $ mv EScooter-Data-Pipeline/Pipeline/e-scooterPipelineSpark.py    ~/Airflow/dags
          
  3. Start Elastic Search database instance

          $ cd elasticsearch-7.16.2/
          
          $ bin/elasticsearch
  
  4. Start Kibana GUI instance

          $ kibana
         
  5. Open Kibana GUI dashborad
  
          open https://localhost:5601/ and navigate to dashboard under left panel

  6. Start Spark Master node instance

          $ cd ~/Spark/master-node/sbin/
          
          $ ./start-master.sh
          
  7. Start Spark Worker node instance
  
          $ cd ~/Spark/worker-node/sbin/
          $ ./start-worker.sh spark://Sankets-MacBook-Air.local:7077 -p 9911

  8. Run Apache Airflow webserver
 
          $ airflow webserver
   
  9. Run Apache Airflow scheduler

          $ airflow scheduler
    
  10. Open Airflow GUI and start pipeline to start execution

          Open https://localhost:8080/   - Master Node
          
          Open https://localhost:8081/   - Worker Node
          
          Turn on green button infront of 'e-scooterSparkPipeline' to start workflow and montior
          
  
   
