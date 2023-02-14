import json
import requests
from datetime import datetime, date, timedelta,timezone
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from urllib.parse import quote
import vaex
import psycopg2 
from calendar import monthrange
import io
import sys
from io import StringIO, BytesIO
import boto3
import glob
import fnmatch
import glob
import pysftp
from common_function import function
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email_operator import EmailOperator
from airflow.exceptions import AirflowFailException
from airflow.operators.dummy_operator import DummyOperator

def CTP47CPEN_LOAD01():
    try:
        #download only config.txt
        function.download_config_file('daily-ctp-47-der-cpen')
    except Exception as e:
        print(e)
        print('Download config file from minio fail')    
        raise AirflowFailException("Download config file from minio fail")
        
    try:
        #read config files
        config = function.read_config()
    except Exception as e:
        print(e)
        print('Read config file fail')    
        raise AirflowFailException("Read config file fail")

    #update batch     
    task_name = config['task_name'].split(",")[0]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

    bucket = function.create_connection_minio(config['bucket_name'])
    path = config['load_path']
    file_name = config['file_name01']

    #batch status
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
    try:
        #read table,convert dataframe to .parquet and upload to minio
        df,con_db2 = function.read_db2_monthly(config['RDT040703'],config['MSR_PRD_ID'])
        function.df_to_parquet(df,file_name)
        con_db2.close()
    except Exception as e:
        print(e)
        print('Connection db2 fail Line 64')

        #update batch  
        function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))
        raise AirflowFailException("Connection db2 fail Line 64")

    try:
        #upload to minio
        function.upload_files_to_minio(file_name,path,bucket)
    except Exception as e:
        print(e)
        print('Cannot upload files to minio Line 77')

        #update batch  
        function.update_btch(config['RDT0407001'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))
        raise AirflowFailException("Cannot upload files to minio Line 77")

    #update batch     
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status)

def CTP47CPEN_XFRM01():
    try:
        #download config folder
        function.download_config_file('daily-ctp-47-der-cpen')
    except Exception as e:
        print(e)
        print('Download config file from minio fail Line 94')    
        raise AirflowFailException("Download config file from minio fail Line 94")

    try:
        #read config files
        config = function.read_config()
    except Exception as e:
        print(e)
        print('Read config file fail Line 102')    
        raise AirflowFailException("Read config file fail Line 102")

    #read config and get object key value {'Load_path': 'load/'}
    config = function.read_config()

    #update batch     
    task_name = config['task_name'].split(",")[0]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

    try:
        #download table from minio
        bucket = function.create_connection_minio(config['bucket_name'])
        function.download_parquet_from_minio(config['load_path'],bucket)

    except Exception as e:
        print(e)
        print('Connect minio fail, cannot download file from minio Line 120')

        #update batch    
        date_process = config['date_process'].split(",")[1]       #[0] start [1] end
        status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
        function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today')) 

        raise AirflowFailException("Connect minio fail, cannot download file from minio Line 120")

    #vaex read table cpen get dataframe
    cpen  = vaex.open('{}*.parquet'.format(config['file_name01'])) # load table cpen

    column_df = ['ent_id','cntpr_id']

    #clear space ans set space = null
    df = function.clear_space(cpen,column_df)
    
    #create column
    msr_daily = config['MSR_PRD_ID']
    org_id = config['org_id']
    df['msr_prd_id'] = np.repeat(msr_daily, df.shape[0])
    df['org_id'] = np.repeat(org_id, df.shape[0])
    df['data_dt'] = np.repeat(msr_daily, df.shape[0])
    df['ppn_tm'] = np.repeat(function.data_dt('timestamp'), df.shape[0])

    #select column
    df_new = df[['msr_prd_id','org_id','data_dt','ent_id','cntpr_id','ppn_tm']]

    #fill null dataframe
    df_new['msr_prd_id'] = df_new['msr_prd_id'].fillna(value=msr_daily)
    df_new['org_id'] = df_new['org_id'].fillna(value=org_id)
    df_new['data_dt'] = df_new['data_dt'].fillna(value=msr_daily)

     #upload table to minio
    name = config['file_name02']
    df_new.export_many(name+'_{i:03}.parquet',chunk_size=int(config['chunksize']))                

    try:
        function.upload_files_to_minio(name,config['fxrm_path'],bucket)

    except Exception as e:
        print(e)
        print('Cannot upload file to minio Line 163')

        #update batch    
        date_process = config['date_process'].split(",")[1]       #[0] start [1] end
        status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
        function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

        raise AirflowFailException("Cannot upload file to minio Line 163")

    #update batch 
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status)
    
def CTP47CPEN_AP01_MPI_DER_CPEN():
    try:
        #download config folder
        function.download_config_file('daily-ctp-47-der-cpen')
    except Exception as e:
        print(e)
        print('Download config file from minio fail Line 184')    
        raise AirflowFailException("Download config file from minio fail Line 184")

    try:
        #read config files
        config = function.read_config()
    except Exception as e:
        print(e)
        print('Read config file fail Line 192')    
        raise AirflowFailException("Read config file fail Line 192")

    #update batch
    task_name = config['task_name'].split(",")[3]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status)

    try:
        #download table from minio
        bucket = function.create_connection_minio(config['bucket_name'])
        function.download_parquet_from_minio(config['fxrm_path'],bucket)

    except Exception as e:
        print(e)
        print('Connect minio fail, cannot download file from minio Line 206')

        #update batch      
        date_process = config['date_process'].split(",")[1]       #[0] start [1] end
        status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
        function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

        raise AirflowFailException("Connect minio fail, cannot download file from minio Line 206")

    table = config['table']
    chunksize = int(config['chunksize'])

    #insert table to PostgreSQL    
    query = function.query_insert(table)

    try:
        #check offset from PostgreSQL
        offset_index = function.check_offset_from_postgresql(table,chunksize)

        function.insert_table_into_postgresql(config['file_name03'],query,offset_index,chunksize)
    except Exception as e:
        print(e)
        print('cannot insert data to PostgreSQL Line 231')

        #update batch     
        date_process = config['date_process'].split(",")[1]       #[0] start [1] end
        status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
        function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

        raise AirflowFailException("cannot insert data to PostgreSQL Line 231")

    #update batch    
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status)
    
def CTP47CPEN_VALD01():
    def RICPEN001(conn):
        query = open(config['RDT040704'], 'rt')
        df = pd.read_sql(query.read(), conn) 
        query.close() 
        return df

    try:
        #download config folder
        function.download_config_file('daily-ctp-47-der-cpen')
    except Exception as e:
        print(e)
        print('Download config file from minio fail Line 257')    
        raise AirflowFailException("Download config file from minio fail Line 257")

    try:
        #read config files
        config = function.read_config()
    except Exception as e:
        print(e)
        print('Read config file fail Line 265')    
        raise AirflowFailException("Read config file fail Line 265")

    #update batch    
    task_name = config['task_name'].split(",")[4]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status)

    conn_psql = function.create_connection_psql()
    bucket_name = config['bucket_name']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')
    chunksize = int(config['chunksize'])
    name = config['file_name04']

    try:
        #validation rule get report column cntpr_id
        result_RICPEN001 = RICPEN001(conn_psql)
        conn_psql.close()
        
    except Exception as e:
        print(e)
        print('Connection PostgreSQL fail Line 287')

        #update batch     
        date_process = config['date_process'].split(",")[1]       #[0] start [1] end
        status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
        function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

        raise AirflowFailException("Connection PostgreSQL fail Line 287")

    #validation success
    if  result_RICPEN001['Counterparty Id'].count() == 0 :
        function.upload_validation_success_check_to_minio(bucket)
        print('success') 
        
        try:
            function.upload_validation_success_check_to_minio(bucket)
        except Exception as e:
            print(e)
            print('upload_validation_success_check_to_minio fail Line 302')    
            raise AirflowFailException("upload_validation_success_check_to_minio fail Line 302")

            #update batch     
            date_process = config['date_process'].split(",")[1]       #[0] start [1] end
            status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
            function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

        print('success')
    else:
        unixtime = str(int(datetime.now().timestamp()))

        try:
            if result_RICPEN001['Counterparty Id'].count() != 0:
                function.upload_csv_to_minio(result_RICPEN001,'{0}{1}/RICPEN001.csv'.format(config['vald_path'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            print(e)
            print('upload_csv_to_minio RICPEN001.csv fail Line 323')    
            raise AirflowFailException("upload_csv_to_minio RICPEN001.csv fail Line 323")
            
             #update batch     
            date_process = config['date_process'].split(",")[1]       #[0] start [1] end
            status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
            function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

        #update batch     
        date_process = config['date_process'].split(",")[1]       #[0] start [1] end
        status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
        function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

    #update batch   
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status)
    
def CTP47CPEN_GEN01(): 
    try:
        #download config folder
        function.download_config_file('daily-ctp-47-der-cpen')
    except Exception as e:
        print(e)
        print('Download config file from minio fail Line 348')    
        raise AirflowFailException("Download config file from minio fail Line 348")

    try:
        #read config files
        config = function.read_config()
    except Exception as e:
        print(e)
        print('Read config file fail Line 348')    
        raise AirflowFailException("Read config file fail Line 348")

    #update batch  
    task_name = config['task_name'].split(",")[4]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status)

    bucket_name = config['bucket_name']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')

    #load table from PostgreSQL and upload file .csv to minio
    path = '{0}{1}.csv'.format(config['gen_path'],config['MSR_PRD_ID'])

    #check folder validation in minio
    file_success ='success.txt'
    list_folder = [config['load_path'],config['fxrm_path'],config['vald_path'],file_success]

    #status batch
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
    try:
        for object in bucket.objects.filter(Prefix = file_success):
            check_validation_fail = object.key
            if check_validation_fail == file_success:

                try:
                    #gen files
                    df,conn_psql = function.read_psql(config['RDT040705'])
                    df = pd.concat(df,ignore_index=True)
                    conn_psql.close()

                except Exception as e:
                    print(e)
                    print('Connection PSQL fail Line 390')

                    #update batch   
                    function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))
                    raise AirflowFailException("Connection PSQL fail Line 390")

                try:
                    function.upload_csv_to_minio(df,path, int(config['chunksize']),s3,bucket_name)

                except Exception as e:
                    print(e)
                    print('Upload files to minio fail Line 403')

                    #update batch   
                    function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))
                    raise AirflowFailException("Upload files to minio fail Line 403")

                try:
                    #delete load, transform, validation, success.txt
                    function.delete_folder_in_minio_when_success(bucket,list_folder)

                except Exception as e:
                    print(e)
                    print('delete succes.txt minio fail Line 415')

                    #update batch   
                    function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))
                    raise AirflowFailException("delete succes.txt minio fail Line 415")

                try:
                    #update MSR_PRD_ID
                    msr_prd_id = function.update_msr_prd_id_daily(config['MSR_PRD_ID'])
                    function.update_config(msr_prd_id,bucket,s3,bucket_name)  

                except Exception as e:
                    print(e)
                    print('update MSR_PRD_ID fail Line 427')

                    #update batch   
                    function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))
                    raise AirflowFailException("update MSR_PRD_ID fail Line 427")

            else:
                pass
    except Exception as e:
        print(e)
        print('Connection minio fail Line 384')

        #update batch   
        function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))
        raise AirflowFailException("Connection minio fail Line 384")

    #update batch    
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT040701'],config['RDT040702'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 11, 15),
    'schedule_interval': None,
}
with DAG('DAILY_CTP_47_DER_CPEN',
         schedule_interval='@daily', 
         default_args=default_args,
         description='DAILY: 4.7 Counterparty Entity',
         catchup=False) as dag:
 
    task1 = PythonOperator(
        task_id='CTP47CPEN_LOAD01',
        python_callable=CTP47CPEN_LOAD01
    )
    
    task2 = PythonOperator(
        task_id='CTP47CPEN_XFRM01',
        python_callable=CTP47CPEN_XFRM01
    )

    task3 = PythonOperator(
        task_id='CTP47CPEN_AP01_MPI_DER_CPEN',
        python_callable=CTP47CPEN_AP01_MPI_DER_CPEN   
    )

    task4 = PythonOperator(
        task_id='CTP47CPEN_VALD01',
        python_callable=CTP47CPEN_VALD01      
    ) 

    task5 = PythonOperator(
        task_id='CTP47CPEN_GEN01',
        python_callable=CTP47CPEN_GEN01      
    ) 

    task1 >> task2 >> task3 >> task4 >> task5 
