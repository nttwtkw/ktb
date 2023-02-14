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

def CMM75AGF_LOAD01():
    #dowload only config.txt
    function.dowload_config_file('monthly-cmm-75-der-agf')
    
    #read config and get object key value {'Load_path': 'load/'}
    config = function.read_config()
    
    #update batch     
    task_name = config['task_name'].split(",")[0]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))
    
    bucket = function.create_connection_minio(config['bucket_name'])
    path = config['load_path']
    file_name = config['file_name01']

    try:
        #read table,convert dataframe to .parquet and upload to minio
        agf,con_db2 = function.read_db2_monthly(config['RDT070503'],config['MSR_PRD_ID'])
        function.df_to_parquet(agf,file_name)
        function.upload_files_to_minio(file_name,path,bucket)
        con_db2.close()
    except Exception as e:
        print(e)
        print('Connection db2 fail,cannot upload files to minio')
        
        #update batch  
        date_process = config['date_process'].split(",")[1]       #[0] start [1] end
        status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
        function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

        raise AirflowFailException("Connection db2 fail,cannot upload files to minio")

    #update batch     
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

def CMM75AGF_XFRM01():          
    function.dowload_config_file('monthly-cmm-75-der-agf')

    #read config and get object key value {'Load_path': 'load/'}
    config = function.read_config()

    #update batch     
    task_name = config['task_name'].split(",")[0]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

    try:
        #dowload table from minio
        bucket = function.create_connection_minio(config['bucket_name'])
        function.dowload_parquet_from_minio(config['load_path'],bucket)
    except Exception as e:
        print(e)
        print('Connect minio fail, cannot dowload file from minio')

    #update batch    
        date_process = config['date_process'].split(",")[1]       #[0] start [1] end
        status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
        function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today')) 

        raise AirflowFailException("Connect minio fail, cannot dowload file from minio")

    #vaex read table agf get dataframe
    agf  = vaex.open('{}*.parquet'.format(config['file_name01'])) # load table dr

    column_df = ['ac_id','mvmt_tp']

    #clear space ans set space = null
    df = function.clear_space(agf,column_df)
    #create column
    date_month = function.data_dt('monthly')
    msr_month = config['MSR_PRD_ID']
    org_id = config['org_id']
    df['msr_prd_id'] = np.repeat(msr_month, df.shape[0])
    df['org_id'] = np.repeat(org_id, df.shape[0])
    df['data_dt'] = np.repeat(date_month, df.shape[0])
    df['ppn_tm'] = np.repeat(function.data_dt('timestamp'), df.shape[0])

    #select column
    df_new = df[['msr_prd_id','org_id','data_dt','ac_id','mvmt_tp','mvmt_amt_in_baht','ppn_tm']]

    #fill null dataframe
    df_new['msr_prd_id'] = df_new['msr_prd_id'].fillna(value=msr_month)
    df_new['org_id'] = df_new['org_id'].fillna(value='006')
    df_new['data_dt'] = df_new['data_dt'].fillna(value=(date(9999,9,9)).isoformat())

    #upload table to minio    
    name = config['file_name02']
    df_new.export_many(name+'_{i:03}.parquet',chunk_size=int(config['chunksize']))                

    try:
        function.upload_files_to_minio(name,config['fxrm_path'],bucket)

    except Exception as e:
        print(e)
        print('Cannot upload file to minio')

        #update batch    
        date_process = config['date_process'].split(",")[1]       #[0] start [1] end
        status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
        function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

        raise AirflowFailException("Cannot upload file to minio")

    #update batch 
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status)
    
def CMM75AGF_AP01_MPI_DER_AGF():
    function.dowload_config_file('monthly-cmm-75-der-agf')
    config = function.read_config()
    
    #update batch
    task_name = config['task_name'].split(",")[2]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status)
    
    try:
        #dowload table from minio
        bucket = function.create_connection_minio(config['bucket_name'])
        function.dowload_parquet_from_minio(config['fxrm_path'],bucket)
    except Exception as e:
        print(e)
        print('Connect minio fail, cannot dowload file from minio')
        
        #update batch      
        date_process = config['date_process'].split(",")[1]       #[0] start [1] end
        status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
        function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

        raise AirflowFailException("Connect minio fail, cannot dowload file from minio")
        
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
        print('cannot insert data to PostgreSQL')
        
        #update batch     
        date_process = config['date_process'].split(",")[1]       #[0] start [1] end
        status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
        function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))


        raise AirflowFailException("cannot insert data to PostgreSQL")

    #update batch    
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status)
    
def CMM75AGF_VALD01():
    #def CMAGF001(conn):
    #    query = open(config['RDT070504'], 'rt')
    #    df = pd.read_sql(query.read(), conn) 
    #    query.close() 
    #    return df
    
    def CMAGF002(conn):
        query = open(config['RDT070505'], 'rt')
        df = pd.read_sql(query.read(), conn) 
        query.close() 
        return df
    
    #def RIAGF001(conn):
    #    query = open(config['RDT070506'], 'rt')
    #    df = pd.read_sql(query.read(), conn) 
    #    query.close() 
    #    return df

    #dowload only config.txt    
    function.dowload_config_file('monthly-cmm-75-der-agf')

    #read config and get object key value {'Load_path': 'load/'}
    config = function.read_config()
    
    #update batch    
    task_name = config['task_name'].split(",")[3]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status)

    conn_psql = function.create_connection_psql()
    bucket_name = config['bucket_name']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')
    chunksize = int(config['chunksize'])
    name = config['file_name04']

    try:
        #validation rule get report column ac_id
        #result_CMAGF001 = CMAGF001(conn_psql)
        result_CMAGF002 = CMAGF002(conn_psql)
        #result_RIAGF001 = RIAGF001(conn_psql)
        conn_psql.close()
        
    except Exception as e:
        print(e)
        print('cannot insert data to PostgreSQL')
        #raise AirflowFailException("Connection PostgreSQL fail")

    #validation success
    if (#result_CMAGF001['Account Id'].count() == 0 and 
        result_CMAGF002['Account Id'].count() == 0 ) :
        #result_RIAGF001['Account Id'].count() == 0 ):
        function.upload_validation_success_check_to_minio(bucket)
        print('success') 

    #validation fail gen report to minio
    else:
        unixtime = str(int(datetime.now().timestamp()))
        #if result_CMAGF001['Account Id'].count() != 0:
        #    function.upload_csv_to_minio(result_CMAGF001,'{0}{1}/CMM75AGF_CMAGF001.csv'.format(config['vald_path'],unixtime),chunksize,s3,bucket_name)
        if result_CMAGF002['Account Id'].count() != 0:
            function.upload_csv_to_minio(result_CMAGF002,'{0}{1}/CMM75AGF_CMAGF002.csv'.format(config['vald_path'],unixtime),chunksize,s3,bucket_name)
        #if result_RIAGF001['Account Id'].count() != 0:
        #    function.upload_csv_to_minio(result_RIAGF001,'{0}{1}/CMM75AGF_RIAGF001.csv'.format(config['vald_path'],unixtime),chunksize,s3,bucket_name)    
            
        #update batch     
        date_process = config['date_process'].split(",")[1]       #[0] start [1] end
        status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
        function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

    #update batch   
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status)
    
def CMM75AGF_GEN01():
    #dowload only config.txt and query.sql file
    function.dowload_config_file('monthly-cmm-75-der-agf')
    
    #read config and get object key value {'Load_path': 'load/'}
    config = function.read_config()
    
     #update batch  
    task_name = config['task_name'].split(",")[4]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status)
    
    bucket_name = config['bucket_name']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')

    #load table from PostgreSQL and upload file .csv to minio
    path = '{0}{1}.csv'.format(config['gen_path'],config['MSR_PRD_ID'])

    #check folder validation in minio
    file_success ='success.txt'

    #status batch
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][1]                         #[0]R      [1]A      [2]S

    try:
        for object in bucket.objects.filter(Prefix = file_success):
            check_validation_fail = object.key
            if check_validation_fail == file_success:

                try:
                    #delete old files
                    obj = s3.Object(bucket_name, file_success )
                    obj.delete()

                except Exception as e:
                    print(e)
                    print('delete succes.txt minio fail')
                    
                    #update batch   
                    function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))
                    raise AirflowFailException("delete succes.txt minio fail")

                try:
                    #gen files
                    df,conn_psql = function.read_psql(config['RDT070507'])
                    df = pd.concat(df,ignore_index=True)
                    function.upload_csv_to_minio(df,path, int(config['chunksize']),s3,bucket_name)
                    conn_psql.close()

                except Exception as e:
                    print(e)
                    print('Connection PSQL fail')
                    
                    #update batch   
                    function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))
                    raise AirflowFailException("Connection PSQL fail")

                try:
                    #update MSR_PRD_ID
                    msr_prd_id = function.update_msr_prd_id_monthly(config['MSR_PRD_ID'])
                    function.update_config(msr_prd_id,bucket,s3,bucket_name)  

                except Exception as e:
                    print(e)
                    print('update MSR_PRD_ID fail')
                    
                    #update batch   
                    function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))
                    raise AirflowFailException("update MSR_PRD_ID fail")
        
            else:
                pass
    except Exception as e:
        print(e)
        print('Connection minio fail')
        
        #update batch   
        function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))
        raise AirflowFailException("Connection minio fail")

    #update batch    
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT070501'],config['RDT070502'],task_name,config['btch_id'],number_id_job,function.data_dt('timestamp'),date_process,status,function.data_dt('today'))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 12, 31),
    'schedule_interval': None,
}
with DAG('MONTHLY_CMM_75_DER_AGF',
         schedule_interval='@monthly',
         default_args=default_args,
         description='Monthly : 7.5 Aggregated Flow',
         catchup=False) as dag:
 
    task_1 = PythonOperator(
        task_id='CMM75AGF_LOAD01',
        python_callable=CMM75AGF_LOAD01
    )

    task_2 = PythonOperator(
        task_id='CMM75AGF_XFRM01',
        python_callable=CMM75AGF_XFRM01
        
    )

    task_3 = PythonOperator(
        task_id='CMM75AGF_AP01_MPI_DER_AGF',
        python_callable=CMM75AGF_AP01_MPI_DER_AGF
        
    )

    task_4 = PythonOperator(
        task_id='CMM75AGF_VALD01',
        python_callable=CMM75AGF_VALD01
        
    )

    task_5 = PythonOperator(
        task_id='CMM75AGF_GEN01',
        python_callable=CMM75AGF_GEN01

        
    ) 

    task_1 >> task_2 >> task_3 >> task_4 >> task_5
