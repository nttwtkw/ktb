from datetime import datetime
import pandas as pd
import numpy as np
import vaex
import time
import sys
from common_function_2 import function
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator

#table batch 1.rdtdba.btch_job_test 2.rdtdba.btch_shd_test

def COL34BLD_WAIT_TRIGGER01():
    try:
        #read config files
        config = function.read_config_psql('P1_DER_BLD')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    #update batch     
    task_name = config['TASK_NAME01']
    number_id_job = '1'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)

    #connect minio ex: download file , upload parquet to minio type file
    bucket = function.create_connection_minio(config['BUCKET_NAME']) 
    files = [config['FILE_TRIGGER']]

    #batch status
    date_process = config['PROCESS_END_DATE']       
    status = config['STATUS_ERROR']                      

    #time out 30 minutes
    timeout = time.time() + 60*30

    try:
        #wait files from minio
        function.wait_files_from_minio(bucket, files, timeout,1 )
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
        raise AirflowFailException('Wait files from minio fail Line', exc_tb.tb_lineno)

    #update batch     
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status)

def COL34BLD_LOAD01():
    try:
        #read config files
        config = function.read_config_psql('P1_DER_BLD')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    #update batch     
    task_name = config['TASK_NAME02']
    number_id_job = '2'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status)

    #connect minio ex: download file , upload parquet to minio type file
    bucket = function.create_connection_minio(config['BUCKET_NAME']) 
    file_name = config['FILE_NAME03']

    #batch status
    date_process = config['PROCESS_END_DATE']      
    status = config['STATUS_ERROR']                         

    try:
        #read table,convert dataframe to .parquet and upload to minio
        df,con_db2 = function.read_db2(config['RDT030403'])
        function.df_to_parquet(df,file_name)
        con_db2.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
        raise AirflowFailException('Connection db2 fail Line', exc_tb.tb_lineno)

    try:
        #upload to minio
        function.upload_files_to_minio(file_name,config['LOAD_PATH'],bucket)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
        raise AirflowFailException('Cannot upload files to minio Line', exc_tb.tb_lineno)

    #update batch     
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status)
def COL34BLD_XFRM01():
    try:
        #read config files
        config = function.read_config_psql('P1_DER_BLD')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    #update batch  
    task_name = config['TASK_NAME03']
    number_id_job = '3'
    date_process = config['PROCESS_START_DATE']      
    status = config['STATUS_RUN']                      
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status)

    #update batch case fail
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    try:
        #download table from minio
        bucket = function.create_connection_minio(config['BUCKET_NAME'])
        function.download_parquet_from_minio(config['LOAD_PATH'],bucket)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True) 
        raise AirflowFailException('Connect minio fail, cannot download file from minio Line', exc_tb.tb_lineno)

    #vaex read data
    df = vaex.open('{0}*.parquet'.format(config['FILE_NAME03']))

    #upload table to minio
    name = config['FILE_NAME04'] 
    df.export_many(name+'_{i:03}.parquet',chunk_size=int(config['CHUNKSIZE']))                

    try:
        function.upload_files_to_minio(name,config['FXRM_PATH'],bucket)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
        raise AirflowFailException('Cannot upload file to minio Line' ,exc_tb.tb_lineno)

    #update batch 
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status)
def COL34BLD_AP01_MPI_DER_BLD():
    try:
        #read config files
        config = function.read_config_psql('P1_DER_BLD')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

    #update batch
    task_name = config['TASK_NAME04']
    number_id_job = '4'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status)

    #update batch case fail
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    try:
        #download table from minio
        bucket = function.create_connection_minio(config['BUCKET_NAME'])
        function.download_parquet_from_minio(config['FXRM_PATH'],bucket)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connect minio fail, cannot download file from minio Line", exc_tb.tb_lineno)

    #parameter
    table = config['TABLE02']
    msr_prd_id = config['MSR_PRD_ID']

    try:
        #delete data in table before insert
        function.delete_data_in_psql_filter_by_msr_prd_id(table,msr_prd_id,config['RDT600004'])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
        raise AirflowFailException("cannot delete data to PostgreSQL Line", exc_tb.tb_lineno)  

    #insert table to PostgreSQL 
    query = function.query_insert(table)
    table = function.read_table_local(config['FILE_NAME05'])

    try:
        function.insert_table_into_postgresql_2(table, query, int(config['CHUNKSIZE']), config['TABLE02'].split(".")[-1])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
        raise AirflowFailException("cannot insert data to PostgreSQL Line", exc_tb.tb_lineno)

    #update batch    
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status)
def COL34BLD_VALD01():
    # query data with condition RDT vilidation and return Collateral Id where not True 
    try:
        #read config files
        config = function.read_config_psql('P1_DER_BLD')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

    #update batch    
    task_name = config['TASK_NAME05']
    number_id_job = '5'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status)

    conn_psql = function.create_connection_psql()
    bucket_name = config['BUCKET_NAME']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')
    chunksize = int(config['CHUNKSIZE'])
    name = config['FILE_NAME06']

    #update batch case fail    
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    try:
        #uery data with condition RDT vilidation and return Collateral Id where not True 
        #validation rule get report column Collateral Id

        #If [Country] is ‘TH’, then either [DOPA Location] or [DOL Location] cannot be blank, [Number of Floor] and [Area Utilization] cannot be blank and must be greater than 0.
        result_CMBLD001 = pd.read_sql(config['RDT030405'] ,conn_psql) 

        #If [Country] is ‘TH’ and [Account Purpose] in Credit Account Detail (DER_CACD) is classified as purchasing residential estates (house or residential land including house), then [Developer Type] cannot be blank.
        result_CMBLD002 = pd.read_sql(config['RDT030406'] ,conn_psql) 

        #If [Country] is ‘TH’ and [Developer Type] is real estate project developer which is listed company in Stock Exchange of Thailand (SET or MAI) or not listed, then [Project Name] cannot be blank.
        result_CMBLD003 = pd.read_sql(config['RDT030407'] ,conn_psql) 

        #If [Country] is ‘TH’ and [Collateral Type] in Collateral (DER_COL) is “building” or “land included building”, then [Building Completion Year] cannot be blank.
        result_CMBLD004 = pd.read_sql(config['RDT030408'] ,conn_psql) 

        #If [Country] is ‘TH’ AND [Property Type] is “condominium”, then [Floor Number] cannot be blank.
        result_CMBLD005 = pd.read_sql(config['RDT030409'] ,conn_psql) 

        #If a building record exists, then a collateral record must exist.
        result_RIBLD001 = pd.read_sql(config['RDT030410'] ,conn_psql) 

        conn_psql.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection PostgreSQL fail Line", exc_tb.tb_lineno)

    #validation success
    if (result_CMBLD001['Collateral Id'].count() == 0 and
        result_CMBLD002['Collateral Id'].count() == 0 and
        result_CMBLD003['Collateral Id'].count() == 0 and
        result_CMBLD004['Collateral Id'].count() == 0 and
        result_CMBLD005['Collateral Id'].count() == 0 and
        result_RIBLD001['Collateral Id'].count() == 0):

        try:
            files = config['FILE_SUCCESS']
            function.create_file_trigger_success_to_minio(bucket, files)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)

            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
            raise AirflowFailException("function create_file_trigger_success_to_minio fail Line", exc_tb.tb_lineno)

        print('success')
    else:
        unixtime = str(int(datetime.now().timestamp()))

        try:
            if result_CMBLD001['Collateral Id'].count() != 0:
                function.upload_csv_to_minio(result_CMBLD001,'{0}{1}/COL34BLD_CMBLD001.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio COL34BLD_CMBLD001.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMBLD002['Collateral Id'].count() != 0:
                function.upload_csv_to_minio(result_CMBLD002,'{0}{1}/COL34BLD_CMBLD002.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio COL34BLD_CMBLD002.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMBLD003['Collateral Id'].count() != 0:
                function.upload_csv_to_minio(result_CMBLD003,'{0}{1}/COL34BLD_CMBLD003.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio COL34BLD_CMBLD003.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMBLD004['Collateral Id'].count() != 0:
                function.upload_csv_to_minio(result_CMBLD004,'{0}{1}/COL34BLD_CMBLD004.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio COL34BLD_CMBLD004.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMBLD005['Collateral Id'].count() != 0:
                function.upload_csv_to_minio(result_CMBLD005,'{0}{1}/COL34BLD_CMBLD005.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio COL34BLD_CMBLD005.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_RIBLD001['Collateral Id'].count() != 0:
                function.upload_csv_to_minio(result_RIBLD001,'{0}{1}/COL34BLD_RIBLD001.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio COL34BLD_RIBLD001.csv fail Line", exc_tb.tb_lineno)

    #update batch   
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status)  
def COL34BLD_GEN01(): 
    try:
        #read config files
        config = function.read_config_psql('P1_DER_BLD')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

    #update batch  
    task_name = config['TASK_NAME06']
    number_id_job = '6'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status)

    #parameter
    bucket_name = config['BUCKET_NAME']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')
    msr_prd_id = config['MSR_PRD_ID']

    #check folder validation in minio
    file_success = config['FILE_SUCCESS']
    list_folder = [config['LOAD_PATH'],config['FXRM_PATH'],config['VALD_PATH'],file_success,config['FILE_TRIGGER']]

    #status batch case fail
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    try:
        #gen files
        df = function.read_table_psql(config['RDT030411'])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch   
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection PSQL fail Line", exc_tb.tb_lineno)

    try:
        #load table from PostgreSQL and upload file .csv to minio   
        path = '{0}{1}.csv'.format(config['GEN_PATH'],msr_prd_id)
        function.upload_csv_to_minio_2(df,path, int(config['CHUNKSIZE']),s3,bucket_name, config['TABLE02'].split('.')[-1])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch   
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
        raise AirflowFailException("Upload files to minio fail Line", exc_tb.tb_lineno)

    try:
        for object in bucket.objects.filter(Prefix = file_success):
            check_validation_fail = object.key

            if check_validation_fail == file_success:
                try:
                    #delete load, transform, validation, success.txt
                    function.delete_folder_in_minio_when_success(bucket,list_folder)

                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    print(e)

                    #update batch   
                    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
                    raise AirflowFailException("Delete folder fail Line", exc_tb.tb_lineno) 

                try:
                    #update MSR_PRD_ID
                    msr_prd_id = function.update_msr_prd_id_monthly(msr_prd_id)
                    list_parm_values = [f'{msr_prd_id}']
                    list_parm_nm = ['MSR_PRD_ID']
                    function.update_config_psql(list_parm_values, list_parm_nm, config['RDT600003'], "P1_"+config['TABLE02'].split(".")[1])
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    print(e)

                    #update batch   
                    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
                    raise AirflowFailException("update msr_prd_id fail Line", exc_tb.tb_lineno)
            else:
                pass
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch   
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection minio fail Line", exc_tb.tb_lineno)

    #update batch    
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID02'],number_id_job,date_process,status,True)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 2, 17),
    'schedule_interval': None,
}
with DAG('P1_MONTHLY_COL_34_DER_BLD',
         schedule_interval='@monthly', 
         default_args=default_args,
         description='Monthly: 3.4 Building',
         catchup=False) as dag:
 
    task1 = PythonOperator(
        task_id='COL34BLD_WAIT_TRIGGER01',
        python_callable=COL34BLD_WAIT_TRIGGER01
    )
    
    task2 = PythonOperator(
        task_id='COL34BLD_LOAD01',
        python_callable=COL34BLD_LOAD01
    )
    
    task3 = PythonOperator(
        task_id='COL34BLD_XFRM01',
        python_callable=COL34BLD_XFRM01
    )

    task4 = PythonOperator(
        task_id='COL34BLD_AP01_MPI_DER_BLD',
        python_callable=COL34BLD_AP01_MPI_DER_BLD    
    )

    task5 = PythonOperator(
        task_id='COL34BLD_VALD01',
        python_callable=COL34BLD_VALD01      
    ) 

    task6 = PythonOperator(
        task_id='COL34BLD_GEN01',
        python_callable=COL34BLD_GEN01      
    ) 

    task1 >> task2 >> task3 >> task4 >> task5 >> task6
