from datetime import datetime
import pandas as pd
import numpy as np
import vaex
import sys
from common_function import function
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator

def CMM78BRD_LOAD01():
    try:
        #read config files
        config = function.read_config_psql('DER_BRD_D')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    #update batch     
    task_name = config['TASK_NAME01']
    number_id_job = '1'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)

    #connect minio ex: download file , upload parquet to minio type file
    bucket = function.create_connection_minio(config['BUCKET_NAME']) 
    file_name = config['FILE_NAME01']

    #batch status
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    #query concat pre validation from psql
    query_psql = config['RDT070802'] + "\n" + config['RDT070803']

    try:
        #read table,convert dataframe to .parquet and upload to minio
        df,con_psql = function.read_psql(query_psql)
        df_copy = pd.concat(df,ignore_index=True)
        con_psql.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException('Connection db2 fail Line', exc_tb.tb_lineno)

    #list rules validation
    list_f_rules, list_table = function.list_rules_validation(df_copy)

    #function upload result pre validation
    if len(list_f_rules) > 0:
        number_incorrect_data = function.upload_pre_validation_fail_to_minio(list_f_rules, df_copy, config['TABLE'].split(".")[-1], conf = 'multiple')
        print(f"PERCNET = {number_incorrect_data/len(df_copy)} %")
    # else:

    #chunk df to parquet
    df = function.df_chunk(df_copy.iloc[:,0:len(list_table)],int(config['CHUNKSIZE']))
    function.df_to_parquet(df, file_name)

    try:
        #upload to minio
        function.upload_files_to_minio(file_name,config['LOAD_PATH'],bucket)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException('Cannot upload files to minio Line', exc_tb.tb_lineno)

    #update batch     
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)

def CMM78BRD_XFRM01():
    try:
        #read config files
        config = function.read_config_psql('DER_BRD_D')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    #update batch  
    task_name = config['TASK_NAME02']
    number_id_job = '2'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)

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

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True) 
        raise AirflowFailException('Connect minio fail, cannot download file from minio Line', exc_tb.tb_lineno)

    #vaex read data
    df = vaex.open('{0}*.parquet'.format(config['FILE_NAME01']))

    #upload table to minio
    name = config['FILE_NAME02']
    df.export_many(name+'_{i:03}.parquet',chunk_size=int(config['CHUNKSIZE']))                

    try:
        function.upload_files_to_minio(name,config['FXRM_PATH'],bucket)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException('Cannot upload file to minio Line' ,exc_tb.tb_lineno)

    #update batch 
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)       

def CMM78BRD_AP01_DPI_DER_BRD():
    try:
        #read config files
        config = function.read_config_psql('DER_BRD_D')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

    #update batch
    task_name = config['TASK_NAME03']
    number_id_job = '3'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)

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

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connect minio fail, cannot download file from minio Line", exc_tb.tb_lineno)

    #parameter
    table = config['TABLE']
    msr_prd_id = config['MSR_PRD_ID']

    try:
        #delete data in table before insert
        function.delete_data_in_psql_filter_by_msr_prd_id(table,msr_prd_id,config['RDT600004'])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("cannot delete data to PostgreSQL Line", exc_tb.tb_lineno)  

    #insert table to PostgreSQL 
    query = function.query_insert(table)
    table = function.read_table_local(config['FILE_NAME03'])

    try:
        function.insert_table_into_postgresql(table,query,int(config['CHUNKSIZE']),config['TABLE'].split(".")[-1])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("cannot insert data to PostgreSQL Line", exc_tb.tb_lineno)

    #update batch    
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)

def CMM78BRD_VALD01():
    try:
        #read config files
        config = function.read_config_psql('DER_BRD_D')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

    #update batch    
    task_name = config['TASK_NAME04']
    number_id_job = '4'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)

    conn_psql = function.create_connection_psql()
    bucket_name = config['BUCKET_NAME']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')
    chunksize = int(config['CHUNKSIZE'])
    name = config['FILE_NAME04']
    table = config['TABLE'].split(".")[-1]

    #update batch case fail    
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    try:
        #query data with condition RDT vilidation and return Transaction Id where not True  

        #If [BOT Document Type] is “Approval Document”, then [BOT Document Date] cannot be blank
        result_CMBRD001 = pd.read_sql(config['RDT070804'], conn_psql)

        #If a BOT reference document record exists, then a transaction flow record must exist, as the two data entities describe the element of transaction.
        result_RIBRD001 = pd.read_sql(config['RDT070805'], conn_psql) 

        conn_psql.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection PostgreSQL fail Line", exc_tb.tb_lineno)

    #validation success
    if (result_CMBRD001['Transaction Id'].count() == 0 
        and result_RIBRD001['Transaction Id'].count() == 0):

        try:
            files = config['FILE_SUCCESS']
            function.create_file_trigger_success_to_minio(bucket, files)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)

            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("function create_file_trigger_success_to_minio fail Line", exc_tb.tb_lineno)

        print('success')
    else:
        unixtime = str(int(datetime.now().timestamp()))

        try:
            if result_CMBRD001['Transaction Id'].count() != 0:
                entity_rule = 'CMM78BRD_CMBRD001.csv'
                function.upload_validation_fail_by_source_to_minio(result_CMBRD001, config['VALD_PATH'], entity_rule, unixtime, chunksize, s3, bucket_name, table)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CMM78BRD_CMBRD001.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_RIBRD001['Transaction Id'].count() != 0:
                entity_rule = 'CMM78BRD_RIBRD001.csv'
                function.upload_validation_fail_by_source_to_minio(result_RIBRD001, config['VALD_PATH'], entity_rule, unixtime, chunksize, s3, bucket_name, table)            
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CMM78BRD_RIBRD001.csv fail Line", exc_tb.tb_lineno)

    #update batch   
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)

def CMM78BRD_GEN01():
    try:
        #read config files
        config = function.read_config_psql('DER_BRD_D')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

    #update batch  
    task_name = config['TASK_NAME05']
    number_id_job = '5'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)

    #parameter
    bucket_name = config['BUCKET_NAME']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')
    msr_prd_id = config['MSR_PRD_ID']
    table = config['TABLE'].split(".")[1]

    #check folder validation in minio
    file_success = config['FILE_SUCCESS']
    list_folder = [config['LOAD_PATH'],config['FXRM_PATH'],config['VALD_PATH'],config['PRE_VALD_PATH'],file_success]

    #status batch case fail
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    try:
        #gen files
        df = function.read_table_psql(config['RDT070806'])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch   
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection PSQL fail Line", exc_tb.tb_lineno)

    try:
        #load table from PostgreSQL and upload file .csv to minio   
        path = '{0}{1}.csv'.format(config['GEN_PATH'],msr_prd_id)
        function.upload_csv_to_minio(df,path, int(config['CHUNKSIZE']),s3,bucket_name,table)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch   
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
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
                    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
                    raise AirflowFailException("Delete folder fail Line", exc_tb.tb_lineno) 

                try:
                    #update MSR_PRD_ID
                    msr_prd_id = function.update_msr_prd_id_daily(msr_prd_id)
                    msr_prd_id_cms = function.update_msr_prd_id_daily(config['MSR_PRD_ID_CMS']) #delete when source have same msr_prd_id
                    msr_prd_id_tfs = function.update_msr_prd_id_daily(config['MSR_PRD_ID_TRS']) #delete when source have same msr_prd_id
                    msr_prd_id_trs = function.update_msr_prd_id_daily(config['MSR_PRD_ID_TFS']) #delete when source have same msr_prd_id
                    list_parm_values = [msr_prd_id, msr_prd_id_cms, msr_prd_id_tfs, msr_prd_id_trs]
                    list_parm_nm = ['MSR_PRD_ID', 'MSR_PRD_ID_CMS', 'MSR_PRD_ID_TRS', 'MSR_PRD_ID_TFS']
                    # list_parm_values = [msr_prd_id] 
                    # list_parm_nm = ['MSR_PRD_ID'] 
                    function.update_config_psql(list_parm_values, list_parm_nm, config['RDT600003'], table)
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    print(e)

                    #update batch   
                    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
                    raise AirflowFailException("update msr_prd_id fail Line", exc_tb.tb_lineno)
            else:
                pass
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch   
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection minio fail Line", exc_tb.tb_lineno)

    #update batch    
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 12, 1),
    'schedule_interval': None,
}
with DAG('DAILY_CMM_78_DER_BRD',
         schedule_interval='@daily', 
         default_args=default_args,
         description='Daily: 7.8 BOT Reference Document',
         catchup=False) as dag:
 
    task1 = PythonOperator(
        task_id='CMM78BRD_LOAD01',
        python_callable=CMM78BRD_LOAD01
    )

    task2 = PythonOperator(
        task_id='CMM78BRD_XFRM01',
        python_callable=CMM78BRD_XFRM01    
    )

    task3 = PythonOperator(
        task_id='CMM78BRD_AP01_DPI_DER_BRD',
        python_callable=CMM78BRD_AP01_DPI_DER_BRD      
    ) 

    task4 = PythonOperator(
        task_id='CMM78BRD_VALD01',
        python_callable=CMM78BRD_VALD01      
    ) 

    task5 = PythonOperator(
        task_id='CMM78BRD_GEN01',
        python_callable=CMM78BRD_GEN01
    )

    
    task1 >> task2 >> task3 >> task4 >> task5
