from datetime import datetime
import pandas as pd
import numpy as np
import vaex
import sys
from common_function_2 import function
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator

#table batch 1.rdtdba.btch_job_test 2.rdtdba.btch_shd_test

def CAC12CACD_LOAD01():
    try:
        #read config files
        config = function.read_config_psql('P1_DER_CACD')
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
    date_process = config['PROCESS_END_DATE']       #[0] start [1] end
    status = config['STATUS_ERROR']                         #[0]R      [1]A      [2]S

    try:
        #read table,convert dataframe to .parquet and upload to minio
        df,con_db2 = function.read_db2(config['RDT010201'])
        function.df_to_parquet(df,file_name)
        con_db2.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException('Connection db2 fail Line', exc_tb.tb_lineno)

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
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status)
def CAC12CACD_XFRM01():
    try:
        #read config files
        config = function.read_config_psql('P1_DER_CACD')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    #update batch  
    task_name = config['TASK_NAME02']
    number_id_job = '2'
    date_process = config['PROCESS_START_DATE']      #[0] start [1] end
    status = config['STATUS_RUN']                        #[0]R      [1]A      [2]S
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status)

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
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status)
def CAC12CACD_AP01_MPI_DER_CACD():
    try:
        #read config files
        config = function.read_config_psql('P1_DER_CACD')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

    #update batch
    task_name = config['TASK_NAME03']
    number_id_job = '3'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status)

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
        function.insert_table_into_postgresql(table,query,int(config['CHUNKSIZE']))
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("cannot insert data to PostgreSQL Line", exc_tb.tb_lineno)

    #update batch    
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status)
def CAC12CACD_VALD01():
    # query data with condition RDT vilidation and return Account Id where not True 
    try:
        #read config files
        config = function.read_config_psql('P1_DER_CACD')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

    #update batch    
    task_name = config['TASK_NAME04']
    number_id_job = '4'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status)

    conn_psql = function.create_connection_psql()
    bucket_name = config['BUCKET_NAME']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')
    chunksize = int(config['CHUNKSIZE'])
    name = config['FILE_NAME04']

    #update batch case fail    
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    try:
        #uery data with condition RDT vilidation and return Account Id where not True 
        #validation rule get report column Account Id

        #[Account Open Date] must be after or the same as [Contract Date] in Credit Account.
        result_CNCACD001 = pd.read_sql(config['RDT010203'] ,conn_psql) 

        #If [Loan and Contingent Type] in Credit Account (DER_CAC) is classified as “Contingent loans”, then both [Notional Contingent Amount in 
        #Original Currency] and [Notional Contingent Amount in Baht] cannot be blank and must be greater than 0.
        result_CMCACD002 = pd.read_sql(config['RDT010204'] ,conn_psql) 

        #If [Account Term Type] in Credit Account is classified as “Fixed Term”, [Loan and Contingent Type] in Credit Account is not classified as “Contingent loans”, 
        #[Revolving Flag] is 0 and both [Principal Payment Frequency Unit] and [Interest Payment Frequency Unit] in Credit Account is not “Unknown Term Unit”,
        #then [First Payment Amount in Original Currency] cannot be blank and must be greater than 0.   
        result_CMCACD003 = pd.read_sql(config['RDT010205'] ,conn_psql)     

        #If [Account Purpose] is classified as “commercial purposes”, then [Lending Business Type] cannot be blank. 
        #If [Account Purpose] is classified as “individual consumption”, then [Lending Business Type] must be blank.
        result_CMCACD004 = pd.read_sql(config['RDT010206'] ,conn_psql) 

        #If [Loan and Contingent Type] is not classified as “Contingent loans”, then [Revolving Flag], [Refinance Flag], [Employee Loan Flag] and [Related Lending Flag] cannot be blank
        #as well as [Original Effective Interest Rate] cannot be blank and must be greater than or equal to 0 and less than or equal to 100.  
        result_CMCACD005 = pd.read_sql(config['RDT010207'] ,conn_psql) 

        #If [Loan and Contingent Type] is not classified as “Contingent loans” and [Account Purpose] is classified as “for consumption”, 
        #then [Digital Loan Flag] cannot be blank.     
        result_CMCACD006 = pd.read_sql(config['RDT010208'] ,conn_psql)    

        #If [Loan and Contingent Type] is not classified as “Contingent loans”  and [Account Purpose] is classified as “for commercial”, 
        #then [Bridge Loan Flag] and [Country to Invest] cannot be blank.     
        result_CMCACD007 = pd.read_sql(config['RDT010209'] ,conn_psql) 

        #If [Loan and Contingent Type] is not classified as “Contingent loans” and [Account Purpose] is classified 
        #as “for commercial” or [Account Purpose] is “for others”, then [Interbank Flag] cannot be blank.    
        result_CMCACD008 = pd.read_sql(config['RDT010210'] ,conn_psql) 

        #If [Product Loan Type Under Regulate] is not blank, then [Total Interest and Fee Rate] cannot be blank and must be between 0 and 100 
        #and [Factor for Consideration] cannot be blank.     
        result_CMCACD009 = pd.read_sql(config['RDT010211'] ,conn_psql) 

        #If a credit account detail record exists, then a credit account record for the credit account detail must exist. 
        result_RICACD001 = pd.read_sql(config['RDT010212'] ,conn_psql) 

        #If a credit line id in a credit account record is specified, then a credit line record must exist.    
        result_RICACD002 = pd.read_sql(config['RDT010213'] ,conn_psql)   

        #If a credit account detail record exists, then a portfolio record must exist.    
        result_RICACD003 = pd.read_sql(config['RDT010214'] ,conn_psql) 

        #If a product program id in a credit account record is specified, then a product program record must exist. 
        result_RICACD004 = pd.read_sql(config['RDT010215'] ,conn_psql) 

        conn_psql.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection PostgreSQL fail Line", exc_tb.tb_lineno)

    #validation success
    if (result_CNCACD001['Account Id'].count() == 0 and
        result_CMCACD002['Account Id'].count() == 0 and
        result_CMCACD003['Account Id'].count() == 0 and
        result_CMCACD004['Account Id'].count() == 0 and
        result_CMCACD005['Account Id'].count() == 0 and
        result_CMCACD006['Account Id'].count() == 0 and
        result_CMCACD007['Account Id'].count() == 0 and
        result_CMCACD008['Account Id'].count() == 0 and
        result_CMCACD009['Account Id'].count() == 0 and
        result_RICACD001['Account Id'].count() == 0 and
        result_RICACD002['Account Id'].count() == 0 and
        result_RICACD003['Account Id'].count() == 0 and
        result_RICACD004['Account Id'].count() == 0 ):

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
            if result_CNCACD001['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CNCACD001,'{0}{1}/CAC12CACD_CNCACD001.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC12CACD_CNCACD001.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMCACD002['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMCACD002,'{0}{1}/CAC12CACD_CMCACD002.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC12CACD_CMCACD002.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMCACD003['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMCACD003,'{0}{1}/CAC12CACD_CMCACD003.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC12CACD_CMCACD003.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMCACD004['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMCACD004,'{0}{1}/CAC12CACD_CMCACD004.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC12CACD_CMCACD004.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMCACD005['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMCACD005,'{0}{1}/CAC12CACD_CMCACD005.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC12CACD_CMCACD005.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMCACD006['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMCACD006,'{0}{1}/CAC12CACD_CMCACD006.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC12CACD_CMCACD006.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMCACD007['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMCACD007,'{0}{1}/CAC12CACD_CMCACD007.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC12CACD_CMCACD007.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMCACD008['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMCACD008,'{0}{1}/CAC12CACD_CMCACD008.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC12CACD_CMCACD008.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMCACD009['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMCACD009,'{0}{1}/CAC12CACD_CMCACD009.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC12CACD_CMCACD009.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_RICACD001['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_RICACD001,'{0}{1}/CAC12CACD_RICACD001.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC12CACD_RICACD001.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_RICACD002['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_RICACD002,'{0}{1}/CAC12CACD_RICACD002.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC12CACD_RICACD002.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_RICACD003['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_RICACD003,'{0}{1}/CAC12CACD_RICACD003.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC12CACD_RICACD003.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_RICACD004['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_RICACD004,'{0}{1}/CAC12CACD_RICACD004.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC12CACD_RICACD004.csv fail Line", exc_tb.tb_lineno)

    #update batch   
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status)  

def CAC12CACD_GEN01(): 
    try:
        #read config files
        config = function.read_config_psql('P1_DER_CACD')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

    #update batch  
    task_name = config['TASK_NAME05']
    number_id_job = '5'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status)

    #parameter
    bucket_name = config['BUCKET_NAME']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')
    msr_prd_id = config['MSR_PRD_ID']

    #check folder validation in minio
    file_success = config['FILE_SUCCESS']
    list_folder = [config['LOAD_PATH'],config['FXRM_PATH'],config['VALD_PATH'],file_success]

    #status batch case fail
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    try:
        #gen files
        df = function.read_table_psql(config['RDT010216'])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch   
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection PSQL fail Line", exc_tb.tb_lineno)

    try:
        #load table from PostgreSQL and upload file .csv to minio   
        path = '{0}{1}.csv'.format(config['GEN_PATH'],msr_prd_id)
        function.upload_csv_to_minio(df,path, int(config['CHUNKSIZE']),s3,bucket_name)
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
                    msr_prd_id = function.update_msr_prd_id_monthly(msr_prd_id)
                    list_parm_values = [f'{msr_prd_id}']
                    list_parm_nm = ['MSR_PRD_ID']
                    function.update_config_psql(list_parm_values, list_parm_nm, config['RDT600003'], "P1_"+config['TABLE'].split(".")[1])
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
    'start_date': datetime(2021, 2, 17),
    'schedule_interval': None,
}
with DAG('P1_MONTHLY_CAC_12_DER_CACD',
         schedule_interval='@monthly', 
         default_args=default_args,
         description='Monthly: 1.2 Credit Account Detail',
         catchup=False) as dag:
 
    task1 = PythonOperator(
        task_id='CAC12CACD_LOAD01',
        python_callable=CAC12CACD_LOAD01
    )
    
    task2 = PythonOperator(
        task_id='CAC12CACD_XFRM01',
        python_callable=CAC12CACD_XFRM01
    )

    task3 = PythonOperator(
        task_id='CAC12CACD_AP01_MPI_DER_CACD',
        python_callable=CAC12CACD_AP01_MPI_DER_CACD    
    )

    task4 = PythonOperator(
        task_id='CAC12CACD_VALD01',
        python_callable=CAC12CACD_VALD01      
    ) 

    task5 = PythonOperator(
        task_id='CAC12CACD_GEN01',
        python_callable=CAC12CACD_GEN01      
    ) 

    task1 >> task2 >> task3 >> task4 >> task5 
