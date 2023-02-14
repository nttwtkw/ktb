from datetime import datetime
import pandas as pd
import numpy as np
import vaex
import glob
import sys
from common_function_2 import function
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator

#table batch 1.rdtdba.btch_job_test 2.rdtdba.btch_shd_test

def CMM74OTDD_LOAD01():
    try:
        #read config files
        config = function.read_config_psql('P1_DER_OTDD_D')
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
    # file_name = config['FILE_NAME01']

    #batch status
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    path_minio = config['LOAD_PATH']
    sftp_name = config['SFTP_NAME']

    try:
        #connect to sftp and download table files
        function.download_file_from_sftp_daily(bucket, path_minio, config['SFTP_PATH'], sftp_name, config['MSR_PRD_ID'])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException('Connection sftp fail, cannot download file from sftp Line', exc_tb.tb_lineno)

    #create list file table name
    filter_file_name = (sftp_name+'*'+'.TXT'+'*')
    table_files = glob.glob(filter_file_name)

    #define columns name, convert .txt to .parquet
    columns_name = ['org_id','data_dt','ac_id','ccy','otsnd_amt_in_orig_ccy']

    #convert .txt to parquet file upload to minio
    table_data = [pd.read_csv(f,sep="|",names=columns_name, dtype={'org_id': 'str'}) for f in table_files]

    try:
        #upload files to minio
        function.upload_files_from_sftp_to_minio(table_data,table_files,bucket,path_minio)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException('Connection db2 fail,cannot upload files to minio Line', exc_tb.tb_lineno)

    #update batch     
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status)

def CMM74OTDD_XFRM01():
    try:
        #read config files
        config = function.read_config_psql('P1_DER_OTDD_D')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    #update batch  
    task_name = config['TASK_NAME02']
    number_id_job = '2'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']                     
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

    #create columns
    df['msr_prd_id'] = np.repeat(config['MSR_PRD_ID'], df.shape[0])
    df['ppn_tm'] = np.repeat(function.data_dt('timestamp'), df.shape[0])

    #select columns
    df_new = df[['msr_prd_id','org_id','data_dt','ac_id','ccy','otsnd_amt_in_orig_ccy','ppn_tm']]

    #fill null dataframe
    df_new['msr_prd_id'] = df_new['msr_prd_id'].fillna(value=config['MSR_PRD_ID'])
    df_new['org_id'] = df_new['org_id'].fillna(value='006')
    df_new['src_stm'] = np.repeat('SFTP', df.shape[0])

    #upload table to minio
    name = config['FILE_NAME02']
    df_new.export_many(name+'_{i:03}.parquet',chunk_size=int(config['CHUNKSIZE']))                

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

def CMM74OTDD_AP01_DPI_DER_OTDD():
    try:
        #read config files
        config = function.read_config_psql('P1_DER_OTDD_D')
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

def CMM74OTDD_VALD01():
    # query data with condition RDT vilidation and return Account Id where not True 
    try:
        #read config files
        config = function.read_config_psql('P1_DER_OTDD_D')
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

        #If a credit account is Thai Baht loans, credit card loans, contingent loans or loans from an offshore branch of the Thai commercial bank,
        #then an outstanding daily record of this credit account is not required to report.
        result_CNOTDD001 = pd.read_sql(config['RDT070401'] ,conn_psql) 

        #[Outstanding Amount in Original Currency] must be greater than or equal to 0
        result_CNOTDD002 = pd.read_sql(config['RDT070402'] ,conn_psql) 

        #If a credit account is inactive or closed, then an outstanding daily record of this credit account is not required to report.
        result_CNOTDD003 = pd.read_sql(config['RDT070403'] ,conn_psql)     

        #Sum up of [Outstanding Amount in Original Currency] must be greater than or equal to Sum up of [Foreign Currency Amount] in 
        #Foreign Currency Position (DS_FCP) with conditions of on-shore lending.
        result_CMOTDD001 = pd.read_sql(config['RDT070404'] ,conn_psql) 

        #Sum up of [Outstanding Amount in Original Currency] must be greater than or equal to Sum up of [Foreign Currency Amount] in
        #Foreign Currency Position (DS_FCP) with conditions of off-shore lending.  
        result_CMOTDD002 = pd.read_sql(config['RDT070405'] ,conn_psql) 

        #Sum up of [Outstanding Amount in Original Currency] must be greater than or equal to Sum up of [Foreign Currency Amount] in
        #Foreign Currency Position (DS_FCP) with conditions of overdraft from deposit account.   
        result_CMOTDD003 = pd.read_sql(config['RDT070406'] ,conn_psql)    

        #Sum up of [Outstanding Amount in Original Currency] must be greater than or equal to Sum up of [Foreign Currency Amount] in
        #Foreign Currency Position (DS_FCP) with conditions of foreign bill inward negotiated     
        result_CMOTDD004 = pd.read_sql(config['RDT070407'] ,conn_psql) 

        #Sum up of [Outstanding Amount in Original Currency] must be greater than or equal to Sum up of [Foreign Currency Amount] in
        #Foreign Currency Position (DS_FCP) with conditions of foreign bill outward negotiated.
        result_CMOTDD005 = pd.read_sql(config['RDT070408'] ,conn_psql) 

        #If an outstanding daily record exists, then a credit account record must exist.    
        result_RIOTDD001 = pd.read_sql(config['RDT070409'] ,conn_psql) 

        conn_psql.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection PostgreSQL fail Line", exc_tb.tb_lineno)

    #validation success
    if (result_CNOTDD001['Account Id'].count() == 0 and
        result_CNOTDD002['Account Id'].count() == 0 and
        result_CNOTDD003['Account Id'].count() == 0 and
        result_CMOTDD001['Account Id'].count() == 0 and
        result_CMOTDD002['Account Id'].count() == 0 and
        result_CMOTDD003['Account Id'].count() == 0 and
        result_CMOTDD004['Account Id'].count() == 0 and
        result_CMOTDD005['Account Id'].count() == 0 and
        result_RIOTDD001['Account Id'].count() == 0 ):

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
            if result_CNOTDD001['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CNOTDD001,'{0}{1}/CMM74OTDD_CNOTDD001.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CMM74OTDD_CNOTDD001.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CNOTDD002['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CNOTDD002,'{0}{1}/CMM74OTDD_CNOTDD002.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CMM74OTDD_CNOTDD002.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CNOTDD003['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CNOTDD003,'{0}{1}/CMM74OTDD_CNOTDD003.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CMM74OTDD_CNOTDD003.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMOTDD001['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMOTDD001,'{0}{1}/CMM74OTDD_CMOTDD001.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CMM74OTDD_CMOTDD001.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMOTDD002['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMOTDD002,'{0}{1}/CMM74OTDD_CMOTDD002.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CMM74OTDD_CMOTDD002.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMOTDD003['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMOTDD003,'{0}{1}/CMM74OTDD_CMOTDD003.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CMM74OTDD_CMOTDD003.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMOTDD004['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMOTDD004,'{0}{1}/CMM74OTDD_CMOTDD004.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CMM74OTDD_CMOTDD004.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMOTDD005['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMOTDD005,'{0}{1}/CMM74OTDD_CMOTDD005.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CMM74OTDD_CMOTDD005.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_RIOTDD001['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_RIOTDD001,'{0}{1}/CMM74OTDD_RIOTDD001.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CMM74OTDD_RIOTDD001.csv fail Line", exc_tb.tb_lineno)

    #update batch   
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status)  

def CMM74OTDD_GEN01(): 
    try:
        #read config files
        config = function.read_config_psql('P1_DER_OTDD_D')
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
        df = function.read_table_psql(config['RDT070410'])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch   
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection PSQL fail Line", exc_tb.tb_lineno)

    try:
        #load table from PostgreSQL and upload file .csv to minio   
        path = '{0}{1}.csv'.format(config['GEN_PATH'],msr_prd_id)
        function.upload_csv_to_minio_2(df,path, int(config['CHUNKSIZE']),s3,bucket_name, config['TABLE'].split(".")[-1])
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
with DAG('P1_DAILY_CMM_74_DER_OTDD',
         schedule_interval='@daily', 
         default_args=default_args,
         description='Daily: 7.4 Outstanding Daily',
         catchup=False) as dag:
 
    task1 = PythonOperator(
        task_id='CMM74OTDD_LOAD01',
        python_callable=CMM74OTDD_LOAD01
    )
    
    task2 = PythonOperator(
        task_id='CMM74OTDD_XFRM01',
        python_callable=CMM74OTDD_XFRM01
    )

    task3 = PythonOperator(
        task_id='CMM74OTDD_AP01_DPI_DER_OTDD',
        python_callable=CMM74OTDD_AP01_DPI_DER_OTDD    
    )

    task4 = PythonOperator(
        task_id='CMM74OTDD_VALD01',
        python_callable=CMM74OTDD_VALD01      
    ) 

    task5 = PythonOperator(
        task_id='CMM74OTDD_GEN01',
        python_callable=CMM74OTDD_GEN01      
    ) 

    task1 >> task2 >> task3 >> task4 >> task5 
