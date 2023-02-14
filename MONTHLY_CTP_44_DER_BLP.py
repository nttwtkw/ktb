from datetime import datetime
import pandas as pd
import numpy as np
import vaex
import sys
from common_function_2 import function
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator

def CTP44BLP_LOAD01():
    try:
        #read config files
        config = function.read_config_psql('DER_BLP')
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

    try:
        #read table,convert dataframe to .parquet and upload to minio
        df,con_db2 = function.read_db2(config['RDT040401'])
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

def CTP44BLP_XFRM01():
    try:
        #read config files
        config = function.read_config_psql('DER_BLP')
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

def CTP44BLP_AP01_API_DER_BLP():
    try:
        #read config files
        config = function.read_config_psql('DER_BLP')
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

def CTP44BLP_VALD01():
    try:
        #read config files
        config = function.read_config_psql('DER_BLP')
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
    
    #---------------------------------------------------------------------------------------------------------------------------------------------------
    config_2 = config['RDT040402']
    config_3 = config['RDT040403']
    config_4 = config['RDT040404']
    config_5 = config['RDT040405']
    config_6 = config['RDT040406']
    config_7 = config['RDT040407']
    #---------------------------------------------------------------------------------------------------------------------------------------------------
    validation_rule1 = 'CNBLP001'
    validation_rule2 = 'CNBLP002'
    validation_rule3 = 'CMBLP001'
    validaiton_rule4 = 'CMBLP002'
    validation_rule5 = 'CMBLP003'
    validation_rule6 = 'RIBLP001'
    
    class res:
        res_name = 'result_' + f'{validation_rule1}' 
        res_query1 = None
        res_query2 = None
        res_query3 = None
        res_query4 = None
        res_query5 = None
        res_query6 = None

    try:
        #Query data with condition RDT vilidation and return Account Id where not True 
        #validation rule get report column Account Id

        #[Labor] must be greater than or equal to 0
        res.res_query1 = pd.read_sql(config_2,conn_psql)

        #[Domestic Income in Baht] must be greater than or equal to 0
        res.res_query2 = pd.read_sql(config_3,conn_psql)

        #If account purpose of the credit account is for commercial purposes (exclude interbank loan), then a business loan profile is required to report.
        res.res_query3 = pd.read_sql(config_4,conn_psql)

        #If [Main Factory Country] is ‘TH’, then [Main Factory Location] cannot be blank.
        res.res_query4 = pd.read_sql(config_5,conn_psql)

        #If [Domestic Income in Baht] is greater than 0, then [Export Income in Baht] can be blank or must be greater than or equal to 0, otherwise [Export Income in Baht] cannot be 0 and must be greater than 0
        res.res_query5 = pd.read_sql(config_6,conn_psql)

        #If a business loan profile record exists, then a counterparty x id record for the business loan profile must exist.
        res.res_query6 = pd.read_sql(config_7,conn_psql)

    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("function create_file_trigger_success_to_minio fail Line", exc_tb.tb_lineno)

    #validation success
    if (res.res_query1.count()[0] == 0
       and res.res_query2.count()[0] == 0
       and res.res_query3.count()[0] == 0
       and res.res_query4.count()[0] == 0
       and res.res_query5.count()[0] == 0
       and res.res_query6.count()[0] == 0):

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
            if res.res_query1.count()[0] != 0:
                function.upload_csv_to_minio(res.res_query1, ('{0}{1}/CTP44BLP_' + f'{validation_rule1}' + '.csv').format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
                print(f'{validation_rule1}' +  ' PASSED')
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException('upload_csv_to_minio CTP44BLP_' + f'{validaiton_rule1}' + '.csv fail Line', exc_tb.tb_lineno)

        try:
            if res.res_query2.count()[0] != 0:
                function.upload_csv_to_minio(res.res_query2, ('{0}{1}/CTP44BLP_' + f'{validation_rule2}' + '.csv').format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
                print(f'{validation_rule2}' +  ' PASSED')                             
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException('upload_csv_to_minio CTP44BLP_' + f'{validaiton_rule2}' + '.csv fail Line', exc_tb.tb_lineno)

        try:
            if res.res_query3.count()[0] != 0:
                function.upload_csv_to_minio(res.res_query3, ('{0}{1}/CTP44BLP_' + f'{validation_rule3}' + '.csv').format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
                print(f'{validation_rule3}' +  ' PASSED')
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException('upload_csv_to_minio CTP44BLP_' + f'{validaiton_rule3}' + '.csv fail Line', exc_tb.tb_lineno)

        try:
            if res.res_query4.count()[0] != 0:
                function.upload_csv_to_minio(res.res_query4, ('{0}{1}/CTP44BLP_' + f'{validation_rule4}' + '.csv').format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
                print(f'{validation_rule4}' +  ' PASSED')
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException('upload_csv_to_minio CTP44BLP_' + f'{validaiton_rule4}' + '.csv fail Line', exc_tb.tb_lineno)

        try:
            if res.res_query5.count()[0] != 0:
                function.upload_csv_to_minio(res.res_query5, ('{0}{1}/CTP44BLP_' + f'{validation_rule5}' + '.csv').format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
                print(f'{validation_rule5}' +  ' PASSED')
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException('upload_csv_to_minio CTP44BLP_' + f'{validaiton_rule5}' + '.csv fail Line', exc_tb.tb_lineno)

        try:
            if res.res_query6.count()[0] != 0:
                function.upload_csv_to_minio(res.res_query6, ('{0}{1}/CTP44BLP_' + f'{validation_rule6}' + '.csv').format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
                print(f'{validation_rule6}' +  ' PASSED')
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException('upload_csv_to_minio CTP44BLP_' + f'{validaiton_rule6}' + '.csv fail Line', exc_tb.tb_lineno)

    #update batch   
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status)
        
def CTP44BLP_GEN01():
    try:
        #read config files
        config = function.read_config_psql('DER_BLP')
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
        df = function.read_table_psql(config['RDT040408'])
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
                    msr_prd_id = function.update_msr_prd_id_daily(msr_prd_id)
                    list_parm_values = [f'{msr_prd_id}']
                    list_parm_nm = ['MSR_PRD_ID']
                    function.update_config_psql(list_parm_values, list_parm_nm, config['RDT600003'], config['TABLE'].split(".")[1])
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
    'start_date': datetime(2021, 12, 31),
    'schedule_interval': None,
}
with DAG('MONTHLY_CTP_44_DER_BLP',
         schedule_interval='@monthly',
         default_args=default_args,
         description='Monthly: 4.4 Business Loan Profile',
         catchup=False) as dag:
 
    task1 = PythonOperator(
        task_id='CTP44BLP_LOAD01',
        python_callable=CTP44BLP_LOAD01
    )

    task2 = PythonOperator(
        task_id='CTP44BLP_XFRM01',
        python_callable=CTP44BLP_XFRM01
    )

    task3 = PythonOperator(
        task_id='CTP44BLP_AP01_API_DER_BLP',
        python_callable=CTP44BLP_AP01_API_DER_BLP
    )

    task4 = PythonOperator(
        task_id='CTP44BLP_VALD01',
        python_callable=CTP44BLP_VALD01
    ) 

    task5 = PythonOperator(
        task_id='CTP44BLP_GEN01',
        python_callable=CTP44BLP_GEN01
    )

    task1 >> task2 >> task3 >> task4 >> task5