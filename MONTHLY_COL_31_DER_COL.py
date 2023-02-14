from datetime import datetime
import pandas as pd
import numpy as np
import vaex
from common_function_2 import function
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator

def COL31COL_LOAD01():
    try:
        #read config files
        config = function.read_config_psql('der_col')
    except Exception as e:
        print(e)
        print('Read config file fail Line 13')    
        raise AirflowFailException("Read config file fail Line 13")

    #update batch     
    task_name = config['task_name'].split(",")[0]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)

    #connect minio ex: download file , upload parquet to minio type file
    bucket = function.create_connection_minio(config['bucket_name']) 
    file_name = config['file_name01']

    #batch status
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][1]                         #[0]R      [1]A      [2]S

    try:
        #read table,convert dataframe to .parquet and upload to minio
        query = function.replace_value(config['RDT030101'], ['{msr_prd_id}'], [config['MSR_PRD_ID']])
        df,con_db2 = function.read_db2(query)
        function.df_to_parquet(df,file_name)
        con_db2.close()
    except Exception as e:
        print(e)
        print('Connection db2 fail Line 37')

        #update batch  
        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection db2 fail Line 37")

    try:
        #upload to minio
        function.upload_files_to_minio(file_name,config['load_path'],bucket)
    except Exception as e:
        print(e)
        print('Cannot upload files to minio Line 50')

        #update batch  
        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
        raise AirflowFailException("Cannot upload files to minio Line 50")

    #update batch     
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status)

def COL31COL_XFRM01():
    try:
        #read config files
        config = function.read_config_psql('der_col')
    except Exception as e:
        print(e)
        print('Read config file fail Line 67')    
        raise AirflowFailException("Read config file fail Line 67")

    #update batch  
    task_name = config['task_name'].split(",")[1]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status)

    #update batch case fail
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][1]                         #[0]R      [1]A      [2]S

    try:
        #download table from minio
        bucket = function.create_connection_minio(config['bucket_name'])
        function.download_parquet_from_minio(config['load_path'],bucket)

    except Exception as e:
        print(e)
        print('Connect minio fail, cannot download file from minio Line 87')

        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True) 

        raise AirflowFailException("Connect minio fail, cannot download file from minio Line 87")

    #vaex read data
    df = vaex.open('{0}*.parquet'.format(config['file_name01']))
    
    #upload table to minio
    name = config['file_name02']
    df.export_many(name+'_{i:03}.parquet',chunk_size=int(config['chunksize']))                

    try:
        function.upload_files_to_minio(name,config['fxrm_path'],bucket)

    except Exception as e:
        print(e)
        print('Cannot upload file to minio Line 105')

        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)

        raise AirflowFailException("Cannot upload file to minio Line 105")

    #update batch 
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status)

def COL31COL_AP01_API_DER_COL():
    try:
        #read config files
        config = function.read_config_psql('der_col')
    except Exception as e:
        print(e)
        print('Read config file fail Line 123')    
        raise AirflowFailException("Read config file fail Line 123")

    #update batch
    task_name = config['task_name'].split(",")[2]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status)

    #update batch case fail
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][1]                         #[0]R      [1]A      [2]S

    try:
        #download table from minio
        bucket = function.create_connection_minio(config['bucket_name'])
        function.download_parquet_from_minio(config['fxrm_path'],bucket)

    except Exception as e:
        print(e)
        print('Connect minio fail, cannot download file from minio Line 143')

        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)

        raise AirflowFailException("Connect minio fail, cannot download file from minio Line 143")

    #parameter
    table = config['table']
    msr_prd_id = config['MSR_PRD_ID']

    try:
        #delete data in table before insert
        function.delete_data_in_psql_filter_by_msr_prd_id(table,msr_prd_id,config['RDT04'])
    except Exception as e:
        print(e)
        print('cannot delete data to PostgreSQL Line 159')

        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)

        raise AirflowFailException("cannot delete data to PostgreSQL Line 159")  

    #insert table to PostgreSQL 
    query = function.query_insert(table)
    table = function.read_table_local(config['file_name03'])

    try:
        query_psql = function.replace_value(config['RDT030102'], ['{msr_prd_id}'], [msr_prd_id])
        table_psql = function.read_table_psql(query_psql)
        table_psql = function.change_type(table_psql,config['pk'],config['pk_type'])
        result = function.check_duplicate(table,table_psql,config['pk'])
    except Exception as e:
        print(e)
        print('Connection PostgreSQL fail Line 174')

        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)

        raise AirflowFailException("Connection PostgreSQL fail Line 174")

    try:
        function.insert_table_into_postgresql(result,query,int(config['chunksize']),config['pk'].split(',')[0])
    except Exception as e:
        print(e)
        print('cannot insert data to PostgreSQL Line 186')

        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)

        raise AirflowFailException("cannot insert data to PostgreSQL Line 186")

    #update batch    
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status)

def COL31COL_VALD01():
    # query data with condition RDT vilidation and return Counterparty Id where not True 
    try:
        #read config files
        config = function.read_config_psql('der_col')
    except Exception as e:
        print(e)
        print('Read config file fail Line 204')    
        raise AirflowFailException("Read config file fail Line 204")

    #update batch    
    task_name = config['task_name'].split(",")[1]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status)

    conn_psql = function.create_connection_psql()
    bucket_name = config['bucket_name']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')
    chunksize = int(config['chunksize'])
    name = config['file_name04']

    #update batch case fail    
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][1]                         #[0]R      [1]A      [2]S

    try:
        #query data with condition RDT vilidation and return Collateral Id where not True 
        #validation rule get report column Collateral Id
        
        #If [Country] in Land (DER_LND) or Building (DER_BLD) is ‘TH’, then [Collateral Registration Flag] cannot be blank.
        query = function.replace_value(config['RDT030104'], ['{msr_prd_id}'], [config['MSR_PRD_ID']])
        result_CMCOL001 = pd.read_sql(query ,conn_psql) 

        conn_psql.close()

    except Exception as e:
        print(e)
        print('Connection PostgreSQL fail Line 233')

        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)

        raise AirflowFailException("Connection PostgreSQL fail Line 233")

    #validation success
    if (result_CMCOL001['Collateral ID'].count() == 0 ):

        try:
            files = config['succuess_path']
            function.create_file_trigger_success_to_minio(bucket, files)
        except Exception as e:
            print(e)
            print('function create_file_trigger_success_to_minio fail Line 251')    
            function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
            raise AirflowFailException("function create_file_trigger_success_to_minio fail Line 251")

        print('success')
    else:
        unixtime = str(int(datetime.now().timestamp()))

        try:
            if result_CMCOL001['Collateral ID'].count() != 0:
                function.upload_csv_to_minio(result_CMCOL001,'{0}{1}/COL31COL_CMCOL001.csv'.format(config['vald_path'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            print(e)
            print('upload_csv_to_minio COL31COL_CMCOL001.csv fail Line 264')    
            function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio COL31COL_CMCOL001.csv fail Line 264")

    #update batch   
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status)

def COL31COL_GEN01():
    try:
        #read config files
        config = function.read_config_psql('der_col')
    except Exception as e:
        print(e)
        print('Read config file fail Line 279')    
        raise AirflowFailException("Read config file fail Line 279")

    #update batch  
    task_name = config['task_name'].split(",")[2]
    number_id_job = str(function.turn_tasks_into_numbers(config['task_name'],task_name))
    date_process = config['date_process'].split(",")[0]       #[0] start [1] end
    status = config['status_btch'][0]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status)

    #parameter
    bucket_name = config['bucket_name']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')
    msr_prd_id = config['MSR_PRD_ID']

    #check folder validation in minio
    file_success = config['succuess_path']
    list_folder = [config['load_path'],config['fxrm_path'],config['vald_path'],file_success]

    #status batch case fail
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][1]                         #[0]R      [1]A      [2]S
    try:
        for object in bucket.objects.filter(Prefix = file_success):
            check_validation_fail = object.key
            if check_validation_fail == file_success:

                try:
                    #gen files
                    query = function.replace_value(config['RDT030109'], ['{msr_prd_id}'], [config['MSR_PRD_ID']])
                    df = function.read_table_psql(query)

                except Exception as e:
                    print(e)
                    print('Connection PSQL fail Line 313')

                    #update batch   
                    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
                    raise AirflowFailException("Connection PSQL fail Line 313")

                try:
                    #load table from PostgreSQL and upload file .csv to minio   
                    path = '{0}{1}.csv'.format(config['gen_path'],msr_prd_id)

                    function.upload_csv_to_minio(df,path, int(config['chunksize']),s3,bucket_name)

                except Exception as e:
                    print(e)
                    print('Upload files to minio fail Line 327')

                    #update batch   
                    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
                    raise AirflowFailException("Upload files to minio fail Line 327")

                try:
                    #delete load, transform, validation, success.txt
                    function.delete_folder_in_minio_when_success(bucket,list_folder)

                except Exception as e:
                    print(e)
                    print('Delete folder fail Line 339')

                    #update batch   
                    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
                    raise AirflowFailException("Delete folder fail Line 339")                

                try:
                    #update MSR_PRD_ID
                    msr_prd_id = function.update_msr_prd_id_monthly(config['MSR_PRD_ID'])
                    list_parm_values = [f'{msr_prd_id}']
                    list_parm_nm = ['MSR_PRD_ID']
                    function.update_config_psql(list_parm_values, list_parm_nm, config['RDT03'], config['table'].split(".")[1])
                except Exception as e:
                    print(e)
                    print('update MSR_PRD_ID fail Line 351')

                    #update batch   
                    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
                    raise AirflowFailException("update MSR_PRD_ID fail Line 351")

            else:
                pass
    except Exception as e:
        print(e)
        print('Connection minio fail Line 306')

        #update batch   
        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection minio fail Line 306")

    #update batch    
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 12, 31),
    'schedule_interval': None,
}
with DAG('MONTHLY_COL_31_DER_COL',
         schedule_interval='@monthly',
         default_args=default_args,
         description='Monthly: 3.1 Collateral',
         catchup=False) as dag:
 
    task1 = PythonOperator(
        task_id='COL31COL_LOAD01',
        python_callable=COL31COL_LOAD01
    )

    task2 = PythonOperator(
        task_id='COL31COL_XFRM01',
        python_callable=COL31COL_XFRM01
    )

    task3 = PythonOperator(
        task_id='COL31COL_AP01_API_DER_COL',
        python_callable=COL31COL_AP01_API_DER_COL
    )

    task4 = PythonOperator(
        task_id='COL31COL_VALD01',
        python_callable=COL31COL_VALD01
    ) 

    task5 = PythonOperator(
        task_id='COL31COL_GEN01',
        python_callable=COL31COL_GEN01
    )

    task1 >> task2 >> task3 >> task4 >> task5
