from datetime import datetime
import pandas as pd
import numpy as np
import vaex
import sys
from common_function_2 import function
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator

#table batch 1.rdtdba.rdt_btch_job 2.rdtdba.rdt_btch_shd

def CTP46RTR_LOAD01():
    try:
        #read config files
        config = function.read_config_psql('der_rtr')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

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
        df,con_db2 = function.read_db2(config['RDT040601'])
        function.df_to_parquet(df,file_name)
        con_db2.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
        raise AirflowFailException('Connection db2 fail Line', exc_tb.tb_lineno)

    try:
        #upload to minio
        function.upload_files_to_minio(file_name,config['load_path'],bucket)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
        raise AirflowFailException('Cannot upload files to minio Line', exc_tb.tb_lineno)

    #update batch     
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status)
    
    
def CTP46RTR_XFRM01():
    try:
        #read config files
        config = function.read_config_psql('der_rtr')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

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
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True) 
        raise AirflowFailException('Connect minio fail, cannot download file from minio Line', exc_tb.tb_lineno)

    #vaex read data
    df = vaex.open('{0}*.parquet'.format(config['file_name01']))

    #upload table to minio
    name = config['file_name02']
    df.export_many(name+'_{i:03}.parquet',chunk_size=int(config['chunksize']))                

    try:
        function.upload_files_to_minio(name,config['fxrm_path'],bucket)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
        raise AirflowFailException('Cannot upload file to minio Line' ,exc_tb.tb_lineno)

    #update batch 
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status)


def CTP46RTR_AP01_MPI_DER_RTR():
    try:
        #read config files
        config = function.read_config_psql('der_rtr')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

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
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connect minio fail, cannot download file from minio Line", exc_tb.tb_lineno)

    #parameter
    table = config['table']
    msr_prd_id = config['MSR_PRD_ID']

    try:
        #delete data in table before insert
        function.delete_data_in_psql_filter_by_msr_prd_id(table,msr_prd_id,config['RDT04'])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
        raise AirflowFailException("cannot delete data to PostgreSQL Line", exc_tb.tb_lineno)  

    #insert table to PostgreSQL 
    query = function.query_insert(table)
    table = function.read_table_local(config['file_name03'])

    try:
        table_psql = function.read_table_psql(config['RDT040602'])
        table_psql = function.change_type(table_psql,config['pk'],config['pk_type'])
        result = function.check_duplicate(table,table_psql,config['pk'])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection PostgreSQL fail Line", exc_tb.tb_lineno)

    try:
        function.insert_table_into_postgresql(result,query,int(config['chunksize']),config['pk'].split(',')[0])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
        raise AirflowFailException("cannot insert data to PostgreSQL Line", exc_tb.tb_lineno)

    #update batch    
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status)


def CTP46RTR_VALD01():
    # query data with condition RDT vilidation and return Counterparty Id where not True 
    try:
        #read config files
        config = function.read_config_psql('der_rtr')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

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
        #uery data with condition RDT vilidation and return Counterparty Id where not True 
        #validation rule get report column Counterparty Id

        #If a counterparty is related with reporter, then a relationship to reporter must be reported.
        query = function.replace_value(config['RDT040603'], ['{msr_prd_id}'], [config['MSR_PRD_ID']])
        result_CMRTR001 = pd.read_sql(query ,conn_psql) 

        #If a relationship to reporter record exists, then a counterparty x id record must exist.
        query = function.replace_value(config['RDT040604'], ['{msr_prd_id}'], [config['MSR_PRD_ID']])
        result_RIRTR001 = pd.read_sql(query ,conn_psql) 

        conn_psql.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection PostgreSQL fail Line", exc_tb.tb_lineno)

    #validation success
    if (result_CMRTR001['Counterparty Id'].count() == 0 and
        result_RIRTR001['Counterparty Id'].count() == 0 ):

        try:
            files = config['succuess_path']
            function.create_file_trigger_success_to_minio(bucket, files)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)

            function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
            raise AirflowFailException("function create_file_trigger_success_to_minio fail Line", exc_tb.tb_lineno)

        print('success')
    else:
        unixtime = str(int(datetime.now().timestamp()))

        try:
            if result_CMRTR001['Counterparty Id'].count() != 0:
                function.upload_csv_to_minio(result_CMRTR001,'{0}{1}/CTP46RTR_CMRTR001.csv'.format(config['vald_path'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CTP46RTR_CMRTR001.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_RIRTR001['Counterparty Id'].count() != 0:
                function.upload_csv_to_minio(result_RIRTR001,'{0}{1}/CTP46RTR_RIRTR001.csv'.format(config['vald_path'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CTP46RTR_RIRTR001.csv fail Line", exc_tb.tb_lineno)

    #update batch   
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status)    

def CTP46RTR_GEN01(): 
    try:
        #read config files
        config = function.read_config_psql('der_rtr')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

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
        #gen files
        df = function.read_table_psql(config['RDT040605'])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch   
        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection PSQL fail Line", exc_tb.tb_lineno)

    try:
        #load table from PostgreSQL and upload file .csv to minio   
        path = '{0}{1}.csv'.format(config['gen_path'],msr_prd_id)
        function.upload_csv_to_minio(df,path, int(config['chunksize']),s3,bucket_name)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch   
        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
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
                    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
                    raise AirflowFailException("Delete folder fail Line", exc_tb.tb_lineno) 

                try:
                    #update MSR_PRD_ID
                    msr_prd_id = function.update_msr_prd_id_monthly(msr_prd_id)
                    list_parm_values = [f'{msr_prd_id}']
                    list_parm_nm = ['MSR_PRD_ID']
                    function.update_config_psql(list_parm_values, list_parm_nm, config['RDT03'], config['table'].split(".")[1])
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    print(e)

                    #update batch   
                    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
                    raise AirflowFailException("update msr_prd_id fail Line", exc_tb.tb_lineno)
            else:
                pass
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch   
        function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection minio fail Line", exc_tb.tb_lineno)

    #update batch    
    date_process = config['date_process'].split(",")[1]       #[0] start [1] end
    status = config['status_btch'][2]                         #[0]R      [1]A      [2]S
    function.update_btch(config['RDT01'],config['RDT02'],task_name,config['btch_id'],number_id_job,date_process,status,True)
    
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 7, 31),
    'schedule_interval': None,
}
with DAG('MONTHLY_CTP_46_DER_RTR',
         schedule_interval='@monthly', 
         default_args=default_args,
         description='Monthly: 4.6 Relationship to Reporter',
         catchup=False) as dag:
 
    task1 = PythonOperator(
        task_id='CTP46RTR_LOAD01',
        python_callable=CTP46RTR_LOAD01
    )
    
    task2 = PythonOperator(
        task_id='CTP46RTR_XFRM01',
        python_callable=CTP46RTR_XFRM01
    )

    task3 = PythonOperator(
        task_id='CTP46RTR_AP01_MPI_DER_RTR',
        python_callable=CTP46RTR_AP01_MPI_DER_RTR    
    )

    task4 = PythonOperator(
        task_id='CTP46RTR_VALD01',
        python_callable=CTP46RTR_VALD01      
    ) 

    task5 = PythonOperator(
        task_id='CTP46RTR_GEN01',
        python_callable=CTP46RTR_GEN01      
    ) 

    task1 >> task2 >> task3 >> task4 >> task5 
