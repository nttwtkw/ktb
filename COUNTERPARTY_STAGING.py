from datetime import datetime
import pandas as pd
import ibm_db
import vaex
import sys
from common_function_2 import function
from common_function import pw
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowFailException

def STG_CTP_LOAD01():
    try:
        #read config files
        config = function.read_config_psql('STG_CTP')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    try:
        #connection db2
        conn = ibm_db.connect(f"DATABASE={pw.DB_NAME_DB2}; HOSTNAME={pw.IP_DB2}; PORT={pw.PORT_DB2};PROTOCOL=TCPIP;UID={pw.USER_DB2}; PWD={pw.PASSWORD_DB2};", "", "")
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    #delete data
    query_delete_data = config['RDT100001']
    parameter_replace = ['{TABLE}','{MSR_PRD_ID}']
    value_replace = [config['TABLE01'], config['MSR_PRD_ID']]
    query_delete_replace = function.replace_value(query_delete_data, parameter_replace, value_replace)

    try:
        #execute
        stmt = ibm_db.exec_immediate(conn, query_delete_replace)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    #show number of record
    print(f'delete: {ibm_db.num_rows(stmt)} records')

    #insert data into STG TABLE
    query_insert_data = config['RDT100002']
    parameter_replace = ['{TABLE}','{MSR_PRD_ID}']
    value_replace = [config['TABLE01'], config['MSR_PRD_ID']]
    query_insert_replace = function.replace_value(query_insert_data, parameter_replace, value_replace)

    try:
        #execute
        stmt = ibm_db.exec_immediate(conn, query_insert_replace)
        print(f"Insert data into {config['TABLE01']} successfully..........")
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    #close connection
    ibm_db.close(conn)
    
def STG_CTP_LOAD_STG_ENT_01():
    try:
        #read config files
        config = function.read_config_psql('STG_CTP')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    #update batch     
    task_name = config['TASK_NAME02']
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
        df,con_db2 = function.read_db2(config['RDT100003'])
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
    
def STG_CTP_AP01_MPI_STG_ENT():
    try:
        #read config files
        config = function.read_config_psql('STG_CTP')
    except Exception as e:
        print(e)
        print('Read config file fail Line 125')    
        raise AirflowFailException("Read config file fail Line 125")

    #update batch
    task_name = config['TASK_NAME03']
    number_id_job = '2'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)

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

    #parameter
    table = config['TABLE02']
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
    table = function.read_table_local(config['FILE_NAME02'])

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
    
def STG_CTP_AP02_MPI_STG_ENT_DTL():
    #insert data from staging to staging on psql
    try:
        #read config files
        config = function.read_config_psql('STG_CTP')
    except Exception as e:
        print(e)
        print('Read config file fail Line 125')    
        raise AirflowFailException("Read config file fail Line 125")

    #update batch
    task_name = config['TASK_NAME04']
    number_id_job = '3'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)

    try:
        #insert data to Staging table on PostgreSQL 
        table = function.read_table_psql_modify(config['RDT100004'])
    except  Exception as e:
        print(e)
        print('Inset data to table on PSQL fail Line 144')

    #update batch    
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status)
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 9, 30),
    'schedule_interval': None,
}

with DAG('COUNTERPARTY_STAGING',
         schedule_interval='@daily', 
         default_args=default_args,
         description='CounterParty Staging',
         catchup=False) as dag:
 
    task1 = PythonOperator(
        task_id='STG_CTP_LOAD01',
        python_callable=STG_CTP_LOAD01
    )

    task2 = PythonOperator(
        task_id='STG_CTP_LOAD_STG_ENT_01',
        python_callable=STG_CTP_LOAD_STG_ENT_01    
    )
    
    task3 = PythonOperator(
        task_id='STG_CTP_AP01_MPI_STG_ENT',
        python_callable=STG_CTP_AP01_MPI_STG_ENT
    )

    task4 = PythonOperator(
        task_id='STG_CTP_AP02_MPI_STG_ENT_DTL',
        python_callable=STG_CTP_AP02_MPI_STG_ENT_DTL    
    )

    task1 >> task2 >> task3 >> task4

