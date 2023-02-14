from datetime import datetime
import pandas as pd
import sys
from common_function import function
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowFailException

def CMM79RDA_LOAD_CMS01():
    try:
        #read config files
        config = function.read_config_psql('DER_RDA_D')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    #update batch     
    task_name = config['TASK_NAME_CMS_01']
    number_id_job = '1'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID_CMS'],number_id_job,date_process,status,True)

    #batch status
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    #connect minio ex: download file , upload parquet to minio type file
    bucket =  function.create_connection_minio(config['BUCKET_NAME']) 

    #parameter
    msr_prd_id = config['MSR_PRD_ID_CMS']
    table = config['TABLE_CMS']

    #query from db2
    query_db2 = config['RDT070901']

    try:

        #read table,convert dataframe to .parquet and upload to minio
        df,con_db2 = function.read_db2(query_db2)
        df_copy = pd.concat(df,ignore_index=True)
        con_db2.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException('Connection db2 fail Line', exc_tb.tb_lineno)

    try:
        #delete data in table before insert
        function.delete_data_in_psql_filter_by_msr_prd_id(table,msr_prd_id,config['RDT600004'])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID_CMS'],number_id_job,date_process,status,True)
        raise AirflowFailException(f'Delete Table {table} in psql fail Line', exc_tb.tb_lineno)

    table_psql_insert = table.split(".")[-1]
    query = function.query_insert(table)

    try:
        #check data type and insert into psql
        function.insert_table_into_postgresql_daily(df_copy, query, int(config['CHUNKSIZE']), table_psql_insert)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID_CMS'],number_id_job,date_process,status,True)
        raise AirflowFailException('Insert data into table psql fail Line', exc_tb.tb_lineno)
        
    #update batch     
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID_CMS'],number_id_job,date_process,status,False)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 2, 17),
    'schedule_interval': None,
}
with DAG('DAILY_CMM_79_DER_RDA_CMS',
         schedule_interval='@daily', 
         default_args=default_args,
         description='Daily: 7.9 Related Deposit Account CMS',
         catchup=False) as dag:
 
    task1 = PythonOperator(
        task_id='CMM79RDA_LOAD_CMS01',
        python_callable=CMM79RDA_LOAD_CMS01
    )

    task1 