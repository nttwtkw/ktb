from datetime import datetime
import pandas as pd
import ibm_db
import vaex
import sys
from common_function import function
from common_function import pw
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowFailException

def STG_CTP_ETT_LOAD01():
    try:
        #read config files
        config = function.read_config_psql('STG_CTP_ETT')
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

    try:
        #delete staging from DB2 where tm_key
        function.execute_db2_ibm(config['RDT100001'], config = 'delete')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException('Connection db2 fail Line', exc_tb.tb_lineno)

    try:
        #insert data to Staging table on DB2
        function.execute_db2_ibm(config['RDT100002'], config = 'insert')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException('Connection db2 fail Line', exc_tb.tb_lineno)

    try:
        #update MSR_PRD_ID
        msr_prd_id = function.update_msr_prd_id_monthly(config['MSR_PRD_ID'])
        list_parm_values = [f'{msr_prd_id}']
        list_parm_nm = ['MSR_PRD_ID']
        function.update_config_psql(list_parm_values, list_parm_nm, config['RDT600003'], config['TABLE'].split(".")[1])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch   
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("update msr_prd_id fail Line", exc_tb.tb_lineno)

    #update batch    
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status)
    
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 12, 31),
    'schedule_interval': None,
}

with DAG('STAGING_COUNTERPARTY_ENTITY',
         schedule_interval='@monthly', 
         default_args=default_args,
         description='Staging Monthly: Counterparty Entity Staging',
         catchup=False) as dag:
 
    task1 = PythonOperator(
        task_id='STG_CTP_ETT_LOAD01',
        python_callable=STG_CTP_ETT_LOAD01
    )
    
    task1 

