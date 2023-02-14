from datetime import datetime
import pandas as pd
import numpy as np
import vaex
import sys
from common_function_2 import function
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator


def CAC13CC_GEN01():
    raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)
    try:
        #read config files
        config = function.read_config_psql('DER_CC')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    task_name = config['TASK_NAME01']
    number_id_job = '1'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)

    #parameter
    bucket_name = config['BUCKET_NAME']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')
    msr_prd_id = config['MSR_PRD_ID']

    #batch status
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    # assign data of lists.  
    data = {'Organization Id': [None], 'Data Date': [datetime(2020, 5, 17)], 'Account Id': [None], 'Credit Card Type': [None], '''Parent's Account Id''': [None], 'Card Holder Counterparty Id': [None],}  
    # Create DataFrame  
    df = pd.DataFrame(data) 
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
        #update MSR_PRD_ID
        msr_prd_id = function.update_msr_prd_id_monthly(msr_prd_id)
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
    'start_date': datetime(2021, 2, 17),
    'schedule_interval': None,
}
with DAG('MONTHLY_CAC_13_DER_CC',
         schedule_interval='@monthly', 
         default_args=default_args,
         description='none',
         catchup=False) as dag:
 

    t1 = PythonOperator(
        task_id='CAC13CC_GEN01',
        python_callable=CAC13CC_GEN01
    )

    
