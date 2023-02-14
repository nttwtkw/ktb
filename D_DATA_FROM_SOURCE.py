import os
import logging
from rdt_engine.engineIIAS2PSQLMgmt import processIIAS2PSQL
from datetime import datetime, date, timedelta,timezone
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email_operator import EmailOperator
from airflow.exceptions import AirflowFailException
from airflow.operators.dummy_operator import DummyOperator

def ETL_LOAD():
    logger = logging.getLogger("ETL_LOAD "+" d_data_from_source_2 " + "rdt_ac")
    logger.info("Start")
    dirname = os.path.dirname(__file__)
    #cwd = os.getcwd()
    #print(cwd)
    logger.info("dirname="+dirname)
    if (dirname.find("rdt_engine")<0):
        os.chdir(dirname + "/rdt_engine/")
    path = os.getcwd()
    logger.info("path="+path)
    try :
        logger.info("Process")
        processIIAS2PSQL('d_data_from_source_2', 'rdt_ac')
    except Exception as e:
        print(e)
        print('Load data fail')
        raise AirflowFailException("Load data fail")
    logger.info("End")

    
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 5, 27),
    'schedule_interval': None,
}
with DAG('D_DATA_FROM_SOURCE',
         schedule_interval='@daily',
         default_args=default_args,
         description='Load data from source',
         catchup=False) as dag:
 
    task1 = PythonOperator(
        task_id='ETL_LOAD',
        python_callable=ETL_LOAD
    )
    
    task1
