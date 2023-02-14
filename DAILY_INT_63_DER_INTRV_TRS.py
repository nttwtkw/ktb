from datetime import datetime, timedelta,timezone
import pandas as pd
import sys
import glob
from common_function import function
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowFailException

def INT63INTRV_LOAD_TRS01():
    try:
        #read config files
        config = function.read_config_psql('DER_INTRV_D')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    #update batch     
    task_name = config['TASK_NAME_TRS_01']
    number_id_job = '1'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID_TRS'],number_id_job,date_process,status,True)

    #batch status
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    #parameter
    path_sftp = config['TRS_PATH']
    name_files_sftp = config['SFTP_NAME_TRS']
    msr_prd_id = config['MSR_PRD_ID_TRS']
    table = config['TABLE_TRS']

    try:
        #download files from sftp
        function.download_file_from_sftp_daily(path_sftp, name_files_sftp,msr_prd_id)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID_TRS'],number_id_job,date_process,status,True)
        raise AirflowFailException('Connection SFTP fail Line', exc_tb.tb_lineno)

    #create list file table name
    filter_file_name = (name_files_sftp+'_'+msr_prd_id+'.TXT'+'*')
    table_files = glob.glob(filter_file_name)

    dtype, columns_name  = function.type_table_psql(table)

    #convert .txt to parquet file upload to minio
    table_data = [pd.read_csv(f,sep="|",names=columns_name, dtype=dtype ) for f in table_files]

    # table_data
    df = pd.concat(table_data,ignore_index=True)

    len_df = len(df) 

    #check number of data
    function.check_record_daily(name_files_sftp, msr_prd_id, len_df)

    #set Thailand timezone + 7 hrs 
    tz = timezone(timedelta(hours = 7))

    #add msr_prd_id and ppn_tm
    df['msr_prd_id'] = msr_prd_id
    df['ppn_tm'] = datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S.%f")

    full_columns_name = columns_name
    full_columns_name.insert(0, 'msr_prd_id')
    full_columns_name.append("ppn_tm")

    #set columns df
    df = df[full_columns_name]

    try:
        #delete data in table before insert
        function.delete_data_in_psql_filter_by_msr_prd_id(table,msr_prd_id,config['RDT600004'])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID_TRS'],number_id_job,date_process,status,True)
        raise AirflowFailException(f'Delete Table {table} in psql fail Line', exc_tb.tb_lineno)

    table_psql_insert = table.split(".")[-1]
    query = function.query_insert(table)

    try:
        #check data type and insert into psql
        function.insert_table_into_postgresql_daily(df, query, int(config['CHUNKSIZE']), table_psql_insert)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch  
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID_TRS'],number_id_job,date_process,status,True)
        raise AirflowFailException('Insert data into table psql fail Line', exc_tb.tb_lineno)

    #update batch     
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID_TRS'],number_id_job,date_process,status,False)
    
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 2, 17),
    'schedule_interval': None,
}
with DAG('DAILY_INT_63_DER_INTRV_TRS',
         schedule_interval='@daily', 
         default_args=default_args,
         description='Daily: 6.3 Interest Reference Value',
         catchup=False) as dag:
 
    task1 = PythonOperator(
        task_id='INT63INTRV_LOAD_TRS01',
        python_callable=INT63INTRV_LOAD_TRS01
    )

    task1 
