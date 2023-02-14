from datetime import datetime
import pandas as pd
import vaex
import sys
from common_function import function
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowFailException

def CLN54COLP_LOAD01():
    try:
        #read config files
        config = function.read_config_psql('DER_COLP')
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

    #query concat pre validation from db2
    query_db2 = config['RDT050401'] + "\n" + config['RDT050402']

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

    #list rules validation
    list_f_rules, list_table = function.list_rules_validation(df_copy)

    #function upload result pre validation
    if len(list_f_rules) > 0:
        number_incorrect_data = function.upload_pre_validation_fail_to_minio(list_f_rules, df_copy, config['TABLE'].split(".")[-1], conf = 'single')
        print(f"PERCNET = {number_incorrect_data/len(df_copy)} %")
    # else:

    #chunk df to parquet
    df_db2 = function.df_chunk(df_copy.iloc[:,0:len(list_table)],int(config['CHUNKSIZE']))
    function.df_to_parquet(df_db2, file_name)

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
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)

def CLN54COLP_XFRM01():
    try:
        #read config files
        config = function.read_config_psql('DER_COLP')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException('Read config file fail Line', exc_tb.tb_lineno)

    #update batch  
    task_name = config['TASK_NAME02']
    number_id_job = '2'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)

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
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)
    
def CLN54COLP_AP01_MPI_DER_COLP():
    try:
        #read config files
        config = function.read_config_psql('DER_COLP')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

    #update batch
    task_name = config['TASK_NAME03']
    number_id_job = '3'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)

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
        function.insert_table_into_postgresql(table,query,int(config['CHUNKSIZE']),config['TABLE'].split(".")[-1])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("cannot insert data to PostgreSQL Line", exc_tb.tb_lineno)

    #update batch    
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)
    
def CLN54COLP_VALD01(): 
    # query data with condition RDT vilidation and return Pledge or Endorsement Id where not True 
    try:
        #read config files
        config = function.read_config_psql('DER_COLP')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

    #update batch    
    task_name = config['TASK_NAME04']
    number_id_job = '4'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)

    conn_psql = function.create_connection_psql()
    bucket_name = config['BUCKET_NAME']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')
    chunksize = int(config['CHUNKSIZE'])
    name = config['FILE_NAME04']
    table = config['TABLE'].split(".")[-1]

    #update batch case fail    
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    try:
        #uery data with condition RDT vilidation and return Pledge or Endorsement Id where not True 
        #validation rule get report column Pledge or Endorsement Id

        #[Mortgage or Pledge Amount in Baht] must be greater than or equal to 0.
        result_CNCOLP001 = pd.read_sql(config['RDT050403'] ,conn_psql) 

        #If [Collateral Type] in collateral is classified as “Land included building and other” and [Country] in either land or building is ‘TH’, then [Mortgage Sequence No] must be greater than 0.
        result_CMCOLP001 = pd.read_sql(config['RDT050404'] ,conn_psql) 

        #If a collateral pledge record exists, then a pledge valuation group record for the pledge valuation must exist.
        result_RICOLP001 = pd.read_sql(config['RDT050405'] ,conn_psql) 

        conn_psql.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection PostgreSQL fail Line", exc_tb.tb_lineno)

    #validation success
    if (result_CNCOLP001['Pledge or Endorsement Id'].count() == 0 and
        result_CMCOLP001['Pledge or Endorsement Id'].count() == 0 and
        result_RICOLP001['Pledge or Endorsement Id'].count() == 0 ):

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
            if result_CNCOLP001['Pledge or Endorsement Id'].count() != 0:
                function.upload_csv_to_minio(result_CNCOLP001,'{0}{1}/CLN54COLP_CNCOLP001.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name,table)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CLN54COLP_CNCOLP001.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_CMCOLP001['Pledge or Endorsement Id'].count() != 0:
                function.upload_csv_to_minio(result_CMCOLP001,'{0}{1}/CLN54COLP_CMCOLP001.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name,table)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CLN54COLP_CMCOLP001.csv fail Line", exc_tb.tb_lineno)

        try:
            if result_RICOLP001['Pledge or Endorsement Id'].count() != 0:
                function.upload_csv_to_minio(result_RICOLP001,'{0}{1}/CLN54COLP_RICOLP001.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name,table)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CLN54COLP_RICOLP001.csv fail Line", exc_tb.tb_lineno)

    #update batch   
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)  
    
def CLN54COLP_GEN01():
    try:
        #read config files
        config = function.read_config_psql('DER_COLP')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)
        raise AirflowFailException("Read config file fail Line", exc_tb.tb_lineno)

    #update batch  
    task_name = config['TASK_NAME05']
    number_id_job = '5'
    date_process = config['PROCESS_START_DATE']
    status = config['STATUS_RUN']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,False)

    #parameter
    bucket_name = config['BUCKET_NAME']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')
    msr_prd_id = config['MSR_PRD_ID']
    table = config['TABLE'].split(".")[-1]

    #check folder validation in minio
    file_success = config['FILE_SUCCESS']
    list_folder = [config['LOAD_PATH'], config['PRE_VALD_PATH'], config['FXRM_PATH'], config['VALD_PATH'], file_success]

    #status batch case fail
    date_process = config['PROCESS_END_DATE']
    status = config['STATUS_ERROR']

    try:
        #gen files
        df = function.read_table_psql(config['RDT050406'])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        #update batch   
        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection PSQL fail Line", exc_tb.tb_lineno)

    try:
        #load table from PostgreSQL and upload file .csv to minio   
        path = '{0}{1}.csv'.format(config['GEN_PATH'],msr_prd_id)
        function.upload_csv_to_minio(df,path, int(config['CHUNKSIZE']),s3,bucket_name,table)
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
                    function.update_config_psql(list_parm_values, list_parm_nm, config['RDT600003'], table)
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
with DAG('MONTHLY_CLN_54_DER_COLP',
         schedule_interval='@monthly', 
         default_args=default_args,
         description='Monthly: 5.4 Collateral Pledge',
         catchup=False) as dag:
 
    task1 = PythonOperator(
        task_id='CLN54COLP_LOAD01',
        python_callable=CLN54COLP_LOAD01
    )

    task2 = PythonOperator(
        task_id='CLN54COLP_XFRM01',
        python_callable=CLN54COLP_XFRM01    
    )

    task3 = PythonOperator(
        task_id='CLN54COLP_AP01_MPI_DER_COLP',
        python_callable=CLN54COLP_AP01_MPI_DER_COLP      
    ) 

    task4 = PythonOperator(
        task_id='CLN54COLP_VALD01',
        python_callable=CLN54COLP_VALD01      
    ) 

    task5 = PythonOperator(
        task_id='CLN54COLP_GEN01',
        python_callable=CLN54COLP_GEN01
    )

    task1 >> task2 >> task3 >> task4 >> task5
