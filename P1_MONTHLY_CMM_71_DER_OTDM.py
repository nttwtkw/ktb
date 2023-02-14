from datetime import datetime
import pandas as pd
import numpy as np
import vaex
import sys
from common_function_2 import function
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator

def CMM71OTDM_LOAD01():
    try:
        #read config files
        config = function.read_config_psql('P1_DER_OTDM')
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
        df,con_db2 = function.read_db2(config['RDT070101'])
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
    
def CMM71OTDM_XFRM01():
    try:
        #read config files
        config = function.read_config_psql('P1_DER_OTDM')
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
    
def CMM71OTDM_AP01_MPI_DER_OTDM():
    try:
        #read config files
        config = function.read_config_psql('P1_DER_OTDM')
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
    
def CMM71OTDM_VALD01():
    try:
        #read config files
        config = function.read_config_psql('P1_DER_OTDM')
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

    try:
        #uery data with condition RDT vilidation and return Account Id where not True 
        #validation rule get report column Account Id

        #[Accrued Fee Amount in Baht] must be greater than or equal to 0.
        result_CNOTDM001 = pd.read_sql(config['RDT070102'] ,conn_psql)
        
        #If [Accrued Interest Receivables in Baht] is not blank, then [Accrued Interest Receivables in Baht] must be greater than or equal to 0.
        result_CNOTDM002 = pd.read_sql(config['RDT070103'] ,conn_psql)
        
        #If [Undue Interest Receivables in Baht] is not blank, then it must be greater than or equal to 0.
        result_CNOTDM003 = pd.read_sql(config['RDT070104'] ,conn_psql)
        
        #If [Contractual Interest Receivables in Baht] is not blank, then it must be greater than or equal to 0.
        result_CNOTDM004 = pd.read_sql(config['RDT070105'] ,conn_psql)
        
        #If [Current Effective Interest Rate] is not blank, then it must be greater than or equal to 0 and less than or equal to 100.
        result_CNOTDM005 = pd.read_sql(config['RDT070106'] ,conn_psql)
        
        #If [Unearned Revenue in Original Currency] is not blank, then it must be greater than or equal to 0.
        result_CNOTDM006 = pd.read_sql(config['RDT070107'] ,conn_psql)
        
        #If [Unearned Revenue in Baht] is not blank, then it must be greater than or equal to 0.
        result_CNOTDM007 = pd.read_sql(config['RDT070108'] ,conn_psql)
        
        #[Suspended Amount in Baht] must be greater than or equal to 0.
        result_CNOTDM008 = pd.read_sql(config['RDT070109'] ,conn_psql)
        
        #If [Days Past Due] is not blank, then it must be greater than or equal to 0.
        result_CNOTDM009 = pd.read_sql(config['RDT070110'] ,conn_psql)
        
        #If [Loan and Contingent Type] in Credit Account (DER_CAC) is not classified as “Contingent loans”, then [Accrued Interest Receivables in Baht], 
        #[Undue Interest Receivables in Baht], [Contractual Interest Receivables in Baht], [Current Effective Interest Rate], [Unearned Revenue in Original Currency], [Unearned Revenue in Baht], [Days Past Due] cannot be blank.
        result_CMOTDM001 = pd.read_sql(config['RDT070111'] ,conn_psql)
        
        #If [Product Loan Type Under Regulate] ที่ Credit Account Detail (DER_CACD) is not blank, then [Total Interest and Fee Rate] cannot be blank and must be between 0 and 100.
        result_CMOTDM002 = pd.read_sql(config['RDT070112'] ,conn_psql)
        
        #Sum up of [Outstanding Amount in Baht] of Outstanding Monthly (DER_OTDM) must be equal to Sum up of [Balance Sheet Amount] in Balance Sheet (DS_BLS) with conditions.
        def CMOTDM003() :
            #Load data from der_otdm 
            otdm = pd.read_sql(config['RDT070113'] ,conn_psql)
            #Load  data from ds_pvs
            query = function.replace_value(config['RDT070115'], ['{msr_prd_id}'], [config['MSR_PRD_ID']])
            bls = function.read_table_db2(query)
            bls['org_id_bls'] = 'TCB'

            #Sum up of [Outstanding Amount in Baht] of Outstanding Monthly (DER_OTDM) must be equal to Sum up of [Amount] in Provision Summary (DS_PVS) with conditions of performing loan.
            otdm_bls = pd.merge(bls,otdm ,left_on=['ds_dt','org_id_bls'], right_on=['data_dt','org_id'], how="inner" )
            sum_bls  = otdm_bls.loc[(((otdm_bls['org_id_bls'] == 'TCB') | (otdm_bls['fi_rpt_grp_id'] =='116002')) | ((otdm_bls['org_id_bls'] == 'FCB') | (otdm_bls['fi_rpt_grp_id'] =='116003'))) 
                & (otdm_bls['bsh_itm_tp_id'].isin(['955010','955015', '955030', '955051','955070','955124'])),['bsh_amt']].sum()
            sum_otdm = otdm_bls['otsnd_amt_in_baht'].sum()
            sum_bls = sum_bls[0]

            if sum_bls == sum_otdm:
                return True
            else : return False
        result_CMOTDM003 = CMOTDM003()
        
        #Sum up of [Outstanding Amount in Baht] of Outstanding Monthly (DER_OTDM) must be equal to Sum up of [Amount] in Provision Summary (DS_PVS) with conditions of performing loan.
        def CMOTDM004() :
            #Load data from der_otdm 
            otdm = pd.read_sql(config['RDT070114'] ,conn_psql)
            #Load  data from ds_pvs
            query = function.replace_value(config['RDT070116'], ['{msr_prd_id}'], [config['MSR_PRD_ID']])
            pvs = function.read_table_db2(query)
            pvs['org_id_pvs'] = 'TCB'

            #Sum up of [Outstanding Amount in Baht] of Outstanding Monthly (DER_OTDM) must be equal to Sum up of [Amount] in Provision Summary (DS_PVS) with conditions of performing loan.
            otdm_pvs = pd.merge(pvs,otdm ,left_on=['ds_dt','org_id_pvs'], right_on=['data_dt','org_id'], how="inner" )
            sum_pvs  = otdm_pvs.loc[(((otdm_pvs['org_id_pvs'] == 'TCB') | (otdm_pvs['fi_rpt_grp_id'] =='116002')) | ((otdm_pvs['org_id_pvs'] == 'FCB') | (otdm_pvs['fi_rpt_grp_id'] =='116008'))) 
                & (otdm_pvs['ast_cl_tp_id'] == '020011') & (otdm_pvs['prvn_smy_itm_tp_id']=='960080'),['amt']].sum()
            sum_otdm = otdm_pvs.loc[(otdm_pvs['ast_and_cntg_clss'] == '2000800001')  & (pd.to_datetime(otdm['data_dt']).dt.month.isin([3,6,9,12])),['otsnd_amt_in_baht','uern_rev_in_baht']].sum()
            sum_pvs  = sum_pvs[0]
            sum_otdm = sum_otdm[0] - sum_otdm[1]
            sum_otdm

            if sum_pvs == sum_otdm :
                return True
            else : return False
        result_CMOTDM004 = CMOTDM004()
    
        #Sum up of [Outstanding Amount in Baht] of Outstanding Monthly (DER_OTDM) must be equal to Sum up of [Amount] in Provision Summary (DS_PVS) with conditions of under-performing loan.
        def CMOTDM005() :
            #Load data from der_otdm 
            otdm = pd.read_sql(config['RDT070114'] ,conn_psql) 
            #Load  data from ds_pvs
            query = function.replace_value(config['RDT070116'], ['{msr_prd_id}'], [config['MSR_PRD_ID']])
            pvs = function.read_table_db2(query)
            pvs['org_id_pvs'] = 'TCB'

            #Sum up of [Outstanding Amount in Baht] of Outstanding Monthly (DER_OTDM) must be equal to Sum up of [Amount] in Provision Summary (DS_PVS) with conditions of performing loan.
            otdm_pvs = pd.merge(pvs,otdm ,left_on=['ds_dt','org_id_pvs'], right_on=['data_dt','org_id'], how="inner" )
            sum_pvs =otdm_pvs.loc[(((otdm_pvs['org_id_pvs'] == 'TCB') | (otdm_pvs['fi_rpt_grp_id'] =='116002')) | ((otdm_pvs['org_id_pvs'] == 'FCB') | (otdm_pvs['fi_rpt_grp_id'] =='116008'))) 
                & (otdm_pvs['ast_cl_tp_id'] == '020012') & (otdm_pvs['prvn_smy_itm_tp_id']=='960080'),['amt']].sum()
            sum_otdm=otdm_pvs.loc[(otdm_pvs['ast_and_cntg_clss'] == '2000800002')  & (pd.to_datetime(otdm['data_dt']).dt.month.isin([3,6,9,12])),['otsnd_amt_in_baht','uern_rev_in_baht']].sum()
            sum_pvs =sum_pvs[0]
            sum_otdm=sum_otdm[0] - sum_otdm[1]
            sum_otdm

            if sum_pvs == sum_otdm :
                return True
            else : return False
        result_CMOTDM005 = CMOTDM005()
    
        #Sum up of [Outstanding Amount in Baht] of Outstanding Monthly (DER_OTDM) must be equal to Sum up of [Amount] in Provision Summary (DS_PVS) with conditions of non-performing loan.
        def CMOTDM006() :
            #Load data from der_otdm 
            otdm = pd.read_sql(config['RDT070114'] ,conn_psql)
            #Load  data from ds_pvs
            query = function.replace_value(config['RDT070116'], ['{msr_prd_id}'], [config['MSR_PRD_ID']])
            pvs = function.read_table_db2(query)
            pvs['org_id_pvs'] = 'TCB'

            #Sum up of [Outstanding Amount in Baht] of Outstanding Monthly (DER_OTDM) must be equal to Sum up of [Amount] in Provision Summary (DS_PVS) with conditions of performing loan.
            otdm_pvs = pd.merge(pvs,otdm ,left_on=['ds_dt','org_id_pvs'], right_on=['data_dt','org_id'], how="inner" )
            sum_pvs =otdm_pvs.loc[(((otdm_pvs['org_id_pvs'] == 'TCB') | (otdm_pvs['fi_rpt_grp_id'] =='116002')) | ((otdm_pvs['org_id_pvs'] == 'FCB') | (otdm_pvs['fi_rpt_grp_id'] =='116008'))) 
                & (otdm_pvs['ast_cl_tp_id'] == '020013') & (otdm_pvs['prvn_smy_itm_tp_id']=='960080'),['amt']].sum()
            sum_otdm=otdm_pvs.loc[(otdm_pvs['ast_and_cntg_clss'] == '2000800003')  & (pd.to_datetime(otdm['data_dt']).dt.month.isin([3,6,9,12])),['otsnd_amt_in_baht','uern_rev_in_baht']].sum()
            sum_pvs =sum_pvs[0]
            sum_otdm=sum_otdm[0] - sum_otdm[1]
            sum_otdm

            if sum_pvs == sum_otdm :
                return True
            else : return False
        result_CMOTDM006 = CMOTDM006()

        #Sum up of [Outstanding Amount in Baht] of Outstanding Monthly (DER_OTDM) must be equal to Sum up of [Amount] in Provision Summary (DS_PVS) with conditions of purchased or originated credit impaired (POCI).
        def CMOTDM007() :
            #Load data from der_otdm 
            otdm = pd.read_sql(config['RDT070114'] ,conn_psql)
            #Load  data from ds_pvs
            query = function.replace_value(config['RDT070116'], ['{msr_prd_id}'], [config['MSR_PRD_ID']])
            pvs = function.read_table_db2(query)
            pvs['org_id_pvs'] = 'TCB'

            #Sum up of [Outstanding Amount in Baht] of Outstanding Monthly (DER_OTDM) must be equal to Sum up of [Amount] in Provision Summary (DS_PVS) with conditions of performing loan.
            otdm_pvs = pd.merge(pvs,otdm ,left_on=['ds_dt','org_id_pvs'], right_on=['data_dt','org_id'], how="inner" )
            sum_pvs =otdm_pvs.loc[(((otdm_pvs['org_id_pvs'] == 'TCB') | (otdm_pvs['fi_rpt_grp_id'] =='116002')) | ((otdm_pvs['org_id_pvs'] == 'FCB') | (otdm_pvs['fi_rpt_grp_id'] =='116008'))) 
                & (otdm_pvs['ast_cl_tp_id'] == '020014') & (otdm_pvs['prvn_smy_itm_tp_id']=='960080'),['amt']].sum()
            sum_otdm=otdm_pvs.loc[(otdm_pvs['ast_and_cntg_clss'] == '2000800004')  & (pd.to_datetime(otdm['data_dt']).dt.month.isin([3,6,9,12])),['otsnd_amt_in_baht','uern_rev_in_baht']].sum()
            sum_pvs =sum_pvs[0]
            sum_otdm=sum_otdm[0] - sum_otdm[1]
            sum_otdm

            if sum_pvs == sum_otdm :
                return True
            else : return False
        result_CMOTDM007 = CMOTDM007()
    
        #Sum up of [Outstanding Amount in Baht] of Outstanding Monthly (DER_OTDM) must be equal to Sum up of [Amount] in Provision Summary (DS_PVS) with conditions of other purchased or originated credit impaired (POCI).
        def CMOTDM008() :
            #Load data from der_otdm 
            otdm = pd.read_sql(config['RDT070114'] ,conn_psql)
            #Load  data from ds_pvs
            query = function.replace_value(config['RDT070116'], ['{msr_prd_id}'], [config['MSR_PRD_ID']])
            pvs = function.read_table_db2(query)
            pvs['org_id_pvs'] = 'TCB'

            #Sum up of [Outstanding Amount in Baht] of Outstanding Monthly (DER_OTDM) must be equal to Sum up of [Amount] in Provision Summary (DS_PVS) with conditions of performing loan.
            otdm_pvs = pd.merge(pvs,otdm ,left_on=['ds_dt','org_id_pvs'], right_on=['data_dt','org_id'], how="inner" )
            sum_pvs =otdm_pvs.loc[(((otdm_pvs['org_id_pvs'] == 'TCB') | (otdm_pvs['fi_rpt_grp_id'] =='116002')) | ((otdm_pvs['org_id_pvs'] == 'FCB') | (otdm_pvs['fi_rpt_grp_id'] =='116008'))) 
                & (otdm_pvs['ast_cl_tp_id'] == '020015') & (otdm_pvs['prvn_smy_itm_tp_id']=='960080'),['amt']].sum()
            sum_otdm=otdm_pvs.loc[(otdm_pvs['ast_and_cntg_clss'] == '2000800005')  & (pd.to_datetime(otdm['data_dt']).dt.month.isin([3,6,9,12])),['otsnd_amt_in_baht','uern_rev_in_baht']].sum()
            sum_pvs =sum_pvs[0]
            sum_otdm=sum_otdm[0] - sum_otdm[1]
            sum_otdm

            if sum_pvs == sum_otdm :
                return True
            else : return False
        result_CMOTDM008 = CMOTDM008()
        
        #If [Account Status] is “Inactive / Closed”, then both [Outstanding Amount in Original Currency] and [Outstanding Amount in Baht] must be equal to 0.
        result_CMOTDM009 = pd.read_sql(config['RDT070117'] ,conn_psql)
        
        #If [Loan and Contingent Type] in Credit Account (DER_CAC) is classified as “Contingent loans”, then [Credit Equivalent Amount in Baht] cannot be blank and must be greater than or equal to 0.
        result_CMOTDM010 = pd.read_sql(config['RDT070118'] ,conn_psql)
        
        #If [Loan and Contingent Type] in Credit Account (DER_CAC) is classified as “Loans”, “Aval to bills” or “Bill certifications” or [Loan and Contingent Type] is “Loan guarantees" or “unconditional loan guarantees", then [Asset and Contingent Class] and [Asset and Contingent Class Reason] cannot be blank.
        result_CMOTDM011 = pd.read_sql(config['RDT070119'] ,conn_psql)

        #If an outstanding monthly record exists, then a credit account detail record must exist.
        result_RIOTDM001 = pd.read_sql(config['RDT070120'] ,conn_psql)
        

        conn_psql.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(e)

        function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
        raise AirflowFailException("Connection PostgreSQL fail Line", exc_tb.tb_lineno)

    #validation success
    if (    result_CNOTDM001['Account Id'].count() == 0 and
            result_CNOTDM002['Account Id'].count() == 0 and
            result_CNOTDM003['Account Id'].count() == 0 and
            result_CNOTDM004['Account Id'].count() == 0 and
            result_CNOTDM005['Account Id'].count() == 0 and
            result_CNOTDM006['Account Id'].count() == 0 and
            result_CNOTDM007['Account Id'].count() == 0 and
            result_CNOTDM008['Account Id'].count() == 0 and
            result_CNOTDM009['Account Id'].count() == 0 and
            result_CMOTDM001['Account Id'].count() == 0 and
            result_CMOTDM002['Account Id'].count() == 0 and
            result_CMOTDM003 == True and
            result_CMOTDM004 == True and
            result_CMOTDM005 == True and 
            result_CMOTDM006 == True and 
            result_CMOTDM007 == True and 
            result_CMOTDM008 == True and
            result_CMOTDM009['Account Id'].count() == 0 and
            result_CMOTDM010['Account Id'].count() == 0 and
            result_CMOTDM011['Account Id'].count() == 0 and
            result_RIOTDM001['Account Id'].count() == 0  ):

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
            if result_CNOTDM001['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CNOTDM001,'{0}{1}/CAC114DRM_CNOTDM001.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CNOTDM001.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CNOTDM0002['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CNOTDM002,'{0}{1}/CAC114DRM_CNOTDM001.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CNOTDM001.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CNOTDM003['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CNOTDM003,'{0}{1}/CAC114DRM_CNOTDM003.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CNOTDM003.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CNOTDM0004['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CNOTDM004,'{0}{1}/CAC114DRM_CNOTDM004.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CNOTDM004.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CNOTDM005['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CNOTDM005,'{0}{1}/CAC114DRM_CNOTDM005.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CNOTDM005.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CNOTDM0006['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CNOTDM006,'{0}{1}/CAC114DRM_CNOTDM006.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CNOTDM006.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CNOTDM0007['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CNOTDM007,'{0}{1}/CAC114DRM_CNOTDM007.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CNOTDM007.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CNOTDM008['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CNOTDM008,'{0}{1}/CAC114DRM_CNOTDM008.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CNOTDM008.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CNOTDM0009['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CNOTDM009,'{0}{1}/CAC114DRM_CNOTDM009.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CNOTDM009.csv fail Line", exc_tb.tb_lineno)            
            
        try:
            if result_CMOTDM001['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMOTDM001,'{0}{1}/CAC114DRM_CMOTDM001.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CMOTDM001.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CMOTDM002['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_CMOTDM002,'{0}{1}/CAC114DRM_CMOTDM002.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CMOTDM002.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CMOTDM003 != True :
            #result_CMOTDM003 = otdm_bls
                function.upload_csv_to_minio(otdm_bls,'{0}{1}/CAC114DRM_CMOTDM003.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CMOTDM003.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CMOTDM004 != True :
            #result_CMOTDM004 = otdm_pvs
                function.upload_csv_to_minio(otdm_pvs,'{0}{1}/CAC114DRM_CMOTDM004.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CMOTDM004.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CMOTDM005 != True :
            #result_CMOTDM005 = otdm_pvs
                function.upload_csv_to_minio(otdm_pvs,'{0}{1}/CAC114DRM_CMOTDM005.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CMOTDM005.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CMOTDM006 != True :
            #result_CMOTDM006 = otdm_pvs
                function.upload_csv_to_minio(otdm_pvs,'{0}{1}/CAC114DRM_CMOTDM006.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CMOTDM006.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CMOTDM007 != True :
            #result_CMOTDM007 = otdm_pvs
                function.upload_csv_to_minio(otdm_pvs,'{0}{1}/CAC114DRM_CMOTDM007.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CMOTDM007.csv fail Line", exc_tb.tb_lineno)
            
        try:
            if result_CMOTDM008 != True :
            #result_CMOTDM008 = otdm_pvs
                function.upload_csv_to_minio(otdm_pvs,'{0}{1}/CAC114DRM_CMOTDM008.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_CMOTDM008.csv fail Line", exc_tb.tb_lineno)
        
        try:
            if result_RIOTDM001['Account Id'].count() != 0:
                function.upload_csv_to_minio(result_RIOTDM001,'{0}{1}/CAC114DRM_RIOTDM001.csv'.format(config['VALD_PATH'],unixtime),chunksize,s3,bucket_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e)
            function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status,True)
            raise AirflowFailException("upload_csv_to_minio CAC114DRM_RIOTDM001.csv fail Line", exc_tb.tb_lineno)


    #update batch   
    status = config['STATUS_SUCCESS']
    function.update_btch(config['RDT600001'],config['RDT600002'],task_name,config['BTCH_ID'],number_id_job,date_process,status)  
    
def CMM71OTDM_GEN01(): 
    try:
        #read config files
        config = function.read_config_psql('P1_DER_OTDM')
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
        df = function.read_table_psql(config['RDT070121'])
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
                    msr_prd_id = function.update_msr_prd_id_monthly(msr_prd_id)
                    list_parm_values = [f'{msr_prd_id}']
                    list_parm_nm = ['MSR_PRD_ID']
                    function.update_config_psql(list_parm_values, list_parm_nm, config['RDT600003'], "P1_"+config['TABLE'].split(".")[1])
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
    'start_date': datetime(2021, 11, 30),
    'schedule_interval': None,
}
with DAG('P1_MONTHLY_CMM_71_DER_OTDM',
         schedule_interval='@monthly',
         default_args=default_args,
         description='Monthly : 7.1 Outstanding Monthly',
         catchup=False) as dag:
 
    task_1 = PythonOperator(
        task_id='CMM71OTDM_LOAD01',
        python_callable=CMM71OTDM_LOAD01
    )

    task_2 = PythonOperator(
        task_id='CMM71OTDM_XFRM01',
        python_callable=CMM71OTDM_XFRM01
        
    )

    task_3 = PythonOperator(
        task_id='CMM71OTDM_AP01_MPI_DER_OTDM',
        python_callable=CMM71OTDM_AP01_MPI_DER_OTDM
        
    )

    task_4 = PythonOperator(
        task_id='CMM71OTDM_VALD01',
        python_callable=CMM71OTDM_VALD01
        
    )

    task_5 = PythonOperator(
        task_id='CMM71OTDM_GEN01',
        python_callable=CMM71OTDM_GEN01

        
    ) 

    task_1 >> task_2 >> task_3 >> task_4 >> task_5
