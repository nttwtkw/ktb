#!/usr/bin/env python
# coding: utf-8

# In[10]:


from rdt_engine.dbConn import getPSQLConn
import logging


# In[11]:


def startJob(batch_nm, job_nm):
    conn = None
    try:
        logger = logging.getLogger("JobSchedule:StartJob:"+batch_nm+":"+job_nm)
        
        conn = getPSQLConn()
        cursor = conn.cursor()

        query = "select distinct job_st from rdtdba.batch_job where batch_nm = '" + batch_nm + "' and job_st <> 'S'"
        cursor.execute(query)
        results = cursor.fetchone()
        if results is None:
            query = "update rdtdba.batch_job set job_st = 'N', start_date = null, end_date = null where batch_nm = '" + batch_nm + "'"
            cursor.execute(query)
            conn.commit()
    
            query = "update rdtdba.batch_schedule set batch_date = "                     + "case when batch_type = 'D' then batch_date + INTERVAL '1 day' else date_trunc('month', batch_date) + INTERVAL '2 month - 1 day' end "                     + ", start_date = null, end_date = null "                     + "where batch_nm = '" + batch_nm + "'"
            cursor.execute(query)
            conn.commit()
            
            logger.info("Update Batch Date is successful")
        
        query = "update rdtdba.batch_schedule set start_date = current_timestamp "                 + "where batch_nm = '" + batch_nm + "' and start_date is null"
        cursor.execute(query)
        conn.commit()
        
        query = "update rdtdba.batch_job set job_st = 'R', start_date = current_timestamp where batch_nm = '" + batch_nm + "' and job_nm = '"  + job_nm + "'"
        cursor.execute(query)
        conn.commit()
        logger.info("Update job status:running is successful")

        query = "select batch_date from rdtdba.batch_schedule where batch_nm = '" + batch_nm + "'"
        cursor.execute(query)
        results = cursor.fetchone()

        jobParm = {"msr_prd_id":results[0].strftime("%Y%m%d")}
        logger.info("Paramter msr_prd_id: "+results[0].strftime("%Y%m%d"))
        
        query = "select parm_nm, parm_val from rdtdba.batch_job_parm where batch_nm = '" + batch_nm + "' and job_nm = '"  + job_nm + "'"
        cursor.execute(query)
        result = cursor.fetchone()
        while result is not None:
            jobParm[result[0]] = result[1]
            logger.info("Paramter " + result[0] + ": "+result[1])
            result = cursor.fetchone()
            
        logger.info("Start Job is successful, Return config values")
        
        return jobParm
    except Exception as e:
        logger.error(e)
        raise Exception(e)
    finally:
        if conn is not None:
            conn.close()
            logger.info("Connection is closed")


# In[12]:


def finishJob(batch_nm, job_nm):
    conn = None
    try:
        logger = logging.getLogger("JobSchedule:FinishJob:"+batch_nm+":"+job_nm)
        
        conn = getPSQLConn()
        cursor = conn.cursor()
        
        query = "update rdtdba.batch_job set job_st = 'S', end_date = current_timestamp where batch_nm = '" + batch_nm + "' and job_nm = '"  + job_nm + "'"
        cursor.execute(query)
        conn.commit()
        
        query = "select distinct job_st from rdtdba.batch_job where batch_nm = '" + batch_nm + "' and job_st <> 'S'"
        cursor.execute(query)
        results = cursor.fetchone()
        if results is None:
            query = "update rdtdba.batch_schedule set end_date = current_timestamp where batch_nm = '" + batch_nm + "'"
            cursor.execute(query)
            conn.commit()
        
        logger.info("Update job status:success is successful")
    except Exception as e:
        logger.error(e)
        raise Exception(e)
    finally:
        if conn is not None:
            conn.close()
            logger.info("Connection is closed")
