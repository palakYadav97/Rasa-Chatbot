import pandas as pd
import datetime
from pyhive import hive
import thrift_sasl 

conn = hive.connect(host='dbsls0202',
            port=10308,
            database='audit',
            username='pyadav97',
            password="Test@123",
            auth='LDAP')

def limit_query(limit):
    df = pd.read_sql("SELECT * FROM audit.jobtracking LIMIT " + limit, conn)
    return df

def show_status(date, jobname):
    df = pd.read_sql("SELECT status FROM audit.jobtracking WHERE mainkey LIKE '%" + jobname + "%' AND startts LIKE '%"+date+"%' LIMIT 30", conn)
    return df

def get_all_jobs(date):
    df = pd.read_sql("SELECT * FROM audit.jobtracking WHERE startts LIKE '%" + date + "%' LIMIT 30", conn)
    return df

def get_by_job_status(date, status):
    df = pd.read_sql("SELECT * FROM audit.jobtracking WHERE startts LIKE '%" + date + "%' AND status LIKE '%" + status + "%' LIMIT 30", conn)
    return df

def get_by_product(date, status, product):    
    df = pd.read_sql("SELECT * FROM audit.jobtracking WHERE startts LIKE '%" + date + "%' AND status LIKE '%" + status + "%' AND product LIKE '%" + product + "%' LIMIT 30", conn)
    return df

def get_by_name(date, status, jobname):
    df = pd.read_sql("SELECT * FROM audit.jobtracking WHERE startts LIKE '%" + date + "%' AND status LIKE '%" + status + "%' AND mainkey LIKE '%" + jobname + "%' LIMIT 30", conn)
    return df

def get_incident(date):
    df = pd.read_sql("SELECT * FROM audit.jobtracking WHERE startts like '%" + date + "%' AND incidentno IS NOT NULL LIMIT 30", conn)
    return df

def get_restart(date):
    df = pd.read_sql("SELECT * FROM audit.jobtracking WHERE startts like '%" + date + "%' AND restartflag = 'Y' LIMIT 30", conn)
    return df

def get_valid_status(status):
    status.lower()
    if (status.find("run")!=-1 or status.find("start")!=-1):
        status="Started"
    elif (status.find("done")!=-1 or status.find("finish")!=-1 or status.find("complete")!=-1):
        status="Completed"
    elif (status.find("fail")!=-1):
        status="Failed"
    return status

def get_valid_date(date):
    try:
        if (date is ""):
            return date
        elif (date is "today"):
            valid_date = datetime.date.today()
        elif (date is "yesterday"):
            valid_date = datetime.date.today() - datetime.timedelta(days = 1)
        elif (date is "day before yesterday"):
            valid_date = datetime.date.today() - datetime.timedelta(days = 2)
        elif (date is "tomorrow"):
            valid_date = datetime.date.today() + datetime.timedelta(days = 1)
        elif (date is "day of tomorrow"):
            valid_date = datetime.date.today() + datetime.timedelta(days = 1)
        else:
            valid_date = pd.to_datetime(date)
    except (ValueError, RuntimeError, NameError,  TypeError):
        return ""
    final_date = valid_date.strftime("%Y") + valid_date.strftime("-%m-") + valid_date.strftime("%d")
    return final_date
