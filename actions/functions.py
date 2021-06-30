import pandas as pd
import datetime
from pyhive import hive

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
    df = pd.read_sql("SELECT mainkey, status FROM audit.jobtracking WHERE startts LIKE '%" + date + "%' LIMIT 30", conn)
    return df

def get_by_job_status(date, status):
    df = pd.read_sql("SELECT mainkey FROM audit.jobtracking WHERE startts LIKE '%" + date + "%' AND status LIKE '%" + status + "%' LIMIT 30", conn)
    return df

def get_by_product(date, status, product):    
    df = pd.read_sql("SELECT mainkey FROM audit.jobtracking WHERE startts LIKE '%" + date + "%' AND status LIKE '%" + status + "%' AND product LIKE '%" + product + "%' LIMIT 30", conn)
    return df

def get_by_name(date, status, jobname):
    df = pd.read_sql("SELECT mainkey FROM audit.jobtracking WHERE startts LIKE '%" + date + "%' AND status LIKE '%" + status + "%' AND mainkey LIKE '%" + jobname + "%' LIMIT 30", conn)
    return df

def get_incident(date):
    df = pd.read_sql("SELECT mainkey, incidentno, status FROM audit.jobtracking WHERE startts LIKE '%" + date + "%' AND incidentno IS NOT NULL LIMIT 30", conn)
    return df

def get_restart(date):
    df = pd.read_sql("SELECT mainkey, status, restartflag as restart FROM audit.jobtracking WHERE startts LIKE '%" + date + "%' AND restartflag = 'Y' LIMIT 30", conn)
    return df

def get_latest_job_status_for_date(date, jobname):
    df = pd.read_sql("SELECT status FROM ( SELECT *, ROW_NUMBER() over (PARTITION by jobname ORDER BY endts DESC) rn FROM audit.jobtracking WHERE mainkey LIKE '%" + jobname + "%' AND startts LIKE '%" + date + "%') t1 WHERE rn = 1", conn)
    return df

def get_run_time_for_date(date):
    df = pd.read_sql("SELECT mainkey, ((unix_timestamp(endts) - unix_timestamp(startts))/60) AS time_taken FROM audit.jobtracking WHERE startts LIKE '%" + date + "%' AND restartflag = 'Y' LIMIT 30", conn)
    return df

def get_sub_jobs(date, jobname):
    df = pd.read_sql("SELECT dtlkey, extractts, status FROM audit.jobtrackingdtl WHERE mainkey in ( SELECT mainkey FROM audit.jobtracking WHERE mainkey LIKE '%" + jobname + "%' AND startts LIKE '%" + date + "%' LIMIT 30) LIMIT 30", conn)
    return df

def get_path(path, jobname):
    df = pd.read_sql("SELECT " + path + " FROM audit.ecg WHERE configkey LIKE '%" + jobname + "%'", conn)
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
        elif (date is "day after tomorrow"):
            valid_date = datetime.date.today() + datetime.timedelta(days = 1)
        else:
            valid_date = pd.to_datetime(date)
    except (ValueError, RuntimeError, NameError,  TypeError):
        return ""
    final_date = valid_date.strftime("%Y") + valid_date.strftime("-%m-") + valid_date.strftime("%d")
    return final_date

def get_valid_path(path):
    path.lower()
    if (path.find("target")!=-1 or path.find("destination")!=-1):
        path="destinationpath"
    elif (path.find("archive")!=-1 or path.find("archived")!=-1):
        path="archivepath"
    elif (path.find("source")!=-1):
        path="sourcepath"
    return path

def get_planscheduledstarttime(jobname):
    df = pd.read_sql("SELECT jobname, planscheduledstarttime FROM audit.sla WHERE jobname LIKE '%" + jobname + "%'", conn)
    return df
