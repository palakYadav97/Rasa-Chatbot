from typing import Any, Text, Dict, List
from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher

class QueryByLimit(Action):
    def name(self) -> Text:
        return "action_get_query"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        limit = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'limit':
                limit = t['value']

        df = limit_query(limit)
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying " + limit + " rows from the DB, here are your results : \n" + str(df))
        return []

class QueryByDateAndKey(Action):
    def name(self) -> Text:
        return "action_get_status"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        date = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'date':
                date = t['value']
        jobname = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'jobname':
                jobname = t['value']

        df = show_status(get_valid_date(date), jobname)
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []

class QueryAllByDate(Action):
    def name(self) -> Text:
        return "action_all_jobs"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        date = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'date':
                date = t['value']

        df = get_all_jobs(get_valid_date(date))
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []
    
class QueryAllByDateAndStatus(Action):
    def name(self) -> Text:
        return "action_job_status"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        date = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'date':
                date = t['value']
        status = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'status':
                status = t['value']

        df = get_by_job_status(get_valid_date(date), get_valid_status(status))
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []

class QueryAllByDateStatusAndProduct(Action):
    def name(self) -> Text:
        return "action_job_product"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        date = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'date':
                date = t['value']
        status = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'status':
                status = t['value']
        product = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'product':
                product = t['value']

        df = get_by_product(get_valid_date(date), get_valid_status(status), product)
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []

class QueryAllByDateStatusAndJobName(Action):
    def name(self) -> Text:
        return "action_job_name"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        date = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'date':
                date = t['value']
        status = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'status':
                status = t['value']
        jobname = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'jobname':
                jobname = t['value']

        df = get_by_name(get_valid_date(date), get_valid_status(status), jobname)
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []

class QueryAllIncidentsByDate(Action):
    def name(self) -> Text:
        return "action_incidents"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        date = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'date':
                date = t['value']

        df = get_incident(get_valid_date(date))
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []

class QueryAllRestartJobsByDate(Action):
    def name(self) -> Text:
        return "action_job_restart"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        date = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'date':
                date = t['value']

        df = get_restart(get_valid_date(date))
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []

class QueryLatestJobStatusForDate(Action):
    def name(self) -> Text:
        return "action_latest_job_status"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        date = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'date':
                date = t['value']
        
        jobname = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'jobname':
                jobname= t['value']

        df = get_latest_job_status_for_date(get_valid_date(date), jobname)
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []

class QueryRunTimeAllByDate(Action):
    def name(self) -> Text:
        return "action_run_time"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        date = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'date':
                date = t['value']

        df = get_run_time_for_date(get_valid_date(date))
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []

class QuerySubJob(Action):
    def name(self) -> Text:
        return "action_sub_job"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        date = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'date':
                date = t['value']
        
        jobname = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'jobname':
                jobname= t['value']

        df = get_sub_jobs(get_valid_date(date), jobname)
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []

class FindPath(Action):
    def name(self) -> Text:
        return "action_path"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        path = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'path':
                path = t['value']
        jobname = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'jobname':
                jobname = t['value']

        df = get_path(get_valid_path(path), jobname)
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no path exists for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []

class FindEStatusFeedname(Action):
    def name(self) -> Text:
        return "action_estatus_feedname"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        estatus = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'estatus':
                estatus = t['value']
        jobname = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'jobname':
                jobname = t['value']

        df = get_estatus_feedname(get_valid_email_status(estatus), jobname)
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no path exists for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []

class FindEmails(Action):
    def name(self) -> Text:
        return "action_emails"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        jobname = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'jobname':
                jobname = t['value']

        df = get_emails(jobname)
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no path exists for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []

class FindExtract(Action):
    def name(self) -> Text:
        return "action_extract"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        file = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'file':
                file = t['value']
        jobname = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'jobname':
                jobname = t['value']

        df = get_extract(get_valid_file(file), jobname)
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no path exists for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []

class FindRally(Action):
    def name(self) -> Text:
        return "action_rally"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        file = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'file':
                file = t['value']
        jobname = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'jobname':
                jobname = t['value']

        df = get_rally(get_valid_file(file), jobname)
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no path exists for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []

class QueryPlanscheduledstarttime(Action):
    def name(self) -> Text:
        return "action_planscheduledstarttime"
    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        jobname = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'jobname':
                jobname= t['value']

        df = get_planscheduledstarttime(jobname)
        if (len(df) == 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results : \n" + str(df))
        return []




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

def get_estatus_feedname(estatus, jobname):
    df = pd.read_sql("SELECT feedname, email" + estatus + "flag FROM audit.notifications WHERE feedname LIKE '%" + jobname + "%' LIMIT 30", conn)
    return df

def get_emails(jobname):
    df = pd.read_sql("SELECT DISTINCT internalemailids, externalemailids FROM audit.notifications WHERE jobname LIKE '%" + jobname + "%' LIMIT 30", conn)
    return df

def get_extract(file, jobname):
    df = pd.read_sql("SELECT " + file + " FROM audit.extracts WHERE key LIKE '%" + jobname + "%' LIMIT 30", conn)
    return df

def get_rally(file, jobname):
    df = pd.read_sql("SELECT " + file + " FROM audit.rally_extracts WHERE key LIKE '%" + jobname + "%' LIMIT 30", conn)
    return df

def get_planscheduledstarttime(jobname):
    df = pd.read_sql("SELECT jobname, planscheduledstarttime FROM audit.sla WHERE jobname LIKE '%" + jobname + "%' LIMIT 30", conn)
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

def get_valid_email_status(estatus):
    estatus.lower()
    if (estatus.find("initiate")!=-1 or estatus.find("start")!=-1):
        estatus="started"
    elif (estatus.find("finish")!=-1 or estatus.find("complete")!=-1):
        estatus="completed"
    elif (estatus.find("fail")!=-1):
        estatus="failed"
    elif (estatus.find("restart")!=-1):
        estatus="restarted"
    return estatus

def get_valid_date(date):
    try:
        if (date == ""):
            return date
        elif (date.find("today")!=-1):
            valid_date = datetime.date.today()
        elif (date.find("yesterday")!=-1):
            valid_date = datetime.date.today() - datetime.timedelta(days = 1)
        elif (date.find("day-before-yesterday")!=-1):
            valid_date = datetime.date.today() - datetime.timedelta(days = 2)
        elif (date.find("tomorrow")!=-1):
            valid_date = datetime.date.today() + datetime.timedelta(days = 1)
        elif (date.find("day after tomorrow")!=-1):
            valid_date = datetime.date.today() + datetime.timedelta(days = 2)
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

def get_valid_file(file):
    file.lower()
    if (file.find("format")!=-1 or file.find("fmt")!=-1 or file.find("frmt")!=-1):
        file="file_frmt"
    elif (file.find("location")!=-1 or file.find("loc")!=-1 or file.find("path")!=-1):
        file="file_loc"
    elif (file.find("nm")!=-1 or file.find("name")!=-1):
        file="file_nm"
    elif (file.find("type")!=-1):
        file="file_type"
    elif (file.find("freq")!=-1 or file.find("frequency")!=-1):
        file="freq"
    return file
