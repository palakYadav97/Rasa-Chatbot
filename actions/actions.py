# This files contains your custom actions which can be used to run
# custom Python code.
#
# See this guide on how to implement these action:
# https://rasa.com/docs/rasa/custom-actions


# This is a simple example for a custom action which utters "Hello World!"

# from typing import Any, Text, Dict, List
#
# from rasa_sdk import Action, Tracker
# from rasa_sdk.executor import CollectingDispatcher
#
#
# class ActionHelloWorld(Action):
#
#     def name(self) -> Text:
#         return "action_hello_world"
#
#     def run(self, dispatcher: CollectingDispatcher,
#             tracker: Tracker,
#             domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
#
#         dispatcher.utter_message(text="Hello World!")
#
#         return []

from typing import Any, Text, Dict, List

from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher

from functions import limit_query
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
        dispatcher.utter_message(text="Hey, so we are querying "+limit+" rows from the DB, here are your results \n"+str(df))
        return []

from functions import show_status
class QueryByDate(Action):
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

        df = show_status(date, jobname)
        dispatcher.utter_message(text="Hey, so we are querying rows from the DB, here are your results \n"+str(df))
        return []

from functions import get_all_jobs
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

        df = get_all_jobs(date)
        dispatcher.utter_message(text="Hey, so we are querying rows from the DB, here are your results \n"+str(df))
        return []
    
from functions import get_valid_status

from functions import get_by_job_status
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

        df = get_by_job_status(date, get_valid_status(status))
        dispatcher.utter_message(text="Hey, so we are querying rows from the DB, here are your results \n"+str(df))
        return []

from functions import get_by_product
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

        df = get_by_product(date, get_valid_status(status), product)
        dispatcher.utter_message(text="Hey, so we are querying rows from the DB, here are your results \n"+str(df))
        return []

from functions import get_by_name
class QueryAllByDateStatusAndJobName(Action):
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
        jobname = ""
        for t in tracker.latest_message['entities']:
            if t['entity'] == 'jobname':
                jobname = t['value']

        df = get_by_name(date, get_valid_status(status), jobname)
        dispatcher.utter_message(text="Hey, so we are querying rows from the DB, here are your results \n"+str(df))
        return []

from functions import get_incident
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

        df = get_incident(date)
        dispatcher.utter_message(text="Hey, so we are querying rows from the DB, here are your results \n"+str(df))
        return []

from functions import get_restart
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

        df = get_restart(date)
        dispatcher.utter_message(text="Hey, so we are querying rows from the DB, here are your results \n"+str(df))
        return []


from datetime import date
import pandas as pd



class StatusJobQuery(Action):

    def name(self) -> Text:
        return "action_get_query_report_of_all_jobs_today"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:


        today = date.today()
        d1 = today.strftime("%Y-%m-%d")
        df = pd.read_sql("select * from audit.jobtracking where startts like '%"+d1+"%'", conn)

        dispatcher.utter_message(text="Hey, so the data we have is: \n"+df)

        return []
