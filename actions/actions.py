from typing import Any, Text, Dict, List
from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher

from functions import get_valid_status
from functions import get_valid_date

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
        if (df.size is 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying " + limit + " rows from the DB, here are your results \n" + str(df))
        return []

from functions import show_status
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
        if (df.size is 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results \n" + str(df))
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

        df = get_all_jobs(get_valid_date(date))
        if (df.size is 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results \n" + str(df))
        return []
    
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

        df = get_by_job_status(get_valid_date(date), get_valid_status(status))
        if (df.size is 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results \n" + str(df))
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

        df = get_by_product(get_valid_date(date), get_valid_status(status), product)
        if (df.size is 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results \n" + str(df))
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

        df = get_by_name(get_valid_date(date), get_valid_status(status), jobname)
        if (df.size is 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results \n" + str(df))
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

        df = get_incident(get_valid_date(date))
        if (df.size is 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results \n" + str(df))
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

        df = get_restart(get_valid_date(date))
        if (df.size is 0):
            dispatcher.utter_message(text = "Hey, result not found! Either your input is wrong, or no records exist for this request.\n")
            return []
        dispatcher.utter_message(text = "Hey, so we are querying rows from the DB, here are your results \n" + str(df))
        return []
