import sys
from prefect import flow, task, get_run_logger
from utilities import AN_IMPORTED_MESSAGE, pull_facebook_clients, \
    fetch_all_report_of_client, fetch_report_per_client
from data import clients
import asyncio


@flow(name="facebook_stats_pull")
def facebook_stats_pull():
    print("inside facebook_stats_pull flow")
    client_ids = pull_facebook_clients.submit().result()
    print(client_ids)
    for client_id in client_ids:
        per_client_listener(client_id)
    print("Flow completed ", clients)


@flow(name="per_client_listener")
def per_client_listener(client_id: str):
    print("Inside per_client_listener ", client_id)
    report_ids = fetch_all_report_of_client.submit(client_id).result()
    print(report_ids)
    results = []
    i = 0
    for report in report_ids:
        results.append(fetch_client_report(client_id, report.get("id")))


@flow(name="per_client_report_fetch")
def fetch_client_report(client_id: str, report_id: str):
    print("fetch_client_report fetching report of client :{} , report: {} ", client_id, report_id)
    result = fetch_report_per_client.submit(client_id, report_id)
    return result


if __name__ == "__main__":
    facebook_stats_pull()

# 1. Facebook Stats
#    a.)Google Cloud SCheduler
#       Task Queue - ALl Client Queue
#       Only be a lamda function
#       Lamda Function - all client - 10
#       Find all clients
#       In another queue add a task to fetch facebook data - per client queue
#     b.)Lambda Function - per client - 10 - 5hr
#         For every type of report
#         10 taarikh
#         Add task in report queue - 11 taarikh ka pull kar lo
#         Jab sabka pull ho jaaye to main date 11 kar dun
#     c.) Lamda Function - per report – 3 retry karna
#          Report schedule kar di
#          10 min tak poll karta hai
#          Failure mark kar deta hai
#          Failure
#          Report check karo schedule hai kya
#          Poll karo
# ……
