from datetime import datetime
from prefect import task
from data import clients
import time

AN_IMPORTED_MESSAGE = "Hello from another file"


@task
def pull_facebook_clients():
    print("started task pull_facebook_clients")
    time.sleep(1)
    return clients.keys()


@task(name="fetch_all_report_of_client" + datetime.now().strftime("%H:%M:%S"))
def fetch_all_report_of_client(client_id: str):
    print("fetch_all_report_of_client", client_id)
    time.sleep(1)
    return clients.get(client_id).get('reports')


@task(name="fetch_report_per_client" + datetime.now().strftime("%H:%M:%S"))
def fetch_report_per_client(client_id: str, report_id: str):
    print("inside fetch_report_per_client client_id:{} report_id", client_id, report_id)
    time.sleep(1)
    for report in clients.get(client_id).get('reports'):
        if report.get('id') == report_id:
            report.__setitem__('status', 'Success')
            report.__setitem__('fetched_date', datetime.now())
            return "success"
    return "failure"
