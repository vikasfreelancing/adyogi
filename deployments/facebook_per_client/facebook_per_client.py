from datetime import datetime

from prefect import flow, task


@flow(name="per_client_listener")
def per_client_listener(client_id: str):
    print("Inside per_client_listener ", client_id)
    report_ids = fetch_all_report_of_client(client_id)
    print(report_ids)
    results = []
    i = 0
    for rep in report_ids:
        results.append(fetch_report_per_client.submit(client_id, rep.get('id')))
    for res in results:
        if res.result == 'failure':
            raise Exception("One of the report is not processed")
    return 'success'


@task(name="fetch_all_report_of_client" + datetime.now().strftime("%H:%M:%S"))
def fetch_all_report_of_client(client_id: str):
    print("fetch_all_report_of_client", client_id)
    return clients.get(client_id).get('reports')


clients = {
    "client_1": {
        "reports": [
            {
                "id": "report_id_1_1",
                "status": "pending",
                "fetched_date": datetime.now(),
            },
            {
                "id": "report_id_1_2",
                "status": "pending",
                "fetched_date": datetime.now(),
            },
            {
                "id": "report_id_1_3",
                "status": "pending",
                "fetched_date": datetime.now(),
            }, {
                "id": "report_id_1_4",
                "status": "pending",
                "fetched_date": datetime.now(),
            }, {
                "id": "report_id_1_5",
                "status": "pending",
                "fetched_date": datetime.now(),
            }, {
                "id": "report_id_1_6",
                "status": "pending",
                "fetched_date": datetime.now(),
            }
        ]
    }, "client_2": {
        "reports": [
            {
                "id": "report_id_2_1",
                "status": "pending",
                "fetched_date": datetime.now(),
            },
            {
                "id": "report_id_2_2",
                "status": "pending",
                "fetched_date": datetime.now(),
            },
            {
                "id": "report_id_2_3",
                "status": "pending",
                "fetched_date": datetime.now(),
            }, {
                "id": "report_id_2_4",
                "status": "pending",
                "fetched_date": datetime.now(),
            }, {
                "id": "report_id_2_5",
                "status": "pending",
                "fetched_date": datetime.now(),
            }, {
                "id": "report_id_2_6",
                "status": "pending",
                "fetched_date": datetime.now(),
            }
        ]
    }, "client_3": {
        "reports": [
            {
                "id": "report_id_3_1",
                "status": "pending",
                "fetched_date": datetime.now(),
            },
            {
                "id": "report_id_3_2",
                "status": "pending",
                "fetched_date": datetime.now(),
            },
            {
                "id": "report_id_3_3",
                "status": "pending",
                "fetched_date": datetime.now(),
            }, {
                "id": "report_id_3_4",
                "status": "pending",
                "fetched_date": datetime.now(),
            }, {
                "id": "report_id_3_5",
                "status": "pending",
                "fetched_date": datetime.now(),
            }, {
                "id": "report_id_3_6",
                "status": "pending",
                "fetched_date": datetime.now(),
            }
        ]
    }}


@task(name="fetch_report_per_client" + datetime.now().strftime("%H:%M:%S"))
def fetch_report_per_client(client_id: str, report_id: str):
    print("inside fetch_report_per_client client_id:{} report_id", client_id, report_id)
    for report in clients.get(client_id).get('reports'):
        if report.get('id') == report_id:
            report.__setitem__('status', 'Success')
            report.__setitem__('fetched_date', datetime.now())
            return "success"
    return "failure"


if __name__ == "__main__":
    per_client_listener('client_1')
