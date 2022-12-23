import asyncio

from prefect import flow, task
from prefect import flow
from prefect import get_client
from datetime import datetime

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


@task
async def pull_facebook_clients():
    print("started task pull_facebook_clients")
    return clients.keys()


@flow(name="facebook_stats_pull")
async def facebook_stats_pull():
    print("inside facebook_stats_pull flow")
    client_ids = await pull_facebook_clients()
    for client_id in client_ids:
        print("calling")
        dep_id = '7c97e4ab-d34e-4730-a88c-9aafec0b686a'
        param = {'client_id': client_id}
        async with get_client() as client:
            await client.create_flow_run_from_deployment(dep_id, parameters=param)
    print("Flow completed ", clients)


if __name__ == "__main__":
    asyncio.run(facebook_stats_pull())
