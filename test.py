import asyncio
import copy
from prefect import flow

@flow
async def subflow_1(a):
    print("Subflow 1 started!" + a)
    await asyncio.sleep(1)
    print("Subflow 1 completed!" + a)


@flow
async def main_flow():
    parallel_subflows = [subflow_1('a'), copy.deepcopy(subflow_1)('b')]
    await asyncio.gather(*parallel_subflows)

if __name__ == "__main__":
    main_flow_state = asyncio.run(main_flow())