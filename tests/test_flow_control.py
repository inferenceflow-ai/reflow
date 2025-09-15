import asyncio
import logging
import os
import sys

from reflow.cluster import FlowCluster
from reflow.flow_engine import FlowEngine
from reflow.internal import network
import flow_control_test

async def main():
    with FlowEngine(cluster_number=0,
                    cluster_size=1,
                    default_queue_size=32,
                    port=5001) as flow_engine:
        flow_engine_task = asyncio.create_task(flow_engine.run())
        cluster = FlowCluster(engine_addresses=[network.ipc_address_for_port(flow_engine.port)])
        job_id = await cluster.deploy(flow_control_test.create_flow())
        completed = await cluster.wait_for_completion(job_id, timeout_secs=100)
        if not completed:
            raise RuntimeError("Exiting because job did not complete in the expected time")

        await flow_engine.request_shutdown()
        await flow_engine_task

    logging.info("exiting")


logging.basicConfig(level=logging.DEBUG)
path = os.path.dirname(__file__)
sys.path.append(path)
asyncio.run(main())

def test_slow_sink():
    assert flow_control_test.consumed_event_list == flow_control_test.source_event_list