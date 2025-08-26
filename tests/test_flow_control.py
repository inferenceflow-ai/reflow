import asyncio
import logging
from typing import List, TypeVar, Generic

from reflow.internal import network
from reflow.cluster import FlowCluster
from reflow import flow_connector_factory, EventSource, EventSink
from reflow.flow_engine import FlowEngine
from reflow.typedefs import EndOfStreamException


T = TypeVar("T")


class ListSource(Generic[T]):
    def __init__(self, data: List[T]):
        self.data = data
        self.next = 0

    def get_data(self, max_items: int)->List[T]:
        if self.next >= len(self.data):
            raise EndOfStreamException()

        limit = min(len(self.data), self.next + max_items)
        result = self.data[self.next:limit]
        self.next = limit
        return result


def slow_sink(events: List[T])->int:
    global consumed_event_list
    if len(events) == 0:
        return 0

    consumed_events = max(int(len(events) / 2), 1)
    consumed_event_list.extend(events[0:consumed_events])

    return consumed_events


@flow_connector_factory
def data_source(data):
    return ListSource(data)


TEST_EVENT_COUNT = 100
consumed_event_list = []
source_event_list = [i for i in range(TEST_EVENT_COUNT)]

async def main():
    event_source = EventSource(data_source(source_event_list)).with_producer_fn(ListSource.get_data)
    event_sink = EventSink().with_consumer_fn(slow_sink)
    event_source.send_to(event_sink)

    with FlowEngine(cluster_number=0,
                    cluster_size=1,
                    default_queue_size=32,
                    port=5001) as flow_engine:
        flow_engine_task = asyncio.create_task(flow_engine.run())
        cluster = FlowCluster(engine_addresses=[network.ipc_address_for_port(flow_engine.port)])
        job_id = await cluster.deploy(event_source)
        completed = await cluster.wait_for_completion(job_id, timeout_secs=100)
        if not completed:
            raise RuntimeError("Exiting because job did not complete in the expected time")

        await flow_engine.request_shutdown()
        await flow_engine_task

    logging.info("exiting")


logging.basicConfig(level=logging.INFO)
asyncio.run(main())


def test_slow_sink():
    assert consumed_event_list == source_event_list