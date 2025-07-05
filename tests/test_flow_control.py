import asyncio
import logging
from typing import List, TypeVar, Generic

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


class ListSink(Generic[T]):
    def __init__(self, consumed_events: List[T]):
        self.results = consumed_events

    def slow_sink(self, events: List[T])->int:
        if len(events) == 0:
            return 0

        consumed_events = max(int(len(events) / 2), 1)
        self.results.extend(events[0:consumed_events])

        return consumed_events


@flow_connector_factory
def data_source(data):
    return ListSource(data)


@flow_connector_factory
def sink(consumed_events: List[int]):
    return ListSink(consumed_events)


TEST_EVENT_COUNT = 100
consumed_event_list = []
source_event_list = [i for i in range(TEST_EVENT_COUNT)]

async def main():
    event_source = EventSource(data_source(source_event_list)).with_producer_fn(ListSource.get_data)
    event_sink = EventSink(sink(consumed_event_list)).with_consumer_fn(ListSink.slow_sink)
    event_source.send_to(event_sink)

    with FlowEngine(default_queue_size=32, bind_addresses=['ipc://5555']) as flow_engine:
        await flow_engine.deploy(event_source)
        flow_engine_task = asyncio.create_task(flow_engine.run())
        await flow_engine.request_shutdown()
        await flow_engine_task


logging.basicConfig(level=logging.INFO)
asyncio.run(main())


def test_slow_sink():
    assert consumed_event_list == source_event_list