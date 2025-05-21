import asyncio
import logging
from typing import List, TypeVar, Generic

from reflow import flow_connector, EventSource, EventSink, LocalFlowEngine

# a source emits events consisting of the numbers 1-100 in order.
# a sink prints out what it receives.


T = TypeVar("T")

class TestSource(Generic[T]):
    def __init__(self, data: List[T]):
        self.data = data
        self.next = 0

    async def get_data(self, max_items: int)->List[T]:
        if self.next >= len(self.data):
            return []

        limit = min(len(self.data), self.next + max_items)
        result = self.data[self.next:limit]
        self.next = limit
        return result


@flow_connector
async def data_source(data):
    return TestSource(data)


async def slow_debug_sink(events: List[str])-> None:
    limit = int(len(events) / 2)
    for event in events[0:limit]:
        print(f'EVENT: {event}')

    return limit


async def main():
    source = EventSource(data_source([i for i in range(100)])).with_producer_fn(TestSource.get_data)
    sink = EventSink().with_consumer_fn(slow_debug_sink)
    source.send_to(sink)

    flow_engine = LocalFlowEngine(queue_size=32)
    await flow_engine.run(source)

logging.basicConfig(level=logging.DEBUG)
asyncio.run(main())

