import asyncio
import logging
from typing import Iterator, List

from reflow.flow_engine import FlowEngine, FlowEngineClient
from reflow import flow_connector_factory, EventSource, EventSink
from reflow.cluster import FlowCluster
from reflow.typedefs import EndOfStreamException

test_data = ["alpha", "beta", "gamma"]

@flow_connector_factory
def iterator_source(data: List[str])->Iterator[str]:
    return iter(data)


# noinspection PyUnusedLocal
def iterator_producer(it: Iterator[str], limit: int)->List[str]:
    try:
        result = next(it)
        logging.info(f"PRODUCING: {result}")
        return [result]
    except StopIteration:
        logging.info("END OF STREAM")
        raise EndOfStreamException

def debug_consumer(events: List[str])->int:
    for event in events:
        logging.info(f'CONSUMING: {event}')

    return len(events)

source = EventSource(iterator_source(test_data)).with_producer_fn(iterator_producer)
sink = EventSink().with_consumer_fn(debug_consumer)
source.send_to(sink)


async def main():
    preferred_network = '127.0.0.1'
    with FlowEngine(100, bind_addresses = ['ipc:///tmp/service_5001.sock'], preferred_network=preferred_network) as engine:
        engine_task = asyncio.create_task(engine.run())
        cluster = FlowCluster(engine_addresses=['ipc:///tmp/service_5001.sock'], preferred_network=preferred_network)
        await cluster.deploy(source)
        logging.info('Deployed flow')

        await engine.request_shutdown()
        await engine_task


logging.basicConfig(level=logging.INFO)
asyncio.run(main())