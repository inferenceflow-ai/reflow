import asyncio
import logging
from typing import Iterator, List

from flow_engine import FlowEngine, DeployRequest, FlowEngineClient
from reflow import flow_connector_factory, EventSource, EventSink
from internal.zmq import ZMQClient
from reflow.typedefs import EndOfStreamException

test_data = ["alpha", "beta", "gamma"]

@flow_connector_factory
def iterator_source(data: List[str])->Iterator[str]:
    return iter(data)


# noinspection PyUnusedLocal
def iterator_producer(it: Iterator[str], limit: int)->List[str]:
    try:
        return [next(it)]
    except StopIteration:
        raise EndOfStreamException

def debug_consumer(events: List[str])->int:
    for event in events:
        logging.info(f'EVENT: {event}')

    return len(events)

source = EventSource(iterator_source(test_data)).with_producer_fn(iterator_producer)
sink = EventSink().with_consumer_fn(debug_consumer)
source.send_to(sink)


async def main():
    with FlowEngine(100, ['ipc://5001']) as engine:
        engine_task = asyncio.create_task(engine.run())
        with FlowEngineClient('ipc://5001') as client:
            await client.deploy(source)
            logging.info('Deployed flow')

        await engine.request_shutdown()
        await engine_task


logging.basicConfig(level=logging.INFO)
asyncio.run(main())