import asyncio
import logging
import multiprocessing
import os
from multiprocessing import Process
from typing import Iterator, List

from reflow import flow_connector_factory, EventSource, EventSink
from reflow.cluster import FlowCluster
from reflow.flow_engine import FlowEngine
from reflow.typedefs import EndOfStreamException

test_data = ["alpha", "beta", "gamma"]

@flow_connector_factory
def iterator_source(data: List[str])->Iterator[str]:
    return iter(data)


# noinspection PyUnusedLocal
def iterator_producer(it: Iterator[str], limit: int)->List[str]:
    try:
        result = next(it)
        annotated_result = f'{result} FROM ({os.getpid()})'
        logging.info(f"({os.getpid()}) PRODUCING: {result}")
        return [annotated_result]
    except StopIteration:
        logging.info(f"({os.getpid()}) END OF STREAM")
        raise EndOfStreamException

def debug_consumer(events: List[str])->int:
    for event in events:
        logging.info(f'({os.getpid()}) CONSUMING: {event}')

    return len(events)

source = EventSource(iterator_source(test_data), max_workers=1).with_producer_fn(iterator_producer)
sink = EventSink().with_consumer_fn(debug_consumer)
source.send_to(sink)

def engine_runner(bind_address: str):
    asyncio.run(run_engine(bind_address))

async def run_engine(bind_address: str):
    with FlowEngine(100, bind_addresses=[bind_address], preferred_network='127.0.0.1') as engine:
        await engine.run()

async def main(addrs: List[str]):
    cluster = FlowCluster(addrs, preferred_network='127.0.0.1')
    await cluster.deploy(source)
    await cluster.request_shutdown()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    preferred_network = '127.0.0.1'
    engine_addresses = ['ipc:///tmp/service_5001.sock', 'ipc:///tmp/service_5002.sock']
    multiprocessing.set_start_method('fork')
    engine_procs = [Process(target=engine_runner, args=[address]) for address in engine_addresses]
    for proc in engine_procs:
        proc.start()

    asyncio.run(main(engine_addresses))
    logging.info("Waiting for engines to stop")

    for proc in engine_procs:
        proc.join()