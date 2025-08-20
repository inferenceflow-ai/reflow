import asyncio
import logging
import multiprocessing
import os
from multiprocessing import Process
from typing import Iterator, List

from reflow.internal import network
from reflow.internal.worker import KeyBasedRoutingPolicy
from reflow import flow_connector_factory, EventSource, EventSink
from reflow.cluster import FlowCluster
from reflow.flow_engine import FlowEngine
from reflow.typedefs import EndOfStreamException

test_data = ["alpha", "beta", "gamma", "delta", "epsilon"]

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

source = EventSource(iterator_source(test_data)).with_producer_fn(iterator_producer)
sink = EventSink().with_consumer_fn(debug_consumer)
source.send_to(sink, routing_policy=KeyBasedRoutingPolicy(lambda event: event))

def engine_runner(cluster_number: int, cluster_size: int, port: int):
    asyncio.run(run_engine(cluster_number, cluster_size, port))

async def run_engine(cluster_number: int, cluster_size: int, port: int):
    with FlowEngine(cluster_number=cluster_number,
                    cluster_size=cluster_size,
                    default_queue_size=100,
                    port=port,
                    preferred_network='127.0.0.1') as engine:
        await engine.run()

async def main(addrs: List[str]):
    cluster = FlowCluster(addrs)
    job_id = await cluster.deploy(source)
    await cluster.wait_for_completion(job_id, 2)
    await cluster.request_shutdown()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    preferred_network = '127.0.0.1'
    engine_ports = [5001, 5002]
    engine_addresses = [network.ipc_address_for_port(p) for p in engine_ports]
    multiprocessing.set_start_method('fork')
    engine_procs = [Process(target=engine_runner, args=[n, len(engine_addresses), p])
                    for n, p in enumerate(engine_ports)]
    for proc in engine_procs:
        proc.start()

    asyncio.run(main(engine_addresses))
    logging.info("Waiting for engines to stop")

    for proc in engine_procs:
        proc.join()