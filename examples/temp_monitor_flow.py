import asyncio
import logging
import random
from typing import List, Tuple

from reflow.internal.worker import KeyBasedRoutingPolicy
from reflow import EventSource, EventSink, EventTransformer
from reflow.cluster import FlowCluster
from reflow.typedefs import EndOfStreamException


#
# Note
#
# Run `docker compose up -d` to start a 2 node cluster before running this example
#


class RandomWalkTempSimulator:
    """
    Emulates a stream of randomly fluctuating temperature observations
    """

    def __init__(self, initial_temp: float, steps: List[float], max_count: int = 0):
        """

        Args:
            initial_temp:  the initial value of the temperature
            steps:         the possible increments (or decrements) to apply at each step - the value returned from
                           next() will be the present temp plus a randomly selected increment
            max_count:     the maximum number of values to produce - calling next after this will yield  an
                            StopIterationException
        """
        self.temp = initial_temp
        self.steps = steps
        self.count = 0
        self.max_count = max_count

    def next(self, limit: int) -> List[float]:
        num_to_produce = min(limit, self.max_count - self.count) if self.max_count > 0 else limit
        if num_to_produce == 0:
            logging.info("END OF STREAM")
            raise EndOfStreamException

        result = []
        temp = self.temp
        for _ in range(num_to_produce):
            temp = self.temp + random.choice(self.steps)
            result.append(temp)

        self.temp = temp
        self.count += num_to_produce
        return result

class MachineShopSimulator:
    def __init__(self, machine_count: int, max_events_per_machine: int):
        self.machine_count = machine_count
        self.machines = [RandomWalkTempSimulator(98, [-2.0, -1.0, 0.0, 1.0, 2.0], max_events_per_machine) for _ in range(machine_count)]

    def next(self, limit: int) -> List[Tuple[int, float]]:
        if limit <= self.machine_count:
            n = random.randint(0, self.machine_count - 1)
            temps = self.machines[n].next(limit)
            return [(n, t) for t in temps]
        else:
            limit_per_machine = int( limit / self.machine_count)
            result = []
            for n, machine in enumerate(self.machines):
                events = machine.next(limit_per_machine)
                result.extend([(n, event) for event in events])

            return result


class ChangeDetector:
    def __init__(self):
        self.machines = {}

    def filter_event(self, event: Tuple[int, float, str])->List[Tuple[int, float, str]]:
        machine, temp, status = event
        if machine not in self.machines:
            self.machines[machine] = status
            return [event]
        else:
            prev_status = self.machines[machine]
            if status == prev_status:
                return []
            else:
                self.machines[machine] = status
                return [event]

def categorize_temp(event: Tuple[int, float])->List[Tuple[int, float, str]]:
    machine, temp = event
    if temp < 97.0:
        status = "COLD"
    elif temp > 99.0:
        status = "HOT"
    else:
        status = "NOMINAL"

    return [(machine, temp, status)]


def print_sink(events: List[str]) -> int:
    for event in events:
        logging.info(f'EVENT: {event}')

    return len(events)

# NOTE
#
# All flow modules must have a function called "create_flow" which returns a connected set of FLowStages describing a
# flow.
#

def create_flow()->EventSource:
    source = EventSource(lambda: MachineShopSimulator(5,100), max_workers=1).with_producer_fn(MachineShopSimulator.next)
    categorize = EventTransformer(expansion_factor=1.0).with_transform_fn(categorize_temp)
    change_detector = EventTransformer(init_fn=ChangeDetector, expansion_factor=1.0).with_transform_fn(ChangeDetector.filter_event)
    sink = EventSink().with_consumer_fn(print_sink)
    source.send_to(categorize, routing_policy=KeyBasedRoutingPolicy(lambda event: event[0])).send_to(change_detector).send_to(sink)
    return source

if __name__ == '__main__':
    engine_addresses = ['tcp://localhost:5001', 'tcp://localhost:5002']

    async def deploy_and_wait_for_flow(engine_addresses: List[str], flow: EventSource, timeout_secs: int):
        flow_cluster = FlowCluster(engine_addresses)
        job_id = await flow_cluster.deploy(flow)
        await flow_cluster.wait_for_completion(job_id, timeout_secs)


    logging.basicConfig(level=logging.INFO)
    flow = create_flow()
    asyncio.run(deploy_and_wait_for_flow(engine_addresses, flow, 10))
