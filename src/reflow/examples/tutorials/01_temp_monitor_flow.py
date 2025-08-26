import asyncio
import random
from typing import List

from reflow.cluster import FlowCluster
from reflow.common import EndOfStreamException
from reflow import EventSource, EventSink, EventTransformer


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
            raise EndOfStreamException

        result = []
        temp = self.temp
        for _ in range(num_to_produce):
            temp = self.temp + random.choice(self.steps)
            result.append(temp)

        self.temp = temp
        self.count += num_to_produce
        return result


def check_temp(temp: float):
    if temp > 100.0:
        return ["WARNING"]
    else:
        return []


def print_sink(events: List[str]) -> int:
    for event in events:
        print(f'EVENT: {event}')

    return len(events)


async def main():
    source = EventSource(lambda: RandomWalkTempSimulator(98, [-1.0, 0, 1.0], 100)).with_producer_fn(
        RandomWalkTempSimulator.next)
    temp_filter = EventTransformer(expansion_factor=1.0).with_transform_fn(check_temp)
    sink = EventSink().with_consumer_fn(print_sink)
    source.send_to(temp_filter).send_to(sink)

    flow_cluster = FlowCluster(['tcp://localhost:5001'])
    job_id = await flow_cluster.deploy(source)
    # await flow_cluster.wait_for_completion(job_id, 10)

asyncio.run(main())
