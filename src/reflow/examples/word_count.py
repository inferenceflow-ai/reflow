import asyncio
import logging
import multiprocessing
import random
from collections import defaultdict
from multiprocessing import Process
from typing import List, TypeVar, Generic, Tuple, Optional, Mapping, Any

from internal.worker import KeyBasedRoutingPolicy
from reflow import flow_connector_factory, EventSource, EventSink, EventTransformer
from reflow.cluster import FlowCluster
from reflow.flow_engine import FlowEngine
from reflow.typedefs import EndOfStreamException

hamlet_sentences =  [
"""To be, or not to be, that is the question:
Whether 'tis nobler in the mind to suffer
The slings and arrows of outrageous fortune,
Or to take arms against a sea of troubles
And by opposing end them.""",
"""To die—to sleep,
No more; and by a sleep to say we end
The heart-ache and the thousand natural shocks
That flesh is heir to: 'tis a consummation
Devoutly to be wish'd. """,
"""To die, to sleep;
To sleep, perchance to dream—ay, there's the rub:
For in that sleep of death what dreams may come,
When we have shuffled off this mortal coil,
Must give us pause—there's the respect
That makes calamity of so long life.""",
"""For who would bear the whips and scorns of time,
Th'oppressor's wrong, the proud man's contumely,
The pangs of dispriz'd love, the law's delay,
The insolence of office, and the spurns
That patient merit of th'unworthy takes,
When he himself might his quietus make
With a bare bodkin?""",
"""Who would fardels bear,
To grunt and sweat under a weary life,
But that the dread of something after death,
The undiscovere'd country, from whose bourn
No traveller returns, puzzles the will,
And makes us rather bear those ills we have
Than fly to others that we know not of?""",
"""Thus conscience doth make cowards of us all,
And thus the native hue of resolution
Is sicklied o'er with the pale cast of thought,
And enterprises of great pith and moment
With this regard their currents turn awry
And lose the name of action"""]

T = TypeVar("T")

class TestDataConnection(Generic[T]):
    """
    Given a list of any type, the TestDataConnection implements  get_events from the InputQueue
    """
    def __init__(self, data: List[T], stop_after: int = 0):
        self.data = data
        self.count = 0
        self.stop_after = stop_after

    # noinspection PyTypeChecker
    def get_data(self, max_items: int)->List[Tuple[int, T]]:
        assert max_items > 0
        if self.stop_after > 0:
            max_items = min(max_items, self.stop_after - self.count)

        if max_items > 0:
            ids = range(self.count, self.count + max_items)
            self.count += max_items
            return zip(ids, random.choices(self.data, k=max_items))

        if self.stop_after > 0:
            raise EndOfStreamException()


def split(id_sentence_tuple: Tuple[int,str])->List[Tuple[int, Optional[str]]]:
    request_id, sentence = id_sentence_tuple
    result: List[Tuple[int, Optional[str]]] = [(request_id, word) for word in sentence.split()]
    result.append((request_id, None))  # this event servers as a marker for downstream stages that the
                                       # batch has concluded
    return result


class WordCounter:
    def __init__(self):
        self.words = defaultdict(lambda :  defaultdict(int))

    def accumulate(self, event: Tuple[int, Optional[str]])->List[Tuple[int, Mapping[str, int]]]:
        event_id, word = event
        if word:
            count = self.words[event_id][word]
            self.words[event_id][word] = count + 1
            return []
        else:
            result = self.words[event_id]
            del self.words[event_id]
            return [(event_id, result)]


@flow_connector_factory
def data_source(data, stop_after):
    return TestDataConnection(data, stop_after)

def debug_sink(events: List[Any])-> int:
    for event in events:
        print(f'EVENT: {event}')

    return len(events)


def engine_runner(cluster_number: int, cluster_size: int, bind_address: str):
    asyncio.run(run_engine(cluster_number, cluster_size, bind_address))

async def run_engine(cluster_number: int, cluster_size: int, bind_address: str):
    with FlowEngine(cluster_number=cluster_number,
                    cluster_size=cluster_size,
                    default_queue_size=100,
                    bind_addresses=[bind_address],
                    preferred_network='127.0.0.1') as engine:
        await engine.run()

async def main(addrs: List[str]):
    source = EventSource(init_fn=data_source(hamlet_sentences, 3), max_workers=1).with_producer_fn(TestDataConnection.get_data)
    splitter = EventTransformer(expansion_factor=40).with_transform_fn(split)
    counter = EventTransformer(init_fn=WordCounter, expansion_factor=1/40).with_transform_fn(WordCounter.accumulate)
    sink = EventSink().with_consumer_fn(debug_sink)
    source.send_to(splitter).send_to(counter, routing_policy=KeyBasedRoutingPolicy(lambda event: event[0])).send_to(sink)

    cluster = FlowCluster(addrs, preferred_network='127.0.0.1')
    job_id = await cluster.deploy(source)

    await cluster.wait_for_completion(job_id, 10)
    await cluster.request_shutdown()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    preferred_network = '127.0.0.1'
    engine_addresses = ['ipc:///tmp/service_5001.sock', 'ipc:///tmp/service_5002.sock']
    multiprocessing.set_start_method('fork')
    engine_procs = [Process(target=engine_runner, args=[n, len(engine_addresses), address]) for n, address in enumerate(engine_addresses)]
    for proc in engine_procs:
        proc.start()

    asyncio.run(main(engine_addresses))
    logging.info("Waiting for engines to stop")

    for proc in engine_procs:
        proc.join()
