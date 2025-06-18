import asyncio
import logging
import random
import time
from typing import List, TypeVar, Generic

from reflow import flow_connector_factory, EventSource, EventSink, Splitter
from reflow.local_flow_engine import FlowEngine
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
    def __init__(self, data: List[T], stop_after: int = 0):
        self.data = data
        self.count = 0
        self.stop_after = stop_after

    def get_data(self, max_items: int)->List[T]:
        assert max_items > 0
        if self.stop_after > 0:
            max_items = min(max_items, self.stop_after - self.count)

        if max_items > 0:
            self.count += max_items
            return random.choices(self.data, k=max_items)

        if self.stop_after > 0:
            raise EndOfStreamException()


@flow_connector_factory
def data_source(data, stop_after):
    return TestDataConnection(data, stop_after)

def debug_sink(events: List[str])-> int:
    for event in events:
        print(f'EVENT: {event}')

    return len(events)

word_count = 0

class CountingSink:
    def __init__(self):
        pass

    def sink(self, events: List[str])->int:
        global word_count
        result = len(events)
        word_count += result
        return result


@flow_connector_factory
def new_counting_sink():
    return CountingSink()


# noinspection PyUnusedLocal
def null_sink(events: List[str])-> None:
    pass


def split_fn(sentence):
    return sentence.split()

async def main():
    t1 = time.perf_counter(), time.process_time()
    source = EventSource(data_source(hamlet_sentences, 100_000)).with_producer_fn(TestDataConnection.get_data)
    splitter = Splitter(expansion_factor=40).with_split_fn(split_fn)
    sink = EventSink(new_counting_sink()).with_consumer_fn(CountingSink.sink)
    source.send_to(splitter).send_to(sink)

    flow_engine = FlowEngine()
    await flow_engine.deploy(source)
    await flow_engine.shutdown_when_done()
    t2 = time.perf_counter(), time.process_time()
    elapsed = t2[0] - t1[0], t2[1] - t1[1]
    logging.info(f'COMPLETED in {elapsed[0]:.03f}s CPU: {elapsed[1]:.03f}  WORDS: {word_count:,d}')
    print(f'COMPLETED in {elapsed[0]:.03f}s CPU: {elapsed[1]:.03f}  WORDS: {word_count:,d}')


logging.basicConfig(level=logging.DEBUG, filename="wordcount.log")
asyncio.run(main())


# The back-pressure algorithm
# - check output event list
# - if there are no events in the output event list
#   - read some number of events from input
#   - process them, yielding output events which are placed in an output event list
#   - record the output events in the in-out map
#
#  - deliver as many as possible of the output events to the output queue (usually all)
#  - remove those events from the output event list
#  - based on the in-out map, acknowledge the appropriate number of events from the input queue
#  - trim the in/out map
#  - after this, some events may remain in the output event list
#
# Note that delivery guarantees are handled at a higher level, not by the algorithm above.  If there is a processor
# failure, the whole processing loop (above) starts again, beginning with the first unacknowledged event in the input
# queue. Depending on when the failure occurred, there could be:
# - events that were processed but not sent down stream - resulting in the event being reprocessed here but not
#   in the down stream tasks
# - events that were processed and sent down stream but not acknowledged on the input side - resulting in events
#   that are reprocessed here and also in the downstream processors





