import asyncio
import logging
import random
from typing import List, TypeVar, Generic

from reflow import flow_connector, EventSource, EventSink, LocalFlowEngine, Splitter

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
    def __init__(self, data: List[T]):
        self.data = data

    async def get_data(self, max_items: int)->List[T]:
        return random.choices(self.data, k=max_items)

@flow_connector
async def data_source(data):
    return TestDataConnection(data)

async def debug_sink(events: List[str])-> int:
    for event in events:
        print(f'EVENT: {event}')

    return len(events)


# noinspection PyUnusedLocal
async def null_sink(events: List[str])-> None:
    pass


async def split_fn(sentence):
    return sentence.split()

async def main():
    source = EventSource(data_source(hamlet_sentences)).with_producer_fn(TestDataConnection.get_data)
    splitter = Splitter(expansion_factor=1).with_split_fn(split_fn)
    sink = EventSink().with_consumer_fn(debug_sink)
    source.send_to(splitter).send_to(sink)

    flow_engine = LocalFlowEngine()
    await flow_engine.run(source)

logging.basicConfig(level=logging.WARNING)
asyncio.run(main())

# TODO
#
# 1. DONE Need to deal with QueueFull by retrying input events that produced the output events that couldn't be saved.
# 2. Expansion factor is critical to flow control - is there a better way to compute it ?
# 3. If the queue is remote, there will be a cost to checking it's size.  The performance penalty may be unacceptable.
#    In that case, is there a better way to estimate the downstream capacity ?  Can we send it back from an enqueue
#    call and have the upstream estimate ?
# 4. DONE Make the event queue test into actual tests (e.g. using pytest, doctest)
# 5. Support multiple workers in different processes
# 6. Support multiple workers in different machines
# 7. Support ordering
# 8. DONE - Modify event queue to support a dictionary of offsets.  Only discard entries earlier than the earliest
#    acknowledged offset
# 9. Implement internal idempotency via event ids and recent event cache
# 10. I need to move the event counting mechanism into common code
# 11. Think about what, if anything, can be done with sources when all received events can't be saved to the output
#     queue. For this case, the source needs to be rewindable.
# 12. Exception handling around all user provided functions (built into *Worker.process most likely).
# 13. DONE Look at worker.py line 52, shouldn't ProducerFn be using the same type vars as those in worker ? Does it ?
# 14. Sources and Sinks are likely to block the event loop and should be offloaded to a separate thread
# 15. DONE Sinks have no way to exert back-pressure
# 16. Support re-joining - the inverse of splitting
# 17. Refine logging to use different loggers for different parts of the program






