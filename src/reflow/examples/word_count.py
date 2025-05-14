import asyncio
import logging
import random
from logging import DEBUG
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

async def debug_sink(events: List[str])-> None:
    for event in events:
        print(f'EVENT: {event}')

async def null_sink(events: List[str])-> None:
    pass


async def split_fn(sentence):
    return sentence.split()

async def main():
    source = EventSource(data_source(hamlet_sentences)).with_producer_fn(TestDataConnection.get_data)
    splitter = Splitter(expansion_factor=50).with_split_fn(split_fn)
    sink = EventSink().with_consumer_fn(debug_sink)
    source.send_to(splitter).send_to(sink)

    flow_engine = LocalFlowEngine()
    await flow_engine.run(source)

logging.basicConfig()
asyncio.run(main())

# TODO
#
# 1. Need to deal with QueueFull
# 2. Expansion factor is critical to flow control - is there a better way to compute it ?
