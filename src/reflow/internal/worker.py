import asyncio
from enum import Enum
from typing import Callable, Awaitable, TypeVar, Generic, List, Optional, Self, Any

EVENT_TYPE = TypeVar('EVENT_TYPE')
IN_EVENT_TYPE = TypeVar('IN_EVENT_TYPE')
OUT_EVENT_TYPE = TypeVar('OUT_EVENT_TYPE')
STATE_TYPE = TypeVar('STATE_TYPE')


class INSTRUCTION(Enum):
    PROCESS_EVENT = 1
    END_OF_SOURCE = 2


class Envelope(Generic[EVENT_TYPE]):
    def __init__(self, event: EVENT_TYPE):
        self.instruction = INSTRUCTION.PROCESS_EVENT
        self.event = event

# NOTE to self
# Do we even need to support multiple workers from the same FlowStage ?

class Worker:
    def __init__(self, *,
                 init_fn: Callable[[],Awaitable[STATE_TYPE]] = None,
                 transform_fn: Callable[[List[Envelope[IN_EVENT_TYPE]], Optional[STATE_TYPE]], List[Envelope[OUT_EVENT_TYPE]]] = None,
                 produce_fn:  Callable[[int, Optional[STATE_TYPE]], Awaitable[List[Envelope[OUT_EVENT_TYPE]]]] = None,
                 consume_fn: Callable[[List[Envelope[IN_EVENT_TYPE]], Optional[STATE_TYPE]], Awaitable[None]] = None,
                 input_queue_size: int = 0,
                 next_workers: List[List[Self]] = None,  # typing.Self requires 3.11+
                 routing_key_extractor: Callable[[OUT_EVENT_TYPE], int] = None,
                 flatten: bool = False):
        self.state = None
        self.init_fn = init_fn
        self.transform_fn = transform_fn
        self.produce_fn = produce_fn
        self.consume_fn = consume_fn
        if input_queue_size > 0:
            self.input_queue = asyncio.Queue(maxsize=input_queue_size)
        else:
            self.input_queue = None

        self.input_queue = asyncio.Queue(maxsize=10000)
        self.next_workers = next_workers if next_workers else []
        self.routing_key_extractor = routing_key_extractor
        self.flatten = flatten

    async def init(self):
        if self.init_fn and not self.state:
            self.state = await self.init_fn()


    async def enqueue_events(self, events: List[Envelope[Any]])->None:
        for event in events:
            await self.input_queue.put(event)

    # Note: one flaw here is that perhaps pure transforms shouldn't be async ?
    async def process(self, max_event_count:int):
        ready_events = []

        if self.produce_fn:
            if self.state:
                ready_events = await self.produce_fn(max_event_count, self.state)
            else:
                ready_events = await self.produce_fn(max_event_count)
        else:
            try:
                for _ in range(max_event_count):
                    ready_events.append(self.input_queue.get_nowait())
            except StopIteration:
                pass

        if len(ready_events) == 0:
            return          # RETURN if there are no events to process

        if self.transform_fn:
            results = self.transform_fn(ready_events, self.state)
        else:
            results = ready_events

        if self.flatten:
            results = [item for sublist in results for item in sublist]

        if self.consume_fn:
            if self.state:
                await self.consume_fn(results, self.state)
            else:
                await self.consume_fn(results)
        elif self.next_workers and len(results) > 0:
            for task_group in self.next_workers:
                i = 0
                n = len(task_group)
                task_lists = [[] for _ in task_group]
                for result in results:
                    if self.routing_key_extractor:
                        s = hash(self.routing_key_extractor(result)) % n
                    else:
                        i += 1
                        s = i % n

                    task_lists[s].append(result)

                for t, task in enumerate(task_group):
                    await task.enqueue_events(task_lists[t])

    def add_worker_group(self, workers: List[Self]):
        if not self.next_workers:
            self.next_workers = []

        # self.next_workers is a list of lists.  The inner lists are composed of groups of similar workers
        # (i.e. workers that implement the same logical FlowStage)
        self.next_workers.append(workers)