import asyncio
import logging
from abc import ABC, abstractmethod
from asyncio import QueueEmpty, QueueFull
from enum import Enum
from typing import Generic, List, Self, Any

from typedefs import IN_EVENT_TYPE, SplitFn,  EVENT_TYPE, STATE_TYPE, InitFn, ProducerFn, ConsumerFn, OUT_EVENT_TYPE

MAX_INPUT_QUEUE_SIZE = 10000

class INSTRUCTION(Enum):
    PROCESS_EVENT = 1
    END_OF_SOURCE = 2


class Envelope(Generic[EVENT_TYPE]):
    def __init__(self, instruction: INSTRUCTION, event: EVENT_TYPE = None):
        self.instruction = instruction
        self.event = event


class Worker(ABC, Generic[STATE_TYPE, OUT_EVENT_TYPE]):
    def __init__(self, init_fn: InitFn = None, expansion_factor = 1):
        self.init_fn = init_fn
        self.state = None
        self.input_queue = asyncio.Queue(maxsize=MAX_INPUT_QUEUE_SIZE)
        self.next_workers = []
        self.routing_key_extractor = None
        self.expansion_factor = expansion_factor

    async def init(self):
        if self.init_fn and not self.state:
            self.state = await self.init_fn()

    # note that this is async because, in general, using the underlying queue storage mechanism may involve network IO
    async def enqueue_events(self, events: List[Any])->int:
        enqueued = 0
        try:
            for event in events:
                self.input_queue.put_nowait(Envelope(INSTRUCTION.PROCESS_EVENT, event))
                enqueued += 1
        except QueueFull:
            logging.warn(f"Queue Full: {self}")

        return enqueued

    def queue_capacity(self)->int:
        return self.input_queue.maxsize - self.input_queue.qsize()

    def input_batch_size(self, unbounded_input: bool = False)->int:
        input_queue_size = MAX_INPUT_QUEUE_SIZE if unbounded_input else self.input_queue.qsize()
        downstream_capacity = MAX_INPUT_QUEUE_SIZE
        for worker_group in self.next_workers:
            for worker in worker_group:
                downstream_capacity = min(downstream_capacity, worker.queue_capacity())

        return int( min( int(downstream_capacity/2), input_queue_size) / self.expansion_factor)

    def get_ready_events(self)->List[IN_EVENT_TYPE]:
        event_envelopes = []
        batch_size = self.input_batch_size()
        try:
            for _ in range(batch_size):
                event_envelopes.append(self.input_queue.get_nowait())
        except QueueEmpty:
            pass

        return  [envelope.event for envelope in event_envelopes if envelope.instruction == INSTRUCTION.PROCESS_EVENT]

    def add_worker_group(self, workers: List[Self]):
        self.next_workers.append(workers)



    async def dispatch(self, events: List[OUT_EVENT_TYPE]):
        if self.next_workers and len(events) > 0:
            for worker_group in self.next_workers:
                i = 0
                n = len(worker_group)
                # worker_event_lists is a list of events for each worker in this group
                # each event is dispatcher to one worker in the group
                worker_event_lists = [[] for _ in worker_group]
                for event in events:
                    if self.routing_key_extractor:
                        s = hash(self.routing_key_extractor(event)) % n
                    else:
                        i += 1
                        s = i % n

                    worker_event_lists[s].append(event)

                for t, worker in enumerate(worker_group):
                    await worker.enqueue_events(worker_event_lists[t])

    @abstractmethod
    async def process(self)->None:
        pass


class SourceWorker(Worker, Generic[EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *, producer_fn: ProducerFn, init_fn: InitFn = None):
        Worker.__init__(self, init_fn)
        self.producer_fn = producer_fn

    async def process(self):
        batch_size = self.input_batch_size(True)
        if self.state:
            ready_events = await self.producer_fn(self.state, batch_size)
        else:
            ready_events = await self.producer_fn(batch_size)

        logging.debug(f'{self} producing {len(ready_events)} events')

        if len(ready_events) == 0:
            return  # RETURN if there are no events to process

        await self.dispatch(ready_events)

class SinkWorker(Worker, Generic[EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *, consumer_fn: ConsumerFn, init_fn: InitFn = None):
        Worker.__init__(self, init_fn)
        self.consumer_fn = consumer_fn

    async def process(self):
        events = self.get_ready_events()
        logging.debug(f'{self} processing {len(events)} events')

        if self.state:
            await self.consumer_fn(self.state, events)
        else:
            await self.consumer_fn(events)


class SplitWorker(Worker, Generic[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *, split_fn: SplitFn, expansion_factor: int, init_fn: InitFn = None):
        Worker.__init__(self, init_fn=init_fn, expansion_factor=expansion_factor)
        self.split_fn = split_fn

    async def process(self):
        events = self.get_ready_events()
        logging.debug(f'{self} processing {len(events)} events')

        result = []
        for event in events:
            # TODO - do all workers have to be coroutines ?
            if self.state:
                parts = await self.split_fn(self.state, event)
            else:
                parts = await self.split_fn(event)

            result.extend(parts)

        await self.dispatch(result)


