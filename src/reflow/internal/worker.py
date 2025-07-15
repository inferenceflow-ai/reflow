import asyncio
import logging
import os
import uuid
from abc import ABC, abstractmethod
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Generic, List

from reflow.internal import Envelope, INSTRUCTION
from reflow.internal.edge_router import LoadBalancingEdgeRouter, RoundRobinEdgeRouter
from reflow.internal.event_queue import OutputQueue, InputQueue, DequeueEventQueue
from reflow.internal.in_out_buffer import InOutBuffer
from reflow.internal.network import Address
from reflow.typedefs import EndOfStreamException
from reflow.typedefs import IN_EVENT_TYPE, TransformerFn, EVENT_TYPE, STATE_TYPE, InitFn, ProducerFn, ConsumerFn, \
    OUT_EVENT_TYPE

MIN_BATCH_SIZE = 10
MAX_BATCH_SIZE = 10_000


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

@dataclass(frozen=True)
class WorkerId:
    cluster_number: int
    worker_number: int

class Worker(ABC, Generic[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *,
                 preferred_network: str,
                 init_fn: InitFn = None,
                 expansion_factor = 1,
                 input_queue_size: int = None,
                 outboxes: List[List[Address]] = None):
        self.init_fn = init_fn
        self.state = None
        self.input_queue = None
        self.expansion_factor = expansion_factor
        self.id = None   # will be set immediately after construction
        self.finished = False
        self.preferred_network = preferred_network
        self.output_queues = None
        self.in_out_buffers = None
        self.exit_stack = ExitStack()
        self.last_event_seen = {}

        if not isinstance(self, SourceWorker):
            self.input_queue = DequeueEventQueue(input_queue_size, preferred_network=preferred_network)
            self.exit_stack.enter_context(self.input_queue)

        if outboxes:
            self.output_queues = [ RoundRobinEdgeRouter(outbox_addr_list, preferred_network) for outbox_addr_list in outboxes ]
            self.in_out_buffers = [ InOutBuffer() for _ in outboxes]
            for outbox in self.output_queues:
                self.exit_stack.enter_context(outbox)

        else:
            self.output_queues = []
            self.in_out_buffers = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.exit_stack.__exit__(exc_type, exc_val, exc_tb)

    async def init(self):
        if self.init_fn and not self.state:
            # noinspection PyTypeChecker
            self.state = await asyncio.get_running_loop().run_in_executor(None, self.init_fn)

    async def input_batch_size(self)->int:
        """
        Based on the capacity available in the output queue and other factors, computes a recommended number of
        events to read from the input queue and process
        """
        min_downstream_capacity = MAX_BATCH_SIZE
        for output_queue in self.output_queues:
            downstream_capacity = await output_queue.remaining_capacity()
            min_downstream_capacity = min(min_downstream_capacity, downstream_capacity)

        if min_downstream_capacity == 0:
            return 0
        else:
            return max(int(min_downstream_capacity/(2 * self.expansion_factor)), MIN_BATCH_SIZE)

    async def get_ready_events(self)->List[Envelope[IN_EVENT_TYPE]]:
        batch_size = await self.input_batch_size()
        if batch_size > 0:
            result =  await self.input_queue.get_events(self.id, batch_size)
            logging.debug(f'{self} attempted to read up to {batch_size} events from input queue and received  {len(result)}.')
        else:
            result = []
            logging.debug(f"{self} not reading input events while at least one output queues is full")

        return result

    async def process(self)->None:
        if self.total_unsent_events() == 0 and not self.finished:
            ready_events = await self.get_ready_events()
            for envelope in ready_events:
                if envelope.instruction == INSTRUCTION.PROCESS_EVENT:
                    output = self.handle_event(envelope)
                    for in_out_buffer in self.in_out_buffers:
                        in_out_buffer.record_split_event(output)
                elif envelope.instruction == INSTRUCTION.END_OF_STREAM:
                    logging.debug(f'({os.getpid()}) {self} received END_OF_STREAM instruction')
                    self.finished = True
                    for in_out_buffer in self.in_out_buffers:
                        in_out_buffer.record_1_1(envelope)
                else:
                    raise RuntimeError(f"unknown processing instruction encountered: {envelope.instruction}")

        if self.total_unsent_events() > 0:
            min_input_events_to_acknowledge = MAX_BATCH_SIZE
            for in_out_buffer, output_queue in zip(self.in_out_buffers, self.output_queues):
                logging.debug(f'{self} attempting to deliver  { len(in_out_buffer.unsent_out_events)}  output events to {output_queue}')
                consumed_events = await output_queue.enqueue(in_out_buffer.unsent_out_events)
                logging.debug(f'{self} delivered  {consumed_events}  output events to {output_queue}')
                input_events_to_acknowledge = in_out_buffer.record_delivered_out_events(consumed_events)
                min_input_events_to_acknowledge = min(min_input_events_to_acknowledge, input_events_to_acknowledge)

            if min_input_events_to_acknowledge > 0:
                await self.input_queue.acknowledge_events(self.id, min_input_events_to_acknowledge)
                logging.debug(f'{self} acknowledged {min_input_events_to_acknowledge} input events')

    def total_unsent_events(self)->int:
        return  sum([len(buffer.unsent_out_events) for buffer in self.in_out_buffers])

    @abstractmethod
    def handle_event(self, event: Envelope[IN_EVENT_TYPE])->List[Envelope[EVENT_TYPE]]:
        pass

class SourceWorker(Worker[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *, producer_fn: ProducerFn, preferred_network: str, outboxes: List[List[Address]], init_fn: InitFn = None):
        Worker.__init__(self, init_fn = init_fn, preferred_network=preferred_network, outboxes=outboxes)
        self.producer_fn = producer_fn

    async def init(self):
        await super().init()
        self.input_queue = SourceAdapter(self.producer_fn, self.state)

    def handle_event(self, event: Envelope[IN_EVENT_TYPE]) ->List[Envelope[OUT_EVENT_TYPE]]:
        # sources do not currently transform events
        return [event]


class SinkWorker(Worker[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *, consumer_fn: ConsumerFn, preferred_network: str, input_queue_size: int, init_fn: InitFn = None):
        Worker.__init__(self, init_fn=init_fn, preferred_network=preferred_network, input_queue_size=input_queue_size)
        self.consumer_fn = consumer_fn

    async def init(self):
        await super().init()
        self.output_queues = [SinkAdapter(self.consumer_fn, self.state)]
        self.in_out_buffers = [InOutBuffer()]

    def handle_event(self, event: Envelope[IN_EVENT_TYPE]) ->List[Envelope[OUT_EVENT_TYPE]]:
        # sinks do not currently transform events
        return [event]


class TransformWorker(Worker[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *, transform_fn: TransformerFn, expansion_factor: int, preferred_network: str, input_queue_size: int, outboxes: List[List[Address]], init_fn: InitFn = None):
        Worker.__init__(self, init_fn=init_fn, expansion_factor=expansion_factor, preferred_network=preferred_network, input_queue_size=input_queue_size, outboxes = outboxes)
        self.transform_fn = transform_fn

    def handle_event(self, envelope: Envelope[IN_EVENT_TYPE]) -> List[Envelope[EVENT_TYPE]]:
        if self.state:
            result =  self.transform_fn(self.state, envelope.event)
        else:
            result = self.transform_fn(envelope.event)

        return [Envelope(INSTRUCTION.PROCESS_EVENT, event) for event in result]


# The functions of the SourceAdapter are to emulate an input queue and to put envelopes around the raw events
class SourceAdapter(Generic[STATE_TYPE, EVENT_TYPE], InputQueue[EVENT_TYPE]):
    def __init__(self, producer_fn: ProducerFn, state: STATE_TYPE = None):
        self.producer_fn = producer_fn
        self.state = state

    async def get_events(self, subscriber: str, limit: int = 0) -> List[Envelope[EVENT_TYPE]]:
        assert limit > 0
        try:
            if self.state:
                result = await asyncio.get_running_loop().run_in_executor( None, self.producer_fn, self.state, limit)
            else:
                result = await asyncio.get_running_loop().run_in_executor( None, self.producer_fn, limit)

            return [Envelope(INSTRUCTION.PROCESS_EVENT, event) for event in result]
        except EndOfStreamException as _:
            return [Envelope(INSTRUCTION.END_OF_STREAM)]

    # this class does not support "acknowledge" functionality
    async def acknowledge_events(self, subscriber: str, n: int) -> None:
        pass


# The functions of the SinkAdapter are to emulate an OutputQueue and to remove envelopes from the raw events
# before sending them to the consumer - note that it will swallow processing instructions without
# processing them because they would have already been processed during the "transform" stage.  The choice to send
# processing instructions to the SinkAdapter was made because there is a need to accurately acknowledge consumed
# events, both regular and processing instructions, also there is a need to maintain ordering of all events and lastly,
# it is important to send batches of events to the consumer whenever possible.
#
# The option of just removing the processing instructions from the stream would result in missing or mis-aligned
# acknowledgements back to the input.  Each event of either type must be specifically acknowledged in sequence.
# One option would be to just iterate through the batch of events one at a time, forwarding only the regular events,
# acknowledging those as the consumer consumes them and handling processing instruction acknowledgements outside
# the Sink Adapter, but we need to send to the consumer in batches. So, we walk through the list of events in
# order, assembling batches of contiguous regular events and sending them to the consumer, acknowledging the
# corresponding input events, and then handling any intervening processing instructions outside the consumer.
#
# In reality, if the SinkAdapter is given a mix of processing instructions and regular events, it will consume
# the first processing instruction or batch and send back the appropriate count from the enqueue method.  This
# is mainly because there is only one return value available.  Anything remaining will stay in the workers
# unsent events list and the process will continue next time the worker gets a turn.
#
class SinkAdapter(Generic[STATE_TYPE, EVENT_TYPE], OutputQueue[EVENT_TYPE]):
    def __init__(self, consumer_fn: ConsumerFn, state: STATE_TYPE = None):
        self.consumer_fn = consumer_fn
        self.state = state

    async def enqueue(self, events: List[Envelope[EVENT_TYPE]]) -> int:
        if len(events) == 0:
            return 0

        next_processing_instruction_index = len(events)
        for i, event in enumerate(events):
            if event.instruction != INSTRUCTION.PROCESS_EVENT:
                next_processing_instruction_index = i
                break

        if next_processing_instruction_index == 0:
            return 1
        else:
            batch = [envelope.event for envelope in events[0:next_processing_instruction_index]]
            if self.state:
                result = await asyncio.get_running_loop().run_in_executor(None, self.consumer_fn, self.state, batch)
            else:
                result =await asyncio.get_running_loop().run_in_executor(None, self.consumer_fn, batch)

        return result

    async def remaining_capacity(self) -> int:
        return MAX_BATCH_SIZE