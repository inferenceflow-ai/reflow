import logging
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from types import NoneType
from typing import Generic, List

from internal.in_out_map import InOutMap
from typedefs import IN_EVENT_TYPE, SplitFn, EVENT_TYPE, STATE_TYPE, InitFn, ProducerFn, ConsumerFn, OUT_EVENT_TYPE, \
    EndOfStreamException

MIN_BATCH_SIZE = 10
MAX_BATCH_SIZE = 10000

class INSTRUCTION(Enum):
    PROCESS_EVENT = 1
    END_OF_STREAM = 2


class Envelope(Generic[EVENT_TYPE]):
    def __init__(self, instruction: INSTRUCTION, event: EVENT_TYPE = None):
        self.instruction = instruction
        self.event = event

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

class Worker(ABC, Generic[IN_EVENT_TYPE]):
    def __init__(self, init_fn: InitFn = None, expansion_factor = 1):
        self.init_fn = init_fn
        self.state = None
        self.input_queue = None
        self.output_queue = None
        self.expansion_factor = expansion_factor
        self.id = uuid.uuid4()
        self.finished = False
        self.in_out_map = InOutMap()
        self.unsent_out_events = []

    async def init(self):
        if self.init_fn and not self.state:
            self.state = await self.init_fn()

    async def input_batch_size(self)->int:
        """
        Based on the capacity available in the output queue and other factors, computes a recommended number of
        events to read from the input queue and process
        """
        downstream_capacity = await self.output_queue.remaining_capacity()
        return max(int(downstream_capacity/(2 * self.expansion_factor)), MIN_BATCH_SIZE)

    async def get_ready_events(self)->List[Envelope[IN_EVENT_TYPE]]:
        batch_size = await self.input_batch_size()
        return await self.input_queue.get_events(self.id, batch_size)

    @abstractmethod
    async def process(self)->None:
        pass


class SourceWorker(Worker[NoneType], Generic[EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *, producer_fn: ProducerFn, init_fn: InitFn = None):
        Worker.__init__(self, init_fn)
        self.producer_fn = producer_fn

    async def process(self):
        if len(self.unsent_out_events) == 0:
            batch_size = await self.input_batch_size()
            try:
                if self.state:
                    ready_events = await self.producer_fn(self.state, batch_size)
                else:
                    ready_events = await self.producer_fn(batch_size)

                logging.debug(f'{self} producing {len(ready_events)} events')

                # put an envelope around each event and add it to unsent_out_events
                self.unsent_out_events.extend([Envelope(INSTRUCTION.PROCESS_EVENT, event) for event in ready_events])
                self.in_out_map.record_batch(len(ready_events), len(ready_events))
            except EndOfStreamException as x:
                self.unsent_out_events.append(Envelope(INSTRUCTION.END_OF_STREAM))
                self.in_out_map.record_no_producer_event()
                self.finished = True

        if len(self.unsent_out_events) > 0:
            delivered = await self.output_queue.enqueue(self.unsent_out_events)
            del self.unsent_out_events[0:delivered]
            # current version of source does not support acknowledgements back to the event producer
            # we only call acknowledgeable_in_events to clean up the in_out_map

            # noinspection PyUnusedLocal
            inputs_to_acknowledge = self.in_out_map.acknowledgeable_in_events(delivered)


class SinkWorker(Worker[IN_EVENT_TYPE], Generic[IN_EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *, consumer_fn: ConsumerFn, init_fn: InitFn = None):
        Worker.__init__(self, init_fn)
        self.consumer_fn = consumer_fn

    async def get_ready_events(self)->List[Envelope[IN_EVENT_TYPE]]:
        return await self.input_queue.get_events(self.id, MAX_BATCH_SIZE)

    async def process(self):
        if len(self.unsent_out_events) == 0:
            self.unsent_out_events = await self.get_ready_events()
            self.in_out_map.record_1_1s(len(self.unsent_out_events))

        if len(self.unsent_out_events) > 0:
            # Now deliver unsent_out_events to the consumer
            # find the first special instruction
            first_special_instruction_index = len(self.unsent_out_events)
            for i, envelope in enumerate(self.unsent_out_events):
                if envelope.instruction != INSTRUCTION.PROCESS_EVENT:
                    first_special_instruction_index = i
                    break

            if first_special_instruction_index < len(self.unsent_out_events):
                special_instruction = self.unsent_out_events[first_special_instruction_index].instruction
            else:
                special_instruction = None

            # deliver up to the first special instruction
            events_to_deliver = [envelope.event for envelope in self.unsent_out_events[0:first_special_instruction_index]]
            logging.debug(f'{self} attempting to send  {len(events_to_deliver)} to consumer')
            if self.state:
                consumed = await self.consumer_fn(self.state, events_to_deliver)
            else:
                consumed = await self.consumer_fn(events_to_deliver)

            logging.debug(f'{self} delivered  {consumed} to consumer')

            del self.unsent_out_events[0:consumed]
            input_events_to_acknowledge = self.in_out_map.acknowledgeable_in_events(consumed)
            assert input_events_to_acknowledge == consumed
            await self.input_queue.acknowledge_events(self.id, input_events_to_acknowledge)

            # process the special instruction if all events before it were delivered
            if consumed == len(events_to_deliver) and special_instruction is not None:
                assert special_instruction == INSTRUCTION.END_OF_STREAM
                self.finished = True
                del self.unsent_out_events[0]
                input_events_to_acknowledge = self.in_out_map.acknowledgeable_in_events(1)
                assert input_events_to_acknowledge == 1
                await self.input_queue.acknowledge_events(self.id, input_events_to_acknowledge)


class SplitWorker(Worker[IN_EVENT_TYPE], Generic[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *, split_fn: SplitFn, expansion_factor: int, init_fn: InitFn = None):
        Worker.__init__(self, init_fn=init_fn, expansion_factor=expansion_factor)
        self.split_fn = split_fn

    async def process(self):
        if len(self.unsent_out_events) == 0:
            in_envelopes = await self.get_ready_events()
            logging.debug(f'{self} processing {len(in_envelopes)} events')
            for envelope in in_envelopes:
                if envelope.instruction == INSTRUCTION.PROCESS_EVENT:
                    if self.state:
                        parts = await self.split_fn(self.state, envelope.event)
                    else:
                        parts = await self.split_fn(envelope.event)

                    self.unsent_out_events.extend([Envelope(INSTRUCTION.PROCESS_EVENT, event) for event in parts])
                    self.in_out_map.record_split_event(len(parts))
                else:
                    self.in_out_map.record_filtered_event()
                    if envelope.instruction == INSTRUCTION.END_OF_STREAM:
                        self.finished = True
                        break

        if len(self.unsent_out_events) > 0:
            delivered = await self.output_queue.enqueue(self.unsent_out_events)
            input_events_to_acknowledge = self.in_out_map.acknowledgeable_in_events(delivered)
            await self.input_queue.acknowledge_events(input_events_to_acknowledge)

