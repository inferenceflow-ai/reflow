import logging
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from types import NoneType
from typing import Generic, List

from typedefs import IN_EVENT_TYPE, SplitFn, EVENT_TYPE, STATE_TYPE, InitFn, ProducerFn, ConsumerFn, OUT_EVENT_TYPE


class INSTRUCTION(Enum):
    PROCESS_EVENT = 1
    END_OF_SOURCE = 2


class Envelope(Generic[EVENT_TYPE]):
    def __init__(self, instruction: INSTRUCTION, event: EVENT_TYPE = None):
        self.instruction = instruction
        self.event = event


class Worker(ABC, Generic[IN_EVENT_TYPE]):
    def __init__(self, init_fn: InitFn = None, expansion_factor = 1):
        self.init_fn = init_fn
        self.state = None
        self.input_queue = None
        self.output_queue = None
        self.expansion_factor = expansion_factor
        self.id = uuid.uuid4()

    async def init(self):
        if self.init_fn and not self.state:
            self.state = await self.init_fn()

    async def input_batch_size(self)->int:
        """
        Based on the capacity available in the output queue and other factors, computes a recommended number of
        events to read from the input queue and process
        """
        downstream_capacity = await self.output_queue.remaining_capacity()
        return int(downstream_capacity/(2 * self.expansion_factor))

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
        batch_size = await self.input_batch_size()
        if self.state:
            ready_events = await self.producer_fn(self.state, batch_size)
        else:
            ready_events = await self.producer_fn(batch_size)

        logging.debug(f'{self} producing {len(ready_events)} events')

        if len(ready_events) == 0:
            return  # RETURN if there are no events to process

        envelopes = [Envelope(INSTRUCTION.PROCESS_EVENT, event) for event in ready_events]
        delivered = await self.output_queue.enqueue(envelopes)

        if delivered < len(envelopes):
            logging.error(f"EVENTS LOST - {len(envelopes)} events received from source but only {delivered} events saved to output queue")

        # in the future, for rewindable sources, acknowledge processed events after the result has been delivered
        # to the downstream queue - we need some way to understand which input events produced which output events

class SinkWorker(Worker[IN_EVENT_TYPE], Generic[IN_EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *, consumer_fn: ConsumerFn, init_fn: InitFn = None):
        Worker.__init__(self, init_fn)
        self.consumer_fn = consumer_fn

    async def process(self):
        # there is no downstream queue to worry about so we just try to get all events from the input queue and
        # send them to the sink.
        envelopes = await self.input_queue.get_events(self.id)
        logging.debug(f'{self} processing {len(envelopes)} events')

        # The event_count_map maps input events to the number of output events produced.  In this case, each input
        # event can produce 0 or 1 output events.  This information is used later to determine how many input events
        # to acknowledge
        event_count_map = [1 if envelope.instruction == INSTRUCTION.PROCESS_EVENT else 0 for envelope in envelopes]

        events = [envelope.event for envelope in envelopes if envelope.instruction == INSTRUCTION.PROCESS_EVENT]
        if self.state:
            consumed = await self.consumer_fn(self.state, events)
        else:
            consumed = await self.consumer_fn(events)

        if consumed == len(events):
            await self.input_queue.acknowledge_events(self.id, len(envelopes))
        elif consumed > 0:
            events_to_acknowledge = 0
            events_encountered = 0
            for c in event_count_map:
                events_encountered += c
                events_to_acknowledge += 1
                if events_encountered == consumed:
                    break

            logging.warning(f'Sink consumed {consumed} events out of {len(events)} possible. {events_to_acknowledge} out of {len(envelopes)} input events will be  acknowledged')
            await self.input_queue.acknowledge_events(self.id, events_to_acknowledge)
        else:
            logging.warning("Sink consumed 0 events")

class SplitWorker(Worker[IN_EVENT_TYPE], Generic[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *, split_fn: SplitFn, expansion_factor: int, init_fn: InitFn = None):
        Worker.__init__(self, init_fn=init_fn, expansion_factor=expansion_factor)
        self.split_fn = split_fn

    async def process(self):
        in_envelopes = await self.get_ready_events()
        logging.debug(f'{self} processing {len(in_envelopes)} events')

        result = []
        event_count_map = []  #maps number of output events produces by each input event
        for envelope in in_envelopes:
            if envelope.instruction == INSTRUCTION.PROCESS_EVENT:
                if self.state:
                    parts = await self.split_fn(self.state, envelope.event)
                else:
                    parts = await self.split_fn(envelope.event)

                result.extend(parts)
                event_count_map.append(len(parts))
            else:
                event_count_map.append(0)

        out_envelopes = [Envelope(INSTRUCTION.PROCESS_EVENT, event) for event in result]
        delivered = await self.output_queue.enqueue(out_envelopes)

        # We can only acknowledge those input events for which the corresponding output events have been delivered
        # to the output queue - this is "input events processed"
        input_events_processed = 0
        output_event_ttl = 0
        for count in event_count_map:
            output_event_ttl += count
            if delivered >= output_event_ttl:
                input_events_processed += 1
            else:
                break

        if input_events_processed < len(in_envelopes):
            logging.warn(f'Only {input_events_processed} out of {len(in_envelopes)} input events could be processed due to a full output queue')

        await self.input_queue.acknowledge_events(self.id, input_events_processed)

