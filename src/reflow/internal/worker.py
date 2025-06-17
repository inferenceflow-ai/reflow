import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from typing import Generic, List

from reflow.internal import Envelope, INSTRUCTION
from reflow.internal.event_queue import OutputQueue, InputQueue
from reflow.internal.in_out_buffer import InOutBuffer
from reflow.typedefs import IN_EVENT_TYPE, SplitFn, EVENT_TYPE, STATE_TYPE, InitFn, ProducerFn, ConsumerFn, \
    OUT_EVENT_TYPE
from typedefs import EndOfStreamException

MIN_BATCH_SIZE = 10
MAX_BATCH_SIZE = 10000


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

class Worker(ABC, Generic[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]):
    def __init__(self, init_fn: InitFn = None, expansion_factor = 1):
        self.init_fn = init_fn
        self.state = None
        self.input_queue = None
        self.output_queue = None
        self.expansion_factor = expansion_factor
        self.id = uuid.uuid4()
        self.finished = False
        self.in_out_buffer = InOutBuffer()

    async def init(self):
        if self.init_fn and not self.state:
            # noinspection PyTypeChecker
            self.state = await asyncio.get_running_loop().run_in_executor(None, self.init_fn)

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

    async def process(self)->None:
        while True:
            if len(self.in_out_buffer.unsent_out_events) == 0 and not self.finished:
                events_to_read = int(await self.output_queue.remaining_capacity() / self.expansion_factor)
                ready_events = await self.input_queue.get_events(self.id, events_to_read)
                logging.debug(f'{self} read {len(ready_events)} from input queue')

                # Regular events get passed through to the transform function but processing instructions
                # get processed at this level.

                for envelope in ready_events:
                    if envelope.instruction == INSTRUCTION.PROCESS_EVENT:
                        output = self.handle_event(envelope)
                        self.in_out_buffer.record_split_event(output)
                    elif envelope.instruction == INSTRUCTION.END_OF_STREAM:
                        self.finished = True
                        self.in_out_buffer.record_1_1(envelope)
                    else:
                        raise RuntimeError(f"unknown processing instruction encountered: {envelope.instruction}")

            if len(self.in_out_buffer.unsent_out_events) > 0:
                logging.debug(f'{self} attempting to deliver  { len(self.in_out_buffer.unsent_out_events)}  output events')
                consumed_events = await self.output_queue.enqueue(self.in_out_buffer.unsent_out_events)
                logging.debug(f'{self} delivered  {consumed_events}  output events')
                if consumed_events > 0:
                    input_events_to_acknowledge = self.in_out_buffer.record_delivered_out_events(consumed_events)
                    await self.input_queue.acknowledge_events(self.id, input_events_to_acknowledge)
                    logging.debug(f'{self} acknowledged {input_events_to_acknowledge} input events')
            else:
                # avoid busy loop
                if not self.finished:
                    logging.debug(f'Nothing to do: {self} sleeping for 1s')
                    await asyncio.sleep(1)
                else:
                    logging.debug(f'{self} exiting due to end of stream')
                    break

            await asyncio.sleep(0)

    @abstractmethod
    def handle_event(self, event: Envelope[IN_EVENT_TYPE])->List[Envelope[EVENT_TYPE]]:
        pass

class SourceWorker(Worker[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *, producer_fn: ProducerFn, init_fn: InitFn = None):
        Worker.__init__(self, init_fn)
        self.producer_fn = producer_fn

    async def init(self):
        await super().init()
        self.input_queue = SourceAdapter(self.producer_fn, self.state)

    def handle_event(self, event: Envelope[IN_EVENT_TYPE]) ->List[Envelope[OUT_EVENT_TYPE]]:
        # sources do not currently transform events
        return [event]


class SinkWorker(Worker[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *, consumer_fn: ConsumerFn, init_fn: InitFn = None):
        Worker.__init__(self, init_fn)
        self.consumer_fn = consumer_fn

    async def init(self):
        await super().init()
        self.output_queue = SinkAdapter(self.consumer_fn, self.state)

    def handle_event(self, event: Envelope[IN_EVENT_TYPE]) ->List[Envelope[OUT_EVENT_TYPE]]:
        # sinks do not currently transform events
        return [event]


class SplitWorker(Worker[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]):
    def __init__(self, *, split_fn: SplitFn, expansion_factor: int, init_fn: InitFn = None):
        Worker.__init__(self, init_fn=init_fn, expansion_factor=expansion_factor)
        self.split_fn = split_fn

    def handle_event(self, envelope: Envelope[IN_EVENT_TYPE]) -> List[Envelope[EVENT_TYPE]]:
        if self.state:
            result =  self.split_fn(self.state, envelope.event)
        else:
            result = self.split_fn(envelope.event)

        return [Envelope(INSTRUCTION.PROCESS_EVENT, event) for event in result]


# The functions of the SourceAdapter are to emulate an input queue and to put envelopes around the raw events
class SourceAdapter(Generic[STATE_TYPE, EVENT_TYPE], InputQueue[EVENT_TYPE]):
    def __init__(self, producer_fn: ProducerFn, state: STATE_TYPE = None):
        self.producer_fn = producer_fn
        self.state = state

    async def get_events(self, subscriber: str, limit: int = 0) -> List[Envelope[EVENT_TYPE]]:
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
# processing them (they would have already been processed during the "transform" stage.  The choice to send processing
# instructions to the SinkAdapter was made because there is a need to accurately acknowledge consumed events, both
# regular and processing instructions, also there is a need to maintain ordering of all events and lastly, its
# important to send batches of events to the consumer whenever possible.
#
# The option of just removing the processing instructions from the stream would result in missing or mis-aligned
# acknowledgements back to the input.  Each event of either type must be specifically acknowledged in sequence.
# One option would be to just iterate through the batch of events one at a time, forwarding only the regular events,
# acknowledging those as the consumer consumes them and handling processing instruction acknowledgements outside
# the Sink Adapter but we need to send to the consumer in batches. So, we walk through the list of events in
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