import abc
from abc import abstractmethod
from contextlib import ExitStack
from typing import List

from reflow.internal import Envelope, INSTRUCTION
from reflow.internal.event_queue import EventQueueClient, OutputQueue, local_event_queue_registry, DequeueEventQueue
from reflow.internal.network import get_preferred_interface_ip, WorkerDescriptor
from reflow.typedefs import EVENT_TYPE, KeyFn

MAX_BATCH_SIZE = 10_000


class EdgeRouter(OutputQueue[EVENT_TYPE], abc.ABC):
    def __init__(self, outbox_descriptors: List[WorkerDescriptor], preferred_network: str):
        self.exit_stack = ExitStack()
        self.outbox_descriptors = outbox_descriptors
        self.outboxes = []
        my_address = get_preferred_interface_ip(preferred_network)
        for address in [descriptor.address for descriptor in outbox_descriptors]:
            if address in local_event_queue_registry:
                outbox = local_event_queue_registry[address]
            elif address.ip == my_address:
                outbox = EventQueueClient(address.ipc_bind_address())
                self.exit_stack.enter_context(outbox)
            else:
                outbox = EventQueueClient(address.tcp_bind_address())
                self.exit_stack.enter_context(outbox)

            self.outboxes.append(outbox)

    def __enter__(self):
        for outbox in self.outboxes:
            # if the outbox is actually a local event queue then don't enter its context since that will already
            # have happened when it was created by the downstream worker
            if not isinstance(outbox, DequeueEventQueue):
                self.exit_stack.enter_context(outbox)

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.exit_stack.__exit__(exc_type, exc_val, exc_tb)

    async def enqueue(self, events: List[Envelope[EVENT_TYPE]]) -> int:
        # Special processing instructions have to be duplicated and sent to all downstream
        # outboxes.
        first_pi_index = len(events)
        for i in range(len(events)):
            if events[i].instruction != INSTRUCTION.PROCESS_EVENT:
                first_pi_index = i
                break

        if first_pi_index == 0:
            success_count = 0
            for outbox in self.outboxes:
                success_count += await outbox.enqueue([events[0]])

            if success_count < len(self.outboxes):
                return 0
            else:
                return 1
        else:
            return await self._enqueue_impl(events[0:first_pi_index])

    @abstractmethod
    async def _enqueue_impl(self, events: List[Envelope[EVENT_TYPE]]) -> int:
        pass

    async def remaining_capacity(self) -> int:
        result = MAX_BATCH_SIZE
        for queue in self.outboxes:
            capacity = await queue.remaining_capacity()
            result = min(result, capacity)

        return result


class LoadBalancingEdgeRouter(EdgeRouter[EVENT_TYPE]):
    def __init__(self, outbox_descriptor: List[WorkerDescriptor], preferred_network: str):
        EdgeRouter.__init__(self, outbox_descriptors=outbox_descriptor, preferred_network=preferred_network)
        self.shortest_event_queue = None
        self.shortest_event_queue_capacity = 0

    async def _enqueue_impl(self, events: List[Envelope[EVENT_TYPE]]) -> int:
        if not self.shortest_event_queue:
            await self.remaining_capacity()  # will set self.shortest_event_queue

        result = await self.shortest_event_queue.enqueue(events)
        self.shortest_event_queue = None
        self.shortest_event_queue_capacity = 0
        return result

    # override the default implementation so we can set the shortest_event_queue
    async def remaining_capacity(self) -> int:
        for queue in self.outboxes:
            capacity = await queue.remaining_capacity()
            if self.shortest_event_queue is None or capacity > self.shortest_event_queue_capacity:
                self.shortest_event_queue = queue
                self.shortest_event_queue_capacity = capacity

        return self.shortest_event_queue_capacity


class KeyBasedEdgeRouter(EdgeRouter[EVENT_TYPE]):
    def __init__(self, outbox_descriptor: List[WorkerDescriptor], preferred_network: str, key_fn: KeyFn):
        EdgeRouter.__init__(self, outbox_descriptors=outbox_descriptor, preferred_network=preferred_network)
        self.key_fn = key_fn

        # validate that all outbox descriptors have the same cluster_size and that all cluster_numbers
        # in [0, cluster_size) are present
        self.cluster_size = self.outbox_descriptors[0].cluster_size
        for n in range(0, self.cluster_size):
            found = False
            for descriptor in self.outbox_descriptors:
                if descriptor.cluster_number == n:
                    found = True
                    break

            if not found:
                raise RuntimeError(f'A KeyBasedRouter must have an outbox for every cluster_number. '
                                   f'No outbox for {n} was provided.')

        for descriptor in self.outbox_descriptors:
            if descriptor.cluster_size != self.cluster_size:
                raise RuntimeError(f'cluster_size must be the same on all provided outbox descriptors')

        if len(self.outbox_descriptors) != self.cluster_size:
            raise RuntimeError(f'The number of outbox descriptors must match the cluster size. '
                               f' {len(self.outbox_descriptors)} descriptors were provided '
                               f'but the cluster size is {self.cluster_size}.')


    async def _enqueue_impl(self, events: List[Envelope[EVENT_TYPE]]) -> int:
        # compile a list of indices of events in [0, first_pi_index) that match each cluster number
        index_lists = [[] for _ in range(0,self.cluster_size)]
        for i, event in enumerate(events):
            cluster_number = hash(self.key_fn(event.event)) % self.cluster_size
            index_lists[cluster_number].append(i)

        # attempt to deliver the correct subset of events to each outbox
        # if an outbox cannot accommodate all enqueued events, we figure out how many consecutive
        # events can be acknowledged - if events 0-9 were dispatched and events 3 and 5 five were the
        # only 2 that couldn't be delivered, we will acknowledge return 3 because events 0,1,2 can
        # safely be acknowledged.  Note that events 3,4,6,7,8,9 will be redelivered.  However, the
        # downstream workers should ignore those events since they will have already seen them

        result = len(events)
        for descriptor, outbox in zip(self.outbox_descriptors, self.outboxes):
            cluster_number = descriptor.cluster_number
            selected_events = [events[i] for i in index_lists[cluster_number]]
            if len(selected_events) > 0:
                num_delivered = await outbox.enqueue(selected_events)
                if num_delivered < len(selected_events):
                    result = min(index_lists[cluster_number][num_delivered], result)

        return result


class LocalEdgeRouter(EdgeRouter[EVENT_TYPE]):
    def __init__(self, outbox_descriptor: List[WorkerDescriptor], preferred_network: str):
        EdgeRouter.__init__(self, outbox_descriptors=outbox_descriptor, preferred_network=preferred_network)

        self.local_outbox = None
        for outbox in self.outboxes:
            if isinstance(outbox, DequeueEventQueue):
                self.local_outbox = outbox
                break

        if not self.local_outbox:
            raise RuntimeError(f'No local outbox found')

    async def _enqueue_impl(self, events: List[Envelope[EVENT_TYPE]]) -> int:
        return await self.local_outbox.enqueue(events)
