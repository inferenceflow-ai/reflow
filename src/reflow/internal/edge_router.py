from contextlib import ExitStack
from typing import List

from reflow.typedefs import EVENT_TYPE
from reflow.internal import Envelope
from reflow.internal.event_queue import EventQueueClient, OutputQueue, local_event_queue_registry
from reflow.internal.network import Address, get_preferred_interface_ip


class LoadBalancingEdgeRouter(OutputQueue[EVENT_TYPE]):
    def __init__(self, outbox_addresses: List[Address], preferred_network: str):
        self.exit_stack = ExitStack()
        self.outboxes = []
        my_address = get_preferred_interface_ip(preferred_network)
        for address in outbox_addresses:
            if address in local_event_queue_registry:
                outbox = local_event_queue_registry[address]
            elif address.ip == my_address:
                outbox = EventQueueClient(address.ipc_bind_address())
                self.exit_stack.enter_context(outbox)
            else:
                outbox = EventQueueClient(address.tcp_bind_address())
                self.exit_stack.enter_context(outbox)

            self.outboxes.append(outbox)

        self.shortest_event_queue = None
        self.shortest_event_queue_capacity = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.exit_stack.__exit__(exc_type, exc_val, exc_tb)

    async def enqueue(self, events: List[Envelope[EVENT_TYPE]])->int:
        if not self.shortest_event_queue:
            await self.remaining_capacity()  # will set self.shortest_event_queue

        result = await self.shortest_event_queue.enqueue(events)
        self.shortest_event_queue = None
        self.shortest_event_queue_capacity = 0
        return result

    async def remaining_capacity(self)->int:
        for queue in self.outboxes:
            capacity = await queue.remaining_capacity()
            if self.shortest_event_queue is None or capacity > self.shortest_event_queue_capacity:
                self.shortest_event_queue = queue
                self.shortest_event_queue_capacity = capacity

        return self.shortest_event_queue_capacity

