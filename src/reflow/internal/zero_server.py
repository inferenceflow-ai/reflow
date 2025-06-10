from typing import Generic

import zmq
from zmq.asyncio import Context

from reflow.internal.event_queue import EnqueueRequest, RemainingCapacityRequest, GetEventsRequest, \
    AcknowledgeEventsRequest, \
    EnqueueResponse, RemainingCapacityResponse, GetEventsResponse, AcknowledgeEventsResponse, EVENT_TYPE, \
    LocalEventQueue


class ZeroServerEventQueue(LocalEventQueue[EVENT_TYPE], Generic[EVENT_TYPE]):
    def __init__(self, capacity: int, bind_address: str):
        LocalEventQueue.__init__(self, capacity)
        self.bind_address = bind_address
        self.context = None
        self.socket = None

    async def process_request(self):
        request = await self.socket.recv_pyobj()

        if isinstance(request, EnqueueRequest):
            count = await self.enqueue(request.events)
            response = EnqueueResponse(count=count)
        elif isinstance(request, RemainingCapacityRequest):
            count = await self.remaining_capacity()
            response = RemainingCapacityResponse(count = count)
        elif isinstance(request, GetEventsRequest):
            events = await self.get_events(request.subscriber, request.limit)
            response = GetEventsResponse(events = events)
        elif isinstance(request, AcknowledgeEventsRequest):
            await self.acknowledge_events(request.subscriber, request.count)
            response = AcknowledgeEventsResponse()
        else:
            raise RuntimeError("received unknown request")

        await self.socket.send_pyobj(response)

    def __enter__(self):
        self.context = Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(self.bind_address)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.socket.close()
        self.context.term()
        return False

