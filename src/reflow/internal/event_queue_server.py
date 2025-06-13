from abc import ABCMeta, abstractmethod
from typing import Generic, List, Any

import zmq
from zmq.asyncio import Context

from reflow.internal.zmq_server import ZMQServer
from reflow.internal.event_queue import EnqueueRequest, RemainingCapacityRequest, GetEventsRequest, \
    AcknowledgeEventsRequest, \
    EnqueueResponse, RemainingCapacityResponse, GetEventsResponse, AcknowledgeEventsResponse, EVENT_TYPE, \
    LocalEventQueue


class EventQueueServer(LocalEventQueue[EVENT_TYPE], Generic[EVENT_TYPE], ZMQServer):
    def __init__(self, capacity: int, bind_addresses: List[str]):
        LocalEventQueue.__init__(self, capacity)
        ZMQServer.__init__(self, bind_addresses)

    async def process_request(self, request: Any):
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

        return response

