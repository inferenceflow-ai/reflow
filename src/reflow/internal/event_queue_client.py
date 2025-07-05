from typing import List

from reflow.internal.zmq import ZMQClient
from reflow.internal import Envelope
from reflow.internal.event_queue import EVENT_TYPE, InputQueue, OutputQueue, EnqueueRequest, \
    RemainingCapacityRequest, GetEventsRequest, AcknowledgeEventsRequest


class EventQueueClient(InputQueue[EVENT_TYPE], OutputQueue[EVENT_TYPE], ZMQClient):
    def __init__(self, server_address: str):
        ZMQClient.__init__(self, server_address)

    async def enqueue(self, events: List[Envelope[EVENT_TYPE]])->int:
        request = EnqueueRequest(events = events)
        await self.socket.send_pyobj(request)
        response = await self.socket.recv_pyobj()
        return response.count

    async def remaining_capacity(self)->int:
        request = RemainingCapacityRequest()
        await self.socket.send_pyobj(request)
        response = await self.socket.recv_pyobj()
        return response.count

    async def get_events(self, subscriber: str, limit: int = 0)->List[Envelope[EVENT_TYPE]]:
        request = GetEventsRequest(subscriber=subscriber, limit=limit)
        await self.socket.send_pyobj(request)
        response = await self.socket.recv_pyobj()
        return response.events

    async def acknowledge_events(self, subscriber: str, n: int)->None:
        request = AcknowledgeEventsRequest(subscriber=subscriber, count = n)
        await self.socket.send_pyobj(request)
        await self.socket.recv_pyobj()
