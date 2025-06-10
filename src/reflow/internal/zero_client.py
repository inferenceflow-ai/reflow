from typing import List

import zmq
from zmq.asyncio import Context

from reflow.internal import Envelope
from reflow.internal.event_queue import EVENT_TYPE, InputQueue, OutputQueue, EnqueueRequest, \
    RemainingCapacityRequest, GetEventsRequest, AcknowledgeEventsRequest


class ZeroClientEventQueue(InputQueue[EVENT_TYPE], OutputQueue[EVENT_TYPE]):
    def __init__(self, bind_address: str):
        self.bind_address = bind_address
        self.context = None
        self.socket = None

    def __enter__(self):
        self.context = Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.bind_address)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.socket.close()
        self.context.term()
        return False

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
