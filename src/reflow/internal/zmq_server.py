import abc
from typing import List, Any

import zmq
from zmq.asyncio import Context


class ZMQServer(abc.ABC):
    def __init__(self, bind_addresses: List[str]):
        self.bind_addresses = bind_addresses
        self.context = None
        self.socket = None

    async def serve(self):
        request = await self.socket.recv_pyobj()
        response = await self.process_request(request)
        await self.socket.send_pyobj(response)

    @abc.abstractmethod
    async def process_request(self, request: Any)->Any:
        pass

    def __enter__(self):
        self.context = Context()
        self.socket = self.context.socket(zmq.REP)
        for bind_address in self.bind_addresses:
            self.socket.bind(bind_address)

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.socket.close()
        self.context.term()
        return False
