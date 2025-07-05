import abc
import asyncio
import logging
from typing import List, Any

import dill
import zmq
from zmq.asyncio import Context


class ZMQServer(abc.ABC):
    def __init__(self, bind_addresses: List[str]):
        self.bind_addresses = bind_addresses
        self.context = None
        self.socket = None
        self.task = None

    async def _serve(self):
        while True:
            request_bytes = await self.socket.recv()
            request = dill.loads(request_bytes)
            response = await self.process_request(request)
            await self.socket.send(dill.dumps(response))

    @abc.abstractmethod
    async def process_request(self, request: Any)->Any:
        pass

    def __enter__(self):
        self.context = Context()
        self.socket = self.context.socket(zmq.REP)
        for bind_address in self.bind_addresses:
            self.socket.bind(bind_address)

        logging.debug(f'starting ZeroMQ server listening on {self.bind_addresses}')
        self.task = asyncio.create_task(self._serve())
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        logging.debug(f'stopping ZeroMQ server listening on {self.bind_addresses}')
        self.task.cancel()
        self.socket.close()
        self.context.term()
        return False


class ZMQClient:
    def __init__(self, server_address: str):
        self.server_address = server_address
        self.context = None
        self.socket = None

    def __enter__(self):
        self.context = Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.server_address)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.socket.close()
        self.context.term()
        return False

    async def send_request(self, request: Any) -> Any:
        request_bytes = dill.dumps(request)
        await self.socket.send(request_bytes)
        response_bytes = await self.socket.recv()
        response = dill.loads(response_bytes)
        return response
