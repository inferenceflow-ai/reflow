import abc
import asyncio
import logging
from typing import List, Any

import dill
import zmq
from zmq.asyncio import Context

from reflow.internal.network import get_preferred_interface_ip, ipc_address_for_port, Address


class ZMQServer(abc.ABC):
    def __init__(self, *, bind_addresses: List[str] = None, preferred_network: str = None):
        """
        At most one of bind_addresses or preferred_network should be provided

        bind_addresses allows explicit control of what addresses the ZeroMQ server socket binds
        to and will be passed directly to the socket.bind method (https://pyzmq.readthedocs.io/en/latest/api/zmq.html).

        preferred_network causes the bind to the tcp address on this host that starts with
        preferred_network.  An example of preferred_network would be "192.168.1".  In this case,
        socket.bind_to_random_port will be called.  In addition to the tcp address, the ZeroMQ server will
        also bind to the IPC port: ipc://service_nnnn where nnnn is the selected port number

        If neither is provided, the ZeroMQ server will never be started, which can be useful when running everything
        in the same process
        """
        provided = 0
        provided += 1 if bind_addresses else 0
        provided += 1 if preferred_network else 0

        if provided > 1:
            raise RuntimeError('At most one of "bind_addresses" and "preferred_network" must be provided')

        self.bind_addresses = bind_addresses
        self.preferred_network = preferred_network
        self.port = None
        self.context = None
        self.socket = None
        self.task = None
        self.address = None

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
        if self.bind_addresses or self.preferred_network:
            try:
                self.context = Context()
                self.socket = self.context.socket(zmq.REP)

                if not self.bind_addresses or len(self.bind_addresses) == 0:
                    self.bind_addresses = []
                    preferred_address = get_preferred_interface_ip(self.preferred_network)
                    self.port = self.socket.bind_to_random_port(f'tcp://{preferred_address}', 5001, 5999, 999)
                    self.bind_addresses.append(f'tcp://{self.preferred_network}:{self.port}')
                    ipc_address = ipc_address_for_port(self.port)
                    self.socket.bind(ipc_address)
                    self.bind_addresses.append(ipc_address)
                    self.address = Address(preferred_address, self.port)

                else:
                    for bind_address in self.bind_addresses:
                        self.socket.bind(bind_address)

                logging.debug(f'starting ZeroMQ server listening on {self.bind_addresses}')
                self.task = asyncio.create_task(self._serve())

            except:
                if self.socket:
                    self.socket.close()
                if self.context:
                    self.context.term()

                raise

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self.bind_addresses:
            logging.debug(f'stopping ZeroMQ server listening on {self.bind_addresses}')
            self.task.cancel()
            self.socket.close()
            self.context.term()

        return False


class CommunicationTimeout(RuntimeError):
    pass


CLIENT_TIMEOUT_MS = 200

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
        socket_state = await self.socket.poll(CLIENT_TIMEOUT_MS, zmq.POLLOUT)
        if socket_state & zmq.POLLOUT:
            await self.socket.send(request_bytes)
        else:
            raise CommunicationTimeout()

        socket_state = await self.socket.poll(CLIENT_TIMEOUT_MS, zmq.POLLIN)
        if socket_state & zmq.POLLIN:
            response_bytes = await self.socket.recv()
            response = dill.loads(response_bytes)
        else:
            raise CommunicationTimeout()

        return response
