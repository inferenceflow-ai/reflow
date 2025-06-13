import zmq
from zmq.asyncio import Context


class ZMQClient:
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
