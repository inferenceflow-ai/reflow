import argparse
import asyncio
import logging
from dataclasses import dataclass
from typing import List, Any, Optional

from reflow.internal.worker import SourceAdapter
from reflow import FlowStage
from reflow.internal.network import Address
from reflow.internal.zmq import ZMQServer, ZMQClient

DEFAULT_QUEUE_SIZE = 10_000

# what is the network naming scheme for flow engines ?
#
# There should be one per host/core - when it is started, just give it a name
# It will be accessible on the same host as ipc://engine/nnnn
# and from remote hosts as tcp://host:nnnn where
# Lets use 5nnn as the port number

@dataclass
class DeployStageRequest:
    stage: FlowStage
    preferred_network: str
    outboxes: List[List[Address]]


@dataclass
class DeployStageResponse:
    inbox_address: Optional[Address]


@dataclass
class ShutdownRequest:
    pass


@dataclass
class ShutdownResponse:
    pass


class FlowEngine(ZMQServer):
    def __init__(self, default_queue_size:int, preferred_network: str, bind_addresses: List[str]):
        ZMQServer.__init__(self, bind_addresses=bind_addresses)
        self.preferred_network = preferred_network
        self.default_queue_size = default_queue_size
        self.running = True
        self.workers = []
        self.shutdown_requested = False

    async def process_request(self, request: Any) -> Any:
        if isinstance(request, DeployStageRequest):
            inbox_address = await self.deploy_stage(stage=request.stage, network=request.preferred_network, outboxes=request.outboxes)
            return DeployStageResponse(inbox_address=inbox_address)
        elif isinstance(request, ShutdownRequest):
            await self.request_shutdown()
            return ShutdownResponse()
        else:
            raise RuntimeError(f'Request must be an instance of a known request type.  Received {type(request)}')

    async def run(self):
        logging.info("FlowEngine started")
        while True:
            if len(self.workers) > 0:
                for worker in self.workers.copy():
                    # iterate over a copy to avoid problems related to removing while iterating
                    # it is a shallow copy - we remove from self.workers but iterate over a
                    # copy
                    if not worker.finished or worker.total_unsent_events() > 0:
                        await worker.process()
                    else:
                        worker.__exit__(None, None, None)
                        self.workers.remove(worker)
            elif self.shutdown_requested:
                break
            else:
                # to avoid a busy idle loop
                await asyncio.sleep(1)

            await asyncio.sleep(0) # yield to other tasks

        logging.info("FlowEngine stopped")

    async def deploy_stage(self, stage: FlowStage, outboxes: List[List[Address]], network: str)->Address | None:
        worker = stage.build_worker(input_queue_size=self.default_queue_size, preferred_network=network, outboxes=outboxes)
        await worker.init()
        self.workers.append(worker)
        if worker.input_queue and not isinstance(worker.input_queue, SourceAdapter):
            return worker.input_queue.address
        else:
            return None

    async def request_shutdown(self):
        """
        Requests shutdown.  This will inhibit attempts to deploy a new jobs.  This method does not wait for
        the engine to exit and the engine will only exit after there are no more active workers, if ever.

        To wait for the engine to exit, request shutdown, then await the task that is running the FlowEngine.run method.
        """
        self.shutdown_requested = True


class FlowEngineClient(ZMQClient):
    def __init__(self, server_address: str):
        ZMQClient.__init__(self, server_address)

    async def deploy_stage(self, stage: FlowStage, outboxes: List[List[Address]], network: str)->Address:
        deploy_request = DeployStageRequest(stage=stage, outboxes=outboxes, preferred_network=network)
        response = await self.send_request(deploy_request)
        return response.inbox_address

    async def request_shutdown(self):
        request = ShutdownRequest()
        await self.send_request(request)


async def main(port: int, preferred_network: str):
    zmq_bind_addresses = [f'ipc://flow_engine_{port:04d}', f'tcp://*:{port}']
    with FlowEngine(DEFAULT_QUEUE_SIZE, bind_addresses=zmq_bind_addresses, preferred_network=preferred_network) as flow_engine:
        task = asyncio.create_task(flow_engine.run())
        await task


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    arg_parser = argparse.ArgumentParser(description="Run a local flow engine", add_help=True, exit_on_error=True)
    arg_parser.add_argument("port", type=int, help="The port number to listen on for deployments")
    arg_parser.add_argument("network", type=str, help="The network prefix that inbox servers will listen on")
    args = arg_parser.parse_args()

    asyncio.run(main(args.port, args.network))


