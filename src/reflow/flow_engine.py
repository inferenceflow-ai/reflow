import argparse
import asyncio
import logging
from dataclasses import dataclass
from typing import List, Any

from reflow.internal.zmq import ZMQServer, ZMQClient
from reflow import FlowStage, Worker, EventSink
from reflow.internal.event_queue import InputQueue, DequeueEventQueue

DEFAULT_QUEUE_SIZE = 10000

# what is the network naming scheme for flow engines ?
#
# There should be one per host/core - when it is started, just give it a name
# It will be accessible on the same host as ipc://engine/nnnn
# and from remote hosts as tcp://host:nnnn where
# Lets use 5nnn as the port number


@dataclass
class DeployRequest:
    flow: FlowStage


@dataclass
class DeployResponse:
    pass


@dataclass
class ShutdownRequest:
    pass


@dataclass
class ShutdownResponse:
    pass


class FlowEngine(ZMQServer):
    def __init__(self, default_queue_size:int, bind_addresses: List[str]):
        ZMQServer.__init__(self, bind_addresses)
        self.default_queue_size = default_queue_size
        self.running = True
        self.workers = []
        self.shutdown_requested = False

    async def process_request(self, request: Any) -> Any:
        if isinstance(request, DeployRequest):
            await self.deploy(request.flow)
            return DeployResponse()
        elif isinstance(request, ShutdownRequest):
            success = await self.request_shutdown()
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
                    if not worker.finished or len(worker.in_out_buffer.unsent_out_events) > 0:
                        await worker.process()
                    else:
                        self.workers.remove(worker)
            elif self.shutdown_requested:
                break
            else:
                # to avoid a busy idle loop
                await asyncio.sleep(1)

        logging.info("FlowEngine stopped")

    # noinspection PyUnboundLocalVariable,PyMethodMayBeStatic
    async def deploy(self, flow_stage: FlowStage):
        if self.shutdown_requested:
            raise RuntimeError('Cannot deploy job because engine is shutting down.')

        builder = JobBuilder(self.default_queue_size)
        workers = []
        builder.build_job(flow_stage, workers)
        for worker in workers:
            await worker.init()

        self.workers.extend(workers)

    async def request_shutdown(self):
        """
        Requests shutdown.  This will inhibit attempts to deploy an new jobs.  This method does not wait for
        the engine to exit and the engine will only exit after there are no more active workers, if ever.

        To wait for the engine to exit, request shutdown, then await the task that is running the FlowEngine.run method.
        """
        self.shutdown_requested = True


class FlowEngineClient(ZMQClient):
    def __init__(self, server_address: str):
        ZMQClient.__init__(self, server_address)

    async def deploy(self, flow_stage: FlowStage):
        deploy_request = DeployRequest(flow_stage)
        await self.send_request(deploy_request)

    async def request_shutdown(self):
        request = ShutdownRequest()
        await self.send_request()


class JobBuilder:
    def __init__(self, queue_size):
        self.queue_size = queue_size

    def build_job(self, stage: FlowStage, worker_list: List[Worker[Any, Any, Any]], input_queue: InputQueue = None)->None:
        # Create the workers for this stage, then create the output queue and connect the workers.
        # Finally, for the subsequent stages, connect them to the input queue and repeat recursively.
        worker = stage.build_worker()
        worker_list.append(worker)
        if input_queue:
            worker.input_queue = input_queue

        if not isinstance(stage, EventSink):
            output_queue = DequeueEventQueue(self.queue_size)
            worker.output_queue = output_queue
            for downstream_stage in stage.downstream_stages:
                self.build_job(downstream_stage, worker_list, output_queue)


async def main(port: int):
    zmq_bind_addresses = [f'ipc://flow_engine_{port:04d}', f'tcp://*:{port}']
    with FlowEngine(DEFAULT_QUEUE_SIZE, zmq_bind_addresses) as flow_engine:
        task = asyncio.create_task(flow_engine.run())
        await task


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    arg_parser = argparse.ArgumentParser(description="Run a local flow engine", add_help=True, exit_on_error=True)
    arg_parser.add_argument("port", type=int, help="The port number to listen on for deployments")
    args = arg_parser.parse_args()

    asyncio.run(main(args.port))


