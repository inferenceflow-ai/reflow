import asyncio
import itertools
import logging
import os
import traceback
from asyncio import Event
from dataclasses import dataclass
from enum import IntEnum
from typing import List, Any, Optional

from reflow import FlowStage
from reflow.internal import WorkerId
from reflow.internal.network import QueueDescriptor
from reflow.internal.worker import SourceAdapter
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
    outboxes: List[List[QueueDescriptor]]


@dataclass
class DeployStageResponse:
    inbox_address: Optional[QueueDescriptor]


@dataclass
class RequestShutdownRequest:
    pass


@dataclass
class RequestShutdownResponse:
    pass


@dataclass
class WaitForAllWorkCompletedRequest:
    timeout: int


@dataclass
class WaitForAllWorkCompletedResponse:
    success: bool


@dataclass
class FinalizeShutdownRequest:
    pass


@dataclass
class FinalizeShutdownResponse:
    pass


class EngineState(IntEnum):
    RUNNING = 1
    SHUTDOWN_REQUESTED = 2
    READY_TO_SHUTDOWN = 3


class FlowEngine(ZMQServer):
    def __init__(self, *, cluster_number: int, cluster_size: int, default_queue_size: int, preferred_network: str,
                 bind_addresses: List[str]):
        ZMQServer.__init__(self, bind_addresses=bind_addresses)
        self.preferred_network = preferred_network
        self.default_queue_size = default_queue_size
        self.running = True
        self.workers = []
        self.state = EngineState.RUNNING
        self.all_work_completed = Event()
        self.cluster_number = cluster_number
        self.cluster_size = cluster_size
        self.worker_id = itertools.count()

    async def process_request(self, request: Any) -> Any:
        if isinstance(request, DeployStageRequest):
            inbox_address = await self.deploy_stage(stage=request.stage, network=request.preferred_network,
                                                    outboxes=request.outboxes)
            return DeployStageResponse(inbox_address=inbox_address)
        elif isinstance(request, RequestShutdownRequest):
            await self.request_shutdown()
            return RequestShutdownResponse()
        elif isinstance(request, WaitForAllWorkCompletedRequest):
            success = await self.wait_for_all_work_completed(request.timeout)
            return WaitForAllWorkCompletedResponse(success=success)
        elif isinstance(request, FinalizeShutdownRequest):
            await self.finalize_shutdown()
            return FinalizeShutdownResponse()
        else:
            raise RuntimeError(f'Request must be an instance of a known request type.  Received {type(request)}')

    async def run(self):
        logging.info("(%d) FlowEngine Started", os.getpid())
        try:
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
                            if len(self.workers) == 0:
                                self.all_work_completed.set()

                elif self.state == EngineState.READY_TO_SHUTDOWN:
                    break
                else:
                    # to avoid a busy idle loop
                    await asyncio.sleep(.5)

                await asyncio.sleep(0)  # yield to other tasks

            logging.info("(%d) FlowEngine stopped", os.getpid())

        except Exception as _:
            error = traceback.format_exc()
            logging.error(f"Main loop of FlowEngine exited due to an exception: \n{error}")


    async def deploy_stage(self, stage: FlowStage, outboxes: List[List[QueueDescriptor]],
                           network: str) -> QueueDescriptor | None:
        if self.state == EngineState.SHUTDOWN_REQUESTED  :
            logging.warn(f'deploy request ignored because the engine is shutting down')
            return None
        else:
            self.all_work_completed.clear()

            worker = stage.build_worker(input_queue_size=self.default_queue_size,
                                        preferred_network=network,
                                        outboxes=outboxes)
            worker.id = WorkerId(cluster_number=self.cluster_number, worker_number=next(self.worker_id))
            await worker.init()
            self.workers.append(worker)
            if worker.input_queue and not isinstance(worker.input_queue, SourceAdapter):
                return QueueDescriptor(address=worker.input_queue.address, cluster_size=self.cluster_size,
                                       cluster_number=self.cluster_number)
            else:
                return None

    # NOTE: the 3 method below are each a part of the shutdown sequence which is coordinated at the cluster  level.
    # It is generally not a good idea to call them directly on a single FlowEngine

    async def request_shutdown(self):
        """
        Requests shutdown.  This will inhibit attempts to deploy a new jobs.  This method does not wait for
        the engine to exit and the engine will only exit after there are no more active workers, if ever.

        To wait for the engine to exit, request shutdown, then await the task that is running the FlowEngine.run method.
        """
        self.state = EngineState.SHUTDOWN_REQUESTED

    async def wait_for_all_work_completed(self, timeout: int)->bool:
        try:
            await asyncio.wait_for(self.all_work_completed.wait(), timeout=timeout)
            return True
        except TimeoutError:
            return False

    async def finalize_shutdown(self):
        self.state = EngineState.READY_TO_SHUTDOWN


class FlowEngineClient(ZMQClient):
    def __init__(self, server_address: str):
        ZMQClient.__init__(self, server_address)

    async def deploy_stage(self, stage: FlowStage, outboxes: List[List[QueueDescriptor]], network: str) -> QueueDescriptor:
        deploy_request = DeployStageRequest(stage=stage, outboxes=outboxes, preferred_network=network)
        response = await self.send_request(deploy_request)
        return response.inbox_address

    async def request_shutdown(self):
        request = RequestShutdownRequest()
        await self.send_request(request)

    async def wait_for_all_work_completed(self, timeout: int)->bool:
        request = WaitForAllWorkCompletedRequest(timeout=timeout)
        result = await self.send_request(request, timeout = timeout * 1000 + 200)
        return result.success

    async def finalize_shutdown(self):
        request = FinalizeShutdownRequest()
        await self.send_request(request)

# async def main(port: int, preferred_network: str):
#     zmq_bind_addresses = [f'ipc://flow_engine_{port:04d}', f'tcp://*:{port}']
#     with FlowEngine(DEFAULT_QUEUE_SIZE, bind_addresses=zmq_bind_addresses,
#                     preferred_network=preferred_network) as flow_engine:
#         task = asyncio.create_task(flow_engine.run())
#         await task


# if __name__ == '__main__':
#     logging.basicConfig(level=logging.INFO)
#     arg_parser = argparse.ArgumentParser(description="Run a local flow engine", add_help=True, exit_on_error=True)
#     arg_parser.add_argument("port", type=int, help="The port number to listen on for deployments")
#     arg_parser.add_argument("network", type=str, help="The network prefix that inbox servers will listen on")
#     args = arg_parser.parse_args()
#
#     asyncio.run(main(args.port, args.network))
