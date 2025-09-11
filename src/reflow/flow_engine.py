import argparse
import asyncio
import itertools
import logging
import os
from asyncio import CancelledError
from typing import List, Any

from reflow import FlowStage, DeployStageRequest, DeployStageResponse, ShutdownRequest, ShutdownResponse, \
    QuiesceWorkerRequest, QuiesceWorkerResponse, RemoveWorkerRequest, RemoveWorkerResponse
from reflow.internal import WorkerId
from reflow.internal.network import WorkerDescriptor
from reflow.internal.worker import SourceAdapter
from reflow.internal.zmq import ZMQServer, ZMQClient, DEFAULT_CLIENT_TIMEOUT_MS

DEFAULT_QUEUE_SIZE = 10_000


class FlowEngine(ZMQServer):
    def __init__(self, *,
                 cluster_number: int,
                 cluster_size: int,
                 port: int,
                 preferred_network: str = None,
                 default_queue_size: int = DEFAULT_QUEUE_SIZE):
        """
        Creates a new FlowEngine server

        Args:
            cluster_number:     the id of this particular instance in the cluster - must be in [0,cluster_size)
            cluster_size:       the total number of instances in the cluster
            port:               the port to listen on
            preferred_network:  the network interface prefix to listen on - if not provided, will listen on all interfaces
            default_queue_size: the size of the message queues between workers
        """
        ZMQServer.__init__(self, preferred_network=preferred_network, port=port)
        self.default_queue_size = default_queue_size
        self.running = True
        self.workers = []
        self.shutdown_requested = False
        self.cluster_number = cluster_number
        self.cluster_size = cluster_size
        self.worker_id = itertools.count()

    async def process_request(self, request: Any) -> Any:
        if isinstance(request, DeployStageRequest):
            inbox_address = await self.deploy_stage(stage=request.stage, outboxes=request.outboxes)
            return DeployStageResponse(inbox_address=inbox_address)
        elif isinstance(request, ShutdownRequest):
            await self.request_shutdown()
            return ShutdownResponse()
        elif isinstance(request, QuiesceWorkerRequest):
            success = await self.quiesce_worker(request.descriptor, request.timeout_secs)
            return QuiesceWorkerResponse(success=success)
        elif isinstance(request, RemoveWorkerRequest):
            await self.remove_worker(request.descriptor)
            return RemoveWorkerResponse()
        else:
            msg = f'Request must be an instance of a known request type.  Received {type(request)}'
            logging.warning(msg)
            raise RuntimeError(f'Request must be an instance of a known request type.  Received {type(request)}')

    async def run(self):
        logging.info("(%d) FlowEngine Started", os.getpid())
        while True:
            if len(self.workers) > 0:
                for worker in self.workers.copy():
                    # iterate over a copy to avoid problems related to removing while iterating
                    # it is a shallow copy - we remove from self.workers but iterate over a
                    # copy
                    if not worker.quiescent.is_set():
                        await worker.process()
            elif self.shutdown_requested:
                break
            else:
                # to avoid a busy idle loop
                await asyncio.sleep(1)

            await asyncio.sleep(0)  # yield to other tasks

        logging.info("(%d) FlowEngine stopped", os.getpid())

    async def deploy_stage(self, stage: FlowStage, outboxes: List[List[WorkerDescriptor]]) -> WorkerDescriptor | None:
        worker = stage.build_worker(input_queue_size=self.default_queue_size,
                                    preferred_network=self.preferred_network,
                                    outboxes=outboxes)
        worker.id = WorkerId(cluster_number=self.cluster_number, worker_number=next(self.worker_id))
        await worker.init()
        self.workers.append(worker)

        if worker.input_queue and not isinstance(worker.input_queue, SourceAdapter):
            address = worker.input_queue.address
        else:
            address = None

        return WorkerDescriptor(address=address,
                                cluster_size=self.cluster_size,
                                cluster_number=self.cluster_number,
                                worker_number=worker.id.worker_number)

    async def request_shutdown(self):
        """
        Requests shutdown.  This will inhibit attempts to deploy a new jobs.  This method does not wait for
        the engine to exit and the engine will only exit after there are no more active workers, if ever.

        To wait for the engine to exit, request shutdown, then await the task that is running the FlowEngine.run method.
        """
        logging.info("Shutdown requested")
        self.shutdown_requested = True

    async def quiesce_worker(self, descriptor: WorkerDescriptor, timeout_secs: float) -> bool:
        worker = self.find_worker(descriptor)
        if not worker:
            logging.warning("Worker %d not found in this engine", descriptor.worker_number)
            return True
        else:
            result = await worker.quiesce(timeout_secs)
            return result

    async def remove_worker(self, descriptor: WorkerDescriptor):
        worker = self.find_worker(descriptor)
        if not worker:
            logging.warning("Worker %d not found in this engine", descriptor.worker_number)
            return True
        else:
            worker.__exit__(None, None, None)
            self.workers.remove(worker)
            return True

    def find_worker(self, descriptor: WorkerDescriptor):
        if self.cluster_number != descriptor.cluster_number:
            assert self.cluster_number == descriptor.cluster_number

        result = None
        for worker in self.workers:
            if worker.id.worker_number == descriptor.worker_number:
                result = worker
                break

        return result


class FlowEngineClient(ZMQClient):
    def __init__(self, server_address: str):
        ZMQClient.__init__(self, server_address)

    async def deploy_stage(self, stage: FlowStage, outboxes: List[List[WorkerDescriptor]]) -> WorkerDescriptor:
        deploy_request = DeployStageRequest(stage=stage, outboxes=outboxes)
        response = await self.send_request(deploy_request)
        return response.inbox_address

    async def request_shutdown(self):
        request = ShutdownRequest()
        await self.send_request(request)

    async def quiesce_worker(self, descriptor: WorkerDescriptor, timeout_secs: float) -> bool:
        request = QuiesceWorkerRequest(descriptor=descriptor, timeout_secs=timeout_secs)
        result = await self.send_request(request, timeout=1000 * timeout_secs + DEFAULT_CLIENT_TIMEOUT_MS)
        return result.success

    async def remove_worker(self, descriptor: WorkerDescriptor):
        request = RemoveWorkerRequest(descriptor=descriptor)
        result = await self.send_request(request)

async def main(*, preferred_network: str, port: int, cluster_number: int, cluster_size: int):
    with FlowEngine(cluster_number=cluster_number,
                    cluster_size=cluster_size,
                    default_queue_size=DEFAULT_QUEUE_SIZE,
                    preferred_network=preferred_network,
                    port = port) as flow_engine:
        task = asyncio.create_task(flow_engine.run())
        try:
            await task
            logging.info("Flow engine finished")
        except CancelledError:
            logging.info("Flow engine task cancelled")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    arg_parser = argparse.ArgumentParser(description="Run a local flow engine", add_help=True, exit_on_error=True)
    arg_parser.add_argument("--cluster-number", type=int, required=True, help="The id of this flow engine instance.  Must be in the range [0,cluster-size)")
    arg_parser.add_argument("--cluster-size", type=int, required=True, help="The number of flow engine instances in this cluster.")
    arg_parser.add_argument("--port", required=True, type=int, help="The port number to listen on for deployments")
    arg_parser.add_argument("--network", required=False, type=str, help="The network prefix that inbox servers will listen on")
    args = arg_parser.parse_args()

    asyncio.run(main(preferred_network=args.network,
                     port=args.port,
                     cluster_size=args.cluster_size,
                     cluster_number=args.cluster_number))
