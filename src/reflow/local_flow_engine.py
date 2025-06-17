import asyncio
from dataclasses import dataclass
from typing import List, Any

from reflow import FlowStage, Worker, EventSink
from reflow.internal.event_queue import InputQueue, LocalEventQueue

DEFAULT_QUEUE_SIZE = 10000

@dataclass
class DeployJobRequest:
    flow: FlowStage
    exit_on_completion: bool


# what is the network naming scheme for flow engines ?
#
# There should be one per host/core - when it is started, just give it a name
# It will be accessible on the same host as ipc://engine/nnnn
# and from remote hosts as tcp://host:nnnn where
# Lets use 5nnn as the port number

class FlowEngine:
    def __init__(self, queue_size: int = DEFAULT_QUEUE_SIZE):
        self.queue_size = queue_size
        self.running = True
        self.worker_tasks = []
        self.shutting_down = False

    # async def process_request(self, request: DeployJobRequest) -> Any:
    #     await self.deploy(request.flow, request.exit_on_completion)

    # noinspection PyUnboundLocalVariable,PyMethodMayBeStatic
    async def deploy(self, flow_stage: FlowStage):
        if self.shutting_down:
            raise RuntimeError('The job cannot be deployed because the engine is shutting down')

        builder = JobBuilder(self.queue_size)
        workers = []
        builder.build_job(flow_stage, workers)
        await asyncio.gather(*[worker.init() for worker in workers])
        tasks = [asyncio.create_task(worker.process()) for worker in workers]  # tasks start running here
        self.worker_tasks.extend(tasks)


    async def shutdown_when_done(self):
        self.shutting_down = True
        await asyncio.gather(*self.worker_tasks)


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
            output_queue = LocalEventQueue(self.queue_size)
            worker.output_queue = output_queue
            for downstream_stage in stage.downstream_stages:
                self.build_job(downstream_stage, worker_list, output_queue)

# if __name__ == '__main__':
#     arg_parser = argparse.ArgumentParser(description="Run a local flow engine", add_help=True, exit_on_error=True)
#     arg_parser.add_argument("port", required=True, type=int, help="The port number to listen on for deployments")
#     args = arg_parser.parse_args()
#     port = args.port
#
#     bind_address_list = [f'ipc://{port:04d}', f'tcp://*:{port}']
#     with FlowEngine(bind_address_list) as engine:
#         pass
    
