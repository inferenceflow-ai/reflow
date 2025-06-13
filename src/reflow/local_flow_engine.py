import argparse
import asyncio
from typing import List, Any

from reflow import FlowStage, Worker, InputQueue, EventSink, LocalEventQueue

DEFAULT_QUEUE_SIZE = 10000

# what is the network naming scheme for flow engines ?
#
# There should be one per host/core - when it is started, just give it a name
# It will be accessible on the same host as ipc://engine/nnnn
# and from remote hosts as tcp://host:nnnn where
# Lets use 5nnn as the port number

class FlowEngine:
    def __init__(self, queue_size:int = DEFAULT_QUEUE_SIZE):
        self.queue_size = queue_size
        self.running = True
        self.workers = []
        self.exit_on_completion = False

    async def run(self):
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
                        if len(self.workers) == 0 and self.exit_on_completion:
                            return # RETURN
            else:
                # to avoid a busy idle loop
                await asyncio.sleep(1)

    # noinspection PyUnboundLocalVariable,PyMethodMayBeStatic
    async def deploy(self, flow_stage: FlowStage, exit_on_completion = False):
        builder = JobBuilder(self.queue_size)
        workers = []
        builder.build_job(flow_stage, workers)
        for worker in workers:
            await worker.init()

        self.workers.extend(workers)
        self.exit_on_completion = exit_on_completion



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

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description="Run a local flow engine", add_help=True, exit_on_error=True)
    arg_parser.add_argument("port", required=True, type=int, help="The port number to listen on for deployments")
    args = arg_parser.parse_args()
    port = args.port
