import logging
import random
import uuid
from typing import List

from reflow import FlowStage
from reflow.flow_engine import FlowEngineClient
from reflow.internal.network import WorkerDescriptor


class FlowCluster:
    def __init__(self, engine_addresses: List[str]):
        self.engine_addresses = engine_addresses
        self.deployed_jobs = {}

    async def deploy(self, source: FlowStage)->str:
        job_id = str(uuid.uuid4())

        await self._deploy_graph(source)

        self.deployed_jobs[job_id] = source
        return job_id

    async def _deploy_graph(self, flow_stage: FlowStage)->List[WorkerDescriptor]:
        # outboxes contains a list of outbox addresses for each downstream stage
        # i.e. it is a list of lists
        outboxes = []
        for stage in flow_stage.downstream_stages:
            outboxes.append(await self._deploy_graph(stage))

        # worker_descriptors is the list of worker_descriptors for this stage - it is used by upstream stages
        # to locate the inboxes for this stage
        worker_descriptors = []
        if flow_stage.max_workers > 0:
            m = min(flow_stage.max_workers, len(self.engine_addresses))
            target_addresses = random.sample(self.engine_addresses, m)
        else:
            target_addresses = self.engine_addresses

        for address in target_addresses:
            logging.info(f'Deploying {flow_stage} to engine at {address}')
            with FlowEngineClient(address) as engine:
                worker_descriptor = await engine.deploy_stage(flow_stage, outboxes=outboxes)
                if worker_descriptor:
                    worker_descriptor.engine_address = address
                    worker_descriptors.append(worker_descriptor)

        flow_stage.worker_descriptors = worker_descriptors
        return worker_descriptors

    async def wait_for_completion(self, job_id: str, timeout_secs: int):
        if job_id not in self.deployed_jobs:
            raise RuntimeError(f'No deployed job with id={job_id} exists')

        source = self.deployed_jobs[job_id]
        await self._quiesce(source, timeout_secs)
        await self._prune_workers(source)

    async def _quiesce(self, stage: FlowStage, timeout_secs: int):
        for worker_descriptor in stage.worker_descriptors:
            with FlowEngineClient(worker_descriptor.engine_address) as engine:
                logging.info(f"Waiting {timeout_secs}s for {worker_descriptor} to quiesce")
                await engine.quiesce_worker(worker_descriptor, timeout_secs)

            for stage in stage.downstream_stages:
                await self._quiesce(stage, timeout_secs)

    async def _prune_workers(self, stage: FlowStage):
        for s in stage.downstream_stages:
            await self._prune_workers(s)

        for worker_descriptor in stage.worker_descriptors:
            with FlowEngineClient(worker_descriptor.engine_address) as engine:
                logging.info(f"Removing {worker_descriptor}")
                await engine.remove_worker(worker_descriptor)



    async def request_shutdown(self):
        for engine_addr in  self.engine_addresses:
            with FlowEngineClient(engine_addr) as engine:
                await engine.request_shutdown()
                logging.info(f'Requested shutdown of engine at {engine_addr}')
