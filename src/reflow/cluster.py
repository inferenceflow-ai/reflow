import logging
import random
import uuid
from typing import List

from reflow import FlowStage
from reflow.flow_engine import FlowEngineClient
from reflow.internal.network import QueueDescriptor


class FlowCluster:
    def __init__(self, engine_addresses: List[str], preferred_network: str):
        self.engine_addresses = engine_addresses
        self.preferred_network = preferred_network

    async def deploy_job(self, flow_stage: FlowStage)->str:
        job_id = str(uuid.uuid4())
        await self._deploy(flow_stage, job_id)
        return job_id

    async def _deploy(self, flow_stage: FlowStage, job_id: str)->List[QueueDescriptor]:
        # outboxes contains a list of outbox addresses for each downstream stage
        # i.e. it is a list of lists
        outboxes = []
        for stage in flow_stage.downstream_stages:
            outboxes.append(await self._deploy(stage, job_id))

        # inboxes is the list of inboxes for this stage
        inboxes = []
        if flow_stage.max_workers > 0:
            m = min(flow_stage.max_workers, len(self.engine_addresses))
            target_addresses = random.sample(self.engine_addresses, m)
        else:
            target_addresses = self.engine_addresses

        for address in target_addresses:
            logging.info(f'Deploying {flow_stage} to engine at {address}')
            with FlowEngineClient(address) as engine:
                inbox = await engine.deploy_stage(flow_stage, job_id, outboxes=outboxes, network=self.preferred_network)
                if inbox:
                    inboxes.append(inbox)

        return inboxes

    async def request_shutdown(self):
        for engine_addr in  self.engine_addresses:
            with FlowEngineClient(engine_addr) as engine:
                await engine.request_shutdown()
                logging.info(f'Requested shutdown of engine at {engine_addr}')
