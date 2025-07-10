import logging
from typing import List

from reflow.flow_engine import FlowEngineClient
from reflow import FlowStage
from reflow.internal.network import Address


class FlowCluster:
    def __init__(self, engine_addresses: List[str], preferred_network: str):
        self.engine_addresses = engine_addresses
        self.preferred_network = preferred_network

    async def deploy(self, flow_stage: FlowStage)->List[Address]:
        # outboxes contains a list of outbox addresses for each downstream stage
        # i.e. it is a list of lists
        outboxes = []
        for stage in flow_stage.downstream_stages:
            outboxes.append(await self.deploy(stage))

        # inboxes is the list of inboxes for this stage
        inboxes = []
        for address in self.engine_addresses:
            logging.info(f'Deploying {flow_stage} to engine at {address}')
            with FlowEngineClient(address) as engine:
                inbox = await engine.deploy_stage(flow_stage, outboxes=outboxes, network=self.preferred_network)
                if inbox:
                    inboxes.append(inbox)

        return inboxes
