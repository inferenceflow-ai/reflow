from abc import abstractmethod
from contextlib import ExitStack
from dataclasses import dataclass, field
from enum import Enum
from typing import TypeVar, Callable, List, Any, Optional, Generic, TYPE_CHECKING

if TYPE_CHECKING:
    from reflow.internal.worker import Worker

from reflow import RoutingPolicy, LocalRoutingPolicy
from reflow.internal.network import ipc_address_for_port

EVENT_TYPE = TypeVar('EVENT_TYPE')
IN_EVENT_TYPE = TypeVar('IN_EVENT_TYPE')
OUT_EVENT_TYPE = TypeVar('OUT_EVENT_TYPE')
STATE_TYPE = TypeVar('STATE_TYPE')

InitFn = Callable[[], STATE_TYPE]
ProducerFn = Callable[[STATE_TYPE, int], List[EVENT_TYPE]] | Callable[[int], List[EVENT_TYPE]]
ConsumerFn = Callable[[STATE_TYPE, List[EVENT_TYPE]], int] | Callable[[List[EVENT_TYPE]], int]
TransformerFn = Callable[[STATE_TYPE, IN_EVENT_TYPE], List[OUT_EVENT_TYPE]] | Callable[[IN_EVENT_TYPE], List[OUT_EVENT_TYPE]]
KeyFn = Callable[[EVENT_TYPE], Any]

# Use this in a producer function to signify there are no items left
class EndOfStreamException(Exception):
    pass

@dataclass(frozen=True)
class Address:
    ip: str
    port: int

    def ipc_bind_address(self):
        return ipc_address_for_port(self.port)

    def tcp_bind_address(self):
        return f'tcp://{self.ip}:{self.port}'


@dataclass
class WorkerDescriptor:
    cluster_size: int
    cluster_number: int
    worker_number: int
    engine_address: str = field(init=False)
    address: Optional[Address] = None



class FlowStage:
    def __init__(self, init_fn: InitFn = None, max_workers:int = 0):
        self.init_fn = init_fn
        self.state = None
        self.downstream_stages = []
        self.routing_policies = []
        self.exit_stack = ExitStack()
        self.max_workers = max_workers
        self.worker_descriptors = []

    def send_to(self, next_stage: "FlowStage", routing_policy: RoutingPolicy = LocalRoutingPolicy())-> "FlowStage":
        self.downstream_stages.append(next_stage)
        self.routing_policies.append(routing_policy)
        return next_stage

    @abstractmethod
    def build_worker(self, *, preferred_network: str,
                     input_queue_size: int = None,
                     outboxes: List[List[WorkerDescriptor]] = None)->Worker[Any, Any, Any]:
        pass



@dataclass
class DeployStageRequest:
    stage: FlowStage
    outboxes: List[List[WorkerDescriptor]]


@dataclass
class DeployStageResponse:
    inbox_address: Optional[WorkerDescriptor]


@dataclass
class ShutdownRequest:
    pass


@dataclass
class ShutdownResponse:
    pass


@dataclass
class QuiesceWorkerRequest:
    descriptor: WorkerDescriptor
    timeout_secs: float


@dataclass
class QuiesceWorkerResponse:
    success: bool


@dataclass
class RemoveWorkerRequest:
    descriptor: WorkerDescriptor


@dataclass
class RemoveWorkerResponse:
    pass


class INSTRUCTION(Enum):
    PROCESS_EVENT = 1


@dataclass(frozen=True)
class WorkerId:
    cluster_number: int
    worker_number: int


class Envelope(Generic[EVENT_TYPE]):
    def __init__(self, instruction: INSTRUCTION, source_id: WorkerId, sequence_num: int, event: EVENT_TYPE = None):
        self.instruction = instruction
        self.source_id = source_id
        self.sequence_num = sequence_num
        self.event = event


def wrap(events: List[EVENT_TYPE]) -> List[Envelope[EVENT_TYPE]]:
    return [
        Envelope(INSTRUCTION.PROCESS_EVENT, WorkerId(cluster_number=0, worker_number=0), sequence_num=0, event=event)
        for event in events]


def unwrap(envelopes: List[Envelope[EVENT_TYPE]]) -> List[EVENT_TYPE]:
    return [envelope.event for envelope in envelopes]
