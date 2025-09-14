import inspect
import logging
import os.path
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Generic, Callable, Awaitable, Self, Any, List, Optional

from reflow.internal.network import WorkerDescriptor
from reflow.internal.worker import RoutingPolicy, LocalRoutingPolicy
from reflow.internal.worker import Worker, SourceWorker, SinkWorker, TransformWorker
from reflow.typedefs import STATE_TYPE, EVENT_TYPE, InitFn, ProducerFn, ConsumerFn, OUT_EVENT_TYPE, TransformerFn, \
    IN_EVENT_TYPE


def get_calling_module_name()->str:
    frame = inspect.currentframe().f_back
    while frame:
        module_name = frame.f_globals['__name__']
        if module_name != 'reflow':
            break
        frame = frame.f_back

    if module_name == '__main__':
        file = frame.f_globals['__file__']
        module_name = os.path.splitext(os.path.basename(file))[0]

    return module_name

def find_stage(base_stage: "FlowStage", address: List[int])->"FlowStage":
    if len(address) == 0:
        return base_stage

    if address[0] >= len(base_stage.downstream_stages):
        raise IndexError()

    next_stage = base_stage.downstream_stages[address[0]]
    return find_stage(next_stage, address[1:])

class FlowStage(ABC):
    def __init__(self, init_fn: InitFn = None, max_workers:int = 0):
        self.init_fn = init_fn
        self.state = None
        self.downstream_stages = []
        self.routing_policies = []
        self.max_workers = max_workers
        self.worker_descriptors = []
        self.flow_module = get_calling_module_name()
        self.address = []

    def send_to(self, next_stage: "FlowStage", routing_policy: RoutingPolicy = LocalRoutingPolicy())-> "FlowStage":
        child_num = len(self.downstream_stages)
        next_stage.address = self.address.copy()
        next_stage.address.append(child_num)
        self.downstream_stages.append(next_stage)
        self.routing_policies.append(routing_policy)
        return next_stage

    @abstractmethod
    def build_worker(self, *, preferred_network: str,
                     input_queue_size: int = None,
                     outboxes: List[List[WorkerDescriptor]] = None)->Worker[Any, Any, Any]:
        pass

class EventSource(Generic[EVENT_TYPE, STATE_TYPE], FlowStage):
    def __init__(self, init_fn: InitFn = None, max_workers:int = 0):
        FlowStage.__init__(self, init_fn, max_workers=max_workers)
        self.producer_fn = None

    def with_producer_fn(self, producer_fn: ProducerFn)->Self:
        self.producer_fn = producer_fn
        return self

    def build_worker(self, *, preferred_network: str = None,
                     input_queue_size: int = None,
                     outboxes: List[List[WorkerDescriptor]] = None)->SourceWorker[EVENT_TYPE, EVENT_TYPE, STATE_TYPE]:
        return SourceWorker(init_fn=self.init_fn,
                            producer_fn=self.producer_fn,
                            outboxes=outboxes,
                            routing_policies = self.routing_policies,
                            preferred_network=preferred_network)


class EventSink(Generic[EVENT_TYPE, STATE_TYPE], FlowStage):
    def __init__(self, init_fn: InitFn = None, max_workers=0):
        FlowStage.__init__(self, init_fn, max_workers=max_workers)
        self.consumer_fn = None

    def with_consumer_fn(self, consumer_fn: ConsumerFn)->Self:
        self.consumer_fn = consumer_fn
        return self

    def build_worker(self, *, preferred_network: str = None,
                     input_queue_size: int = None,
                     outboxes: List[List[WorkerDescriptor]] = None)->SinkWorker[EVENT_TYPE, EVENT_TYPE, STATE_TYPE]:
        return SinkWorker(init_fn=self.init_fn,
                          consumer_fn=self.consumer_fn,
                          input_queue_size = input_queue_size,
                          preferred_network=preferred_network)

    def send_to(self, next_stage: "EventSink", routing_policy: RoutingPolicy = LocalRoutingPolicy()):
        logging.warning("Attempt to connect a downstream stage to a sink has been ignored")


class EventTransformer(Generic[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE], FlowStage):
    def __init__(self, expansion_factor: float, init_fn: InitFn = None, max_workers = 0):
        FlowStage.__init__(self, init_fn=init_fn, max_workers=max_workers)
        self.expansion_factor = expansion_factor
        self.transform_fn = None

    def with_transform_fn(self, transform_fn: TransformerFn)->Self:
        self.transform_fn = transform_fn
        return self

    def build_worker(self, *, preferred_network: str,
                     input_queue_size: int = None,
                     outboxes: List[List[WorkerDescriptor]] = None)->TransformWorker[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]:
        return TransformWorker(init_fn=self.init_fn,
                               transform_fn=self.transform_fn,
                               expansion_factor=self.expansion_factor,
                               input_queue_size = input_queue_size,
                               outboxes=outboxes,
                               routing_policies = self.routing_policies,
                               preferred_network=preferred_network)


def flow_connector_factory(init_fn: Callable[..., Awaitable[STATE_TYPE]])->Callable[..., InitFn]:
    """
    A decorator that is used to create custom connections to outside services.
    """
    def wrapper(*args, **kwargs)->Callable[[],Awaitable[STATE_TYPE]]:
        def inner():
            return init_fn(*args, **kwargs)

        return inner

    return wrapper


@dataclass
class DeployStageRequest:
    flow_module: str
    stage_address: List[int]
    outboxes: List[List[WorkerDescriptor]]


@dataclass
class DeployStageResponse:
    inbox_address: Optional[WorkerDescriptor]
    error: Optional[str] = None


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
