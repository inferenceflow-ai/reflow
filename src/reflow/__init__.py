import logging
from abc import abstractmethod
from contextlib import ExitStack
from typing import Generic, Callable, Awaitable, Self, Any, List

from reflow.internal.worker import RoutingPolicy, LocalRoutingPolicy

from reflow.internal.network import QueueDescriptor
from reflow.internal.worker import Worker, SourceWorker, SinkWorker, TransformWorker
from reflow.typedefs import STATE_TYPE, EVENT_TYPE, InitFn, ProducerFn, ConsumerFn, OUT_EVENT_TYPE, TransformerFn, \
    IN_EVENT_TYPE


class FlowStage:
    def __init__(self, init_fn: InitFn = None, max_workers:int = 0):
        self.init_fn = init_fn
        self.state = None
        self.downstream_stages = []
        self.routing_policies = []
        self.exit_stack = ExitStack()
        self.max_workers = max_workers

    def send_to(self, next_stage: "FlowStage", routing_policy: RoutingPolicy = LocalRoutingPolicy())-> "FlowStage":
        self.downstream_stages.append(next_stage)
        self.routing_policies.append(routing_policy)
        return next_stage

    @abstractmethod
    def build_worker(self, *, preferred_network: str,
                     input_queue_size: int = None,
                     outboxes: List[List[QueueDescriptor]] = None)->Worker[Any, Any, Any]:
        pass

class EventSource(Generic[EVENT_TYPE, STATE_TYPE], FlowStage):
    def __init__(self, init_fn: InitFn = None, max_workers:int = 0):
        FlowStage.__init__(self, init_fn, max_workers=max_workers)
        self.producer_fn = None

    def with_producer_fn(self, producer_fn: ProducerFn)->Self:
        self.producer_fn = producer_fn
        return self

    def build_worker(self, *, preferred_network: str,
                     input_queue_size: int = None,
                     outboxes: List[List[QueueDescriptor]] = None)->SourceWorker[EVENT_TYPE, EVENT_TYPE, STATE_TYPE]:
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

    def build_worker(self, *, preferred_network: str,
                     input_queue_size: int = None,
                     outboxes: List[List[QueueDescriptor]] = None)->SinkWorker[EVENT_TYPE, EVENT_TYPE, STATE_TYPE]:
        return SinkWorker(init_fn=self.init_fn,
                          consumer_fn=self.consumer_fn,
                          input_queue_size = input_queue_size,
                          preferred_network=preferred_network)

    def send_to(self, next_stage: "EventSink", routing_policy: RoutingPolicy = LocalRoutingPolicy()):
        logging.warning("Attempt to connect a downstream stage to a sink has been ignored")


class EventTransformer(Generic[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE], FlowStage):
    def __init__(self, expansion_factor: int, init_fn: InitFn = None, max_workers = 0):
        FlowStage.__init__(self, init_fn=init_fn, max_workers=max_workers)
        self.expansion_factor = expansion_factor
        self.transform_fn = None

    def with_transform_fn(self, transform_fn: TransformerFn)->Self:
        self.transform_fn = transform_fn
        return self

    def build_worker(self,*, preferred_network: str,
                     input_queue_size: int = None,
                     outboxes: List[List[QueueDescriptor]] = None)->TransformWorker[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]:
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


