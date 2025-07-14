import logging
from abc import abstractmethod
from contextlib import ExitStack
from typing import Generic, Callable, Awaitable, Self, Any, List

from reflow.internal.network import Address
from reflow.internal.worker import Worker, SourceWorker, SinkWorker, TransformWorker
from reflow.typedefs import STATE_TYPE, EVENT_TYPE, InitFn, ProducerFn, ConsumerFn, OUT_EVENT_TYPE, TransformerFn, \
    IN_EVENT_TYPE, GroupingFn, GroupInitFn
from typedefs import GROUP_STATE_TYPE


class FlowStage:
    def __init__(self, init_fn: InitFn = None):
        self.init_fn = init_fn
        self.state = None
        self.downstream_stages = []
        self.exit_stack = ExitStack()
        self.max_workers = 0

    def with_max_workers(self, max_workers: int) -> "self":
        self.max_workers = max_workers
        return self

    def send_to(self, next_stage: "FlowStage") -> "FlowStage":
        self.downstream_stages.append(next_stage)
        return next_stage

    @abstractmethod
    def build_worker(self, *, preferred_network: str, input_queue_size: int = None,
                     outboxes: List[List[Address]] = None) -> Worker[Any, Any, Any]:
        pass


class EventSource(Generic[EVENT_TYPE, STATE_TYPE], FlowStage):
    def __init__(self, init_fn: InitFn = None):
        FlowStage.__init__(self, init_fn)
        self.producer_fn = None

    def with_producer_fn(self, producer_fn: ProducerFn) -> Self:
        self.producer_fn = producer_fn
        return self

    def build_worker(self, *, preferred_network: str, input_queue_size: int = None,
                     outboxes: List[List[Address]] = None) -> SourceWorker[EVENT_TYPE, EVENT_TYPE, STATE_TYPE]:
        return SourceWorker(init_fn=self.init_fn, producer_fn=self.producer_fn, outboxes=outboxes,
                            preferred_network=preferred_network)


class EventSink(Generic[EVENT_TYPE, STATE_TYPE], FlowStage):
    def __init__(self, init_fn: InitFn = None):
        FlowStage.__init__(self, init_fn)
        self.consumer_fn = None

    def with_consumer_fn(self, consumer_fn: ConsumerFn) -> Self:
        self.consumer_fn = consumer_fn
        return self

    def build_worker(self, *, preferred_network: str, input_queue_size: int = None,
                     outboxes: List[List[Address]] = None) -> SinkWorker[EVENT_TYPE, EVENT_TYPE, STATE_TYPE]:
        return SinkWorker(init_fn=self.init_fn, consumer_fn=self.consumer_fn, input_queue_size=input_queue_size,
                          preferred_network=preferred_network)

    def send_to(self, next_stage: "EventSink"):
        logging.warn("Attempt to connect a stage downstream of an EventSink is ignored")
        pass


class EventTransformer(Generic[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE, GROUP_STATE_TYPE], FlowStage):
    def __init__(self, init_fn: InitFn = None):
        FlowStage.__init__(self, init_fn=init_fn)
        self.expansion_factor = 1
        self.transform_fn = None
        self.group_init_fn = None
        self.grouping_key_fn = None

    def with_transform_fn(self, transform_fn: TransformerFn) -> Self:
        self.transform_fn = transform_fn
        return self

    def with_expansion_factor(self, expansion_factor) -> "self":
        self.expansion_factor = expansion_factor
        return self

    def with_group_init_fn(self, group_init_fn: GroupInitFn) -> "self":
        self.group_init_fn = group_init_fn

    def with_grouping_key_fn(self, grouping_key_fn: GroupingKeyFn):
        self.grouping_key_fn = grouping_key_fn

    def build_worker(self, *,
                     preferred_network: str,
                     input_queue_size: int = None,
                     outboxes: List[List[Address]] = None) \
            -> TransformWorker[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]:
        return TransformWorker(
            init_fn=self.init_fn,
            transform_fn=self.transform_fn,
            group_init_fn=self.group_init_fn,
            grouping_key_fn=self.grouping_key_fn,
            expansion_factor=self.expansion_factor,
            input_queue_size=input_queue_size,
            outboxes=outboxes,
            preferred_network=preferred_network)


def flow_connector_factory(init_fn: Callable[..., Awaitable[STATE_TYPE]]) -> Callable[..., InitFn]:
    """
    A decorator that is used to create custom connections to outside services.
    """

    def wrapper(*args, **kwargs) -> Callable[[], Awaitable[STATE_TYPE]]:
        def inner():
            return init_fn(*args, **kwargs)

        return inner

    return wrapper
