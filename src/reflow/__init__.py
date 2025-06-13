from abc import abstractmethod
from typing import Generic, Callable, Awaitable, Self, Any

from .internal.worker import Worker, SourceWorker, SinkWorker, SplitWorker
from .typedefs import STATE_TYPE, EVENT_TYPE, InitFn, ProducerFn, ConsumerFn, OUT_EVENT_TYPE, SplitFn, IN_EVENT_TYPE


class FlowStage:
    def __init__(self, init_fn: InitFn = None):
        self.init_fn = init_fn
        self.state = None
        self.downstream_stages = []

    def send_to(self, next_stage: "FlowStage")->"FlowStage":
        self.downstream_stages.append(next_stage)
        return next_stage

    @abstractmethod
    def build_worker(self)->Worker[Any, Any, Any]:
        pass


class EventSource(Generic[EVENT_TYPE, STATE_TYPE], FlowStage):
    def __init__(self, init_fn: InitFn = None):
        FlowStage.__init__(self, init_fn)
        self.producer_fn = None

    def with_producer_fn(self, producer_fn: ProducerFn)->Self:
        self.producer_fn = producer_fn
        return self

    def build_worker(self)->SourceWorker[EVENT_TYPE, EVENT_TYPE, STATE_TYPE]:
        return SourceWorker(init_fn=self.init_fn, producer_fn=self.producer_fn)


class EventSink(Generic[EVENT_TYPE, STATE_TYPE], FlowStage):
    def __init__(self, init_fn: InitFn = None):
        FlowStage.__init__(self, init_fn)
        self.consumer_fn = None

    def with_consumer_fn(self, consumer_fn: ConsumerFn)->Self:
        self.consumer_fn = consumer_fn
        return self

    def build_worker(self)->SinkWorker[EVENT_TYPE, EVENT_TYPE, STATE_TYPE]:
        return SinkWorker(init_fn=self.init_fn, consumer_fn=self.consumer_fn)

    def send_to(self, next_stage: "EventSink"):
        # TODO log a warning
        pass


class Splitter(Generic[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE], FlowStage):
    def __init__(self, expansion_factor: int, init_fn: InitFn = None):
        FlowStage.__init__(self, init_fn=init_fn)
        self.expansion_factor = expansion_factor
        self.split_fn = None

    def with_split_fn(self, split_fn: SplitFn)->Self:
        self.split_fn = split_fn
        return self

    def build_worker(self)->SplitWorker[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE]:
        return SplitWorker(init_fn=self.init_fn, split_fn=self.split_fn, expansion_factor=self.expansion_factor)


def flow_connector_factory(init_fn: Callable[..., Awaitable[STATE_TYPE]])->Callable[..., InitFn]:
    """
    A decorator that is used to create custom connections to outside services.
    """
    def wrapper(*args, **kwargs)->Callable[[],Awaitable[STATE_TYPE]]:
        def inner():
            return init_fn(*args, **kwargs)

        return inner

    return wrapper


