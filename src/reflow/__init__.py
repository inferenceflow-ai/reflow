import logging
from abc import abstractmethod
from typing import Generic, Callable, Awaitable, Self, Any, List

from .internal.event_queue import LocalEventQueue, InputQueue
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
        async def inner():
            return await init_fn(*args, **kwargs)

        return inner

    return wrapper


DEFAULT_QUEUE_SIZE = 10000


class LocalFlowEngine:
    def __init__(self, queue_size:int = DEFAULT_QUEUE_SIZE):
        self.queue_size = queue_size

    # noinspection PyUnboundLocalVariable,PyMethodMayBeStatic
    async def run(self, flow_stage: FlowStage):
        builder = JobBuilder(self.queue_size)
        workers = []
        builder.build_job(flow_stage, workers)
        for worker in workers:
            await worker.init()

        while len(workers) > 0:
            for worker in workers:
                if not worker.finished or len(worker.unsent_out_events) > 0:
                    await worker.process()
                else:
                    workers.remove(worker)

        logging.info("No workers are active.  Exiting.")


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
