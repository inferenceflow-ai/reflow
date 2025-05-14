import logging
from abc import abstractmethod
from typing import Generic, Callable, Awaitable, List, Self

from internal import SplitWorker
from .internal import Worker, SourceWorker, SinkWorker

from typedefs import STATE_TYPE, EVENT_TYPE, InitFn, ProducerFn, ConsumerFn, OUT_EVENT_TYPE, SplitFn, IN_EVENT_TYPE


class FlowStage:
    def __init__(self, init_fn: InitFn = None):
        self.init_fn = init_fn
        self.state = None
        self.downstream_stages = []

    def send_to(self, next_stage: "FlowStage")->"FlowStage":
        self.downstream_stages.append(next_stage)
        return next_stage

    @abstractmethod
    def build_worker(self) ->Worker:
        pass

    def build_flow(self)->List[Worker]:
        """
        Recursively builds and connects the workers for downstream stages. The return value is the list of Workers
        for this stage, with all downstream workers connected.
        """
        downstream_workers = []
        for stage in self.downstream_stages:
            downstream_workers.append(stage.build_flow())

        # downstream workers now contains a list of worker lists
        # there is one entry in the outer list for each downstream stage consisting of the list of workers for that stage

        # currently we build only one worker per stage
        result = [self.build_worker()]

        for worker in result:
            for worker_list in downstream_workers:
                worker.add_worker_group(worker_list)

        return result


class EventSource(Generic[EVENT_TYPE, STATE_TYPE], FlowStage):
    def __init__(self, init_fn: InitFn = None):
        FlowStage.__init__(self, init_fn)
        self.producer_fn = None

    def with_producer_fn(self, producer_fn: ProducerFn)->Self:
        self.producer_fn = producer_fn
        return self

    def build_worker(self)->SourceWorker:
        return SourceWorker(init_fn=self.init_fn, producer_fn=self.producer_fn)


class EventSink(Generic[EVENT_TYPE, STATE_TYPE], FlowStage):
    def __init__(self, init_fn: InitFn = None):
        FlowStage.__init__(self, init_fn)
        self.consumer_fn = None

    def with_consumer_fn(self, consumer_fn: ConsumerFn)->Self:
        self.consumer_fn = consumer_fn
        return self

    def build_worker(self)->SinkWorker:
        return SinkWorker(init_fn=self.init_fn, consumer_fn=self.consumer_fn)

    def send_to(self, next_stage: "EventSink"):
        # TODO log a warning
        pass


class Splitter(Generic[IN_EVENT_TYPE, OUT_EVENT_TYPE, STATE_TYPE], FlowStage):
    def __init__(self, init_fn: InitFn = None):
        FlowStage.__init__(self, init_fn)
        self.split_fn = None

    def with_split_fn(self, split_fn: SplitFn)->Self:
        self.split_fn = split_fn
        return self

    def build_worker(self)->SplitWorker:
        return SplitWorker(init_fn=self.init_fn, split_fn=self.split_fn)


def flow_connector(init_fn: Callable[..., Awaitable[STATE_TYPE]])->Callable[..., InitFn]:
    """
    A decorator that is used to create custom connections to outside services.
    """
    def wrapper(*args, **kwargs)->Callable[[],Awaitable[STATE_TYPE]]:
        async def inner():
            return await init_fn(*args, **kwargs)

        return inner

    return wrapper


class LocalFlowEngine:

    async def init_workers_and_downstream_workers(self, workers: List[Worker]):
        for worker in workers:
            await worker.init()
            for downstream_worker_group in worker.next_workers:
                # note we are calling init too many times
                await self.init_workers_and_downstream_workers(downstream_worker_group)

    async def process_workers_and_downstream_workers(self, workers: List[Worker]):
        for worker in workers:
            logging.debug(f'processing worker {worker} with {worker.input_queue.qsize()} waiting events')
            await worker.process(10)
            for downstream_worker_group in worker.next_workers:
                # note we are calling init too many times
                await self.process_workers_and_downstream_workers(downstream_worker_group)

    async def run(self, flow: FlowStage):
        workers = flow.build_flow()
        await self.init_workers_and_downstream_workers(workers)
        while True:
            await self.process_workers_and_downstream_workers(workers)

