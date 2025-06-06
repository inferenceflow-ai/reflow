from collections import deque, defaultdict
from typing import Protocol, TypeVar, List

from reflow.internal import Envelope

EVENT_TYPE = TypeVar('EVENT_TYPE')


class InputQueue(Protocol[EVENT_TYPE]):
    async def get_events(self, subscriber: str, limit: int = 0)->List[Envelope[EVENT_TYPE]]:
        """
        Get up to limit events from the queue, starting with the first _unacknowledged_ event. If limit is not provided
        or is 0, all ready events will be returned.

        If "get_events" is called again with no intervening calls to acknowledge_events, the returned events will
        contain the same list of events and possibly additional events that were enqueued since the last time.

        If fewer than limit events are available, this method will return only the events that are
        immediately available. It will not wait for more events to be enqueued.

        If no events are available an empty list will be returned
        """
        ...

    async def acknowledge_events(self, subscriber: str, n: int)->None:
        """
        Acknowledges the last n events have been processed and may be discarded. If there are less than n events
        in the queue then all events will be discarded but no error will be raised
        """
        ...


class OutputQueue(Protocol[EVENT_TYPE]):
    """
    EventQueues are ordered, FIFO collections with a fixed size.
    """
    async def enqueue(self, events: List[Envelope[EVENT_TYPE]])->int:
        """
        Attempts to enqueue events.  If the remaining queue capacity is less than the number of events provided,
        as many events as possible will be enqueued, starting with the first item in the event list and maintaining
        order.

        This method returns the number of items actually enqueued.  _It is very important that the caller check the
        return value in order to avoid dropping events._
        """
        ...

    async def remaining_capacity(self)->int:
        """
        Returns the remaining capacity in the queue (e.g. how many more events could be enqueued without loss).
        """
        ...


class LocalEventQueue(InputQueue[EVENT_TYPE], OutputQueue[EVENT_TYPE]):
    def __init__(self, capacity: int):
        self.events = deque(maxlen=capacity)
        self.next_event = defaultdict(lambda: -1)

    async def enqueue(self, events: List[Envelope[EVENT_TYPE]])->int:
        result = min(self.events.maxlen - len(self.events), len(events))
        self.events.extendleft(events[0:result])
        return result

    async def remaining_capacity(self)->int:
        return self.events.maxlen - len(self.events)

    async def get_events(self, subscriber: str, limit: int = 0)->List[EVENT_TYPE]:
        if limit == 0:
            limit = self.events.maxlen

        available_events = len(self.events) + self.next_event[subscriber] + 1
        result_size = min(limit, available_events)
        next_event = self.next_event[subscriber]
        result = [self.events[next_event - i] for i in range(result_size)]
        return result

    async def acknowledge_events(self, subscriber: str, n: int)->None:
        next_event = self.next_event[subscriber]
        new_next_event = max(-len(self.events)-1, next_event - n)
        self.next_event[subscriber] = new_next_event

        ceiling = -self.events.maxlen
        for n in self.next_event.values():
            ceiling = max(ceiling, n)

        num_to_pop = -ceiling - 1
        for _ in range(num_to_pop):
            self.events.pop()

        if num_to_pop > 0:
            for k,v in self.next_event.items():
                self.next_event[k] = v + num_to_pop


