from typing import TypeVar, Callable, Awaitable, List

EVENT_TYPE = TypeVar('EVENT_TYPE')
IN_EVENT_TYPE = TypeVar('IN_EVENT_TYPE')
OUT_EVENT_TYPE = TypeVar('OUT_EVENT_TYPE')
STATE_TYPE = TypeVar('STATE_TYPE')

InitFn = Callable[[], Awaitable[STATE_TYPE]]
ProducerFn = Callable[[STATE_TYPE, int], Awaitable[List[EVENT_TYPE]]] | Callable[[int], Awaitable[List[EVENT_TYPE]]]
ConsumerFn = Callable[[STATE_TYPE, List[EVENT_TYPE]], Awaitable[int]] | Callable[[List[EVENT_TYPE]], Awaitable[int]]
SplitFn = Callable[[STATE_TYPE, IN_EVENT_TYPE], List[OUT_EVENT_TYPE]] | Callable[[IN_EVENT_TYPE], List[OUT_EVENT_TYPE]]

# Use this in a producer function to signify there are no items left
class EndOfStreamException(Exception):
    pass