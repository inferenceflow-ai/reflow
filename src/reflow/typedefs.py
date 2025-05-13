from typing import TypeVar, Callable, Awaitable, List

EVENT_TYPE = TypeVar('EVENT_TYPE')
IN_EVENT_TYPE = TypeVar('IN_EVENT_TYPE')
OUT_EVENT_TYPE = TypeVar('OUT_EVENT_TYPE')
STATE_TYPE = TypeVar('STATE_TYPE')

InitFn = Callable[[], Awaitable[STATE_TYPE]]
ProducerFn = Callable[[STATE_TYPE, int], Awaitable[List[EVENT_TYPE]]] | Callable[[int], Awaitable[List[EVENT_TYPE]]]
ConsumerFn = Callable[[List[EVENT_TYPE], STATE_TYPE], Awaitable[None]] | Callable[[List[EVENT_TYPE]], Awaitable[None]]