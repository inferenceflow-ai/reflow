from typing import TypeVar, Callable, List, Any

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