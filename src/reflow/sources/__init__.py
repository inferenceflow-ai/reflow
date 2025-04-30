from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List

class EndOfStream(StopIteration):
    """
    This exception will be thrown when the stream contains no more data.  This is distinct from
    the state of no data available right now.
    """
    pass

SOURCE_TYPE = TypeVar('SOURCE_TYPE')

class Source(ABC, Generic[SOURCE_TYPE]):

    @abstractmethod
    def poll(self, max_items: int):
        pass


class TestSource(Source[SOURCE_TYPE]):
    def __init__(self, test_items: List[SOURCE_TYPE]):
        self.next_item = 0
        self.test_items = test_items

    def poll(self, max_items: int):
        if self.next_item >= len(self.test_items):
            raise EndOfStream()

        limit = min(self.next_item + max_items, len(self.test_items))
        result = self.test_items[self.next_item: limit]
        self.next_item = limit
        return result
