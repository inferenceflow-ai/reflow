from enum import Enum
from typing import Generic

from reflow.typedefs import EVENT_TYPE

class INSTRUCTION(Enum):
    PROCESS_EVENT = 1
    END_OF_STREAM = 2

class Envelope(Generic[EVENT_TYPE]):
    def __init__(self, instruction: INSTRUCTION, event: EVENT_TYPE = None):
        self.instruction = instruction
        self.event = event


