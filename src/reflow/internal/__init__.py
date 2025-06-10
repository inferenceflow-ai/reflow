from enum import Enum
from typing import Generic, List

from reflow.typedefs import EVENT_TYPE

class INSTRUCTION(Enum):
    PROCESS_EVENT = 1
    END_OF_STREAM = 2

class Envelope(Generic[EVENT_TYPE]):
    def __init__(self, instruction: INSTRUCTION, event: EVENT_TYPE = None):
        self.instruction = instruction
        self.event = event


def wrap(events: List[EVENT_TYPE])->List[Envelope[EVENT_TYPE]]:
    return [Envelope(INSTRUCTION.PROCESS_EVENT, event) for event in events]


def unwrap(envelopes: List[Envelope[EVENT_TYPE]])->List[EVENT_TYPE]:
    return [envelope.event for envelope in envelopes]
