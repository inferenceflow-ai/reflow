from dataclasses import dataclass
from enum import Enum
from typing import Generic, List

from reflow.typedefs import EVENT_TYPE


class INSTRUCTION(Enum):
    PROCESS_EVENT = 1


@dataclass(frozen=True)
class WorkerId:
    cluster_number: int
    worker_number: int


# wrap and unwrap are used for testing only

class Envelope(Generic[EVENT_TYPE]):
    def __init__(self, instruction: INSTRUCTION, source_id: WorkerId, sequence_num: int, event: EVENT_TYPE = None):
        self.instruction = instruction
        self.source_id = source_id
        self.sequence_num = sequence_num
        self.event = event


def wrap(events: List[EVENT_TYPE]) -> List[Envelope[EVENT_TYPE]]:
    return [
        Envelope(INSTRUCTION.PROCESS_EVENT, WorkerId(cluster_number=0, worker_number=0), sequence_num=0, event=event)
        for event in events]


def unwrap(envelopes: List[Envelope[EVENT_TYPE]]) -> List[EVENT_TYPE]:
    return [envelope.event for envelope in envelopes]
