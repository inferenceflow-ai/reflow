from typing import List

from reflow.internal import INSTRUCTION, Envelope, WorkerId
from reflow.internal.in_out_buffer import InOutBuffer

def event(x: str):
    return Envelope(INSTRUCTION.PROCESS_EVENT, WorkerId(-1, -1), 0,  x)

def events(l: List[str]):
    return [event(e) for e in l]

def test_basic():
    in_out_map = InOutBuffer()
    in_out_map.record_1_1(event("A"))
    in_out_map.record_filtered_event()
    in_out_map.record_split_event(events(["B","C","D"]))
    in_out_map.record_no_producer_event(event("E"))
    in_out_map.record_batch(2, events(["F", "G", "H"]))

    assert in_out_map.record_delivered_out_events(0, trim=False) == 0
    assert in_out_map.record_delivered_out_events(1, trim=False) == 2
    assert in_out_map.record_delivered_out_events(2, trim=False) == 2
    assert in_out_map.record_delivered_out_events(3, trim=False) == 2
    assert in_out_map.record_delivered_out_events(4, trim=False) == 3
    assert in_out_map.record_delivered_out_events(5, trim=False) == 3
    assert in_out_map.record_delivered_out_events(6, trim=False) == 3
    assert in_out_map.record_delivered_out_events(7, trim=False) == 3
    assert in_out_map.record_delivered_out_events(8, trim=False) == 5

    assert in_out_map.record_delivered_out_events(1) == 2
    assert in_out_map.record_delivered_out_events(5) == 1
    assert in_out_map.record_delivered_out_events(2) == 0
    assert in_out_map.record_delivered_out_events(3) == 2

def test_empty_then_full():
    in_out_map = InOutBuffer()
    for _  in range(3):
        in_out_map.record_1_1(event("A"))

    assert in_out_map.record_delivered_out_events(3) == 3

    for _  in range(3):
        in_out_map.record_1_1(event("A"))

    assert in_out_map.record_delivered_out_events(3) == 3
