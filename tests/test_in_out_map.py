from reflow.internal.in_out_map import InOutMap


def test_basic():
    in_out_map = InOutMap()
    in_out_map.record_1_1()
    in_out_map.record_filtered_event()
    in_out_map.record_split_event(3)
    in_out_map.record_no_producer_event()
    in_out_map.record_batch(2,3)

    assert in_out_map.acknowledgeable_in_events(0, trim=False) == 0
    assert in_out_map.acknowledgeable_in_events(1, trim=False) == 2
    assert in_out_map.acknowledgeable_in_events(2, trim=False) == 2
    assert in_out_map.acknowledgeable_in_events(3, trim=False) == 2
    assert in_out_map.acknowledgeable_in_events(4, trim=False) == 3
    assert in_out_map.acknowledgeable_in_events(5, trim=False) == 3
    assert in_out_map.acknowledgeable_in_events(6, trim=False) == 3
    assert in_out_map.acknowledgeable_in_events(7, trim=False) == 3
    assert in_out_map.acknowledgeable_in_events(8, trim=False) == 5

    assert in_out_map.acknowledgeable_in_events(1) == 2
    assert in_out_map.acknowledgeable_in_events(5) == 1
    assert in_out_map.acknowledgeable_in_events(2) == 0
    assert in_out_map.acknowledgeable_in_events(3) == 2

def test_empty_then_full():
    in_out_map = InOutMap()
    for _  in range(3):
        in_out_map.record_1_1()

    assert in_out_map.acknowledgeable_in_events(3) == 3

    for _  in range(3):
        in_out_map.record_1_1()

    assert in_out_map.acknowledgeable_in_events(3) == 3
