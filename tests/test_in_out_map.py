from internal.in_out_map import InOutMap


def test_basic():
    in_out_map = InOutMap()
    in_out_map.record_1_1()
    in_out_map.record_filtered_event()
    in_out_map.record_split_event(3)
    in_out_map.record_no_producer_event()
    in_out_map.record_batch(2,3)

    assert in_out_map.acknowledgeable_in_events(0) == 0
    assert in_out_map.acknowledgeable_in_events(1) == 2
    assert in_out_map.acknowledgeable_in_events(2) == 2
    assert in_out_map.acknowledgeable_in_events(3) == 2
    assert in_out_map.acknowledgeable_in_events(4) == 3
    assert in_out_map.acknowledgeable_in_events(5) == 3
    assert in_out_map.acknowledgeable_in_events(6) == 3
    assert in_out_map.acknowledgeable_in_events(7) == 3
    assert in_out_map.acknowledgeable_in_events(8) == 5
