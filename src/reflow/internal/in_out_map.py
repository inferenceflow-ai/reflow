class InOutMap:
    def __init__(self):
        self.event_mappings = []
        self.processed_in_events = 0
        self.delivered_out_events = 0

    def clear(self):
        self.event_mappings = []
        self.processed_in_events = 0
        self.delivered_out_events = 0

    def record_1_1(self):
        self.record_batch(1, 1)

    def record_filtered_event(self):
        self.record_batch(1, 0)

    def record_split_event(self, output_count: int):
        self.record_batch(1, output_count)

    def record_no_producer_event(self):
        self.record_batch(0, 1)

    def record_batch(self, in_event_count: int, out_event_count: int):
        if in_event_count < 0:
            raise ValueError(f'in_event_count cannot be negative')

        if out_event_count < 0:
            raise ValueError(f'out_event_count cannot be negative')

        self.processed_in_events +=  in_event_count
        self.delivered_out_events += out_event_count
        self.event_mappings.append((self.processed_in_events, self.delivered_out_events))

    def acknowledgeable_in_events(self, delivered_out_events: int)->int:
        for in_count, out_count in reversed(self.event_mappings):
            if delivered_out_events >= out_count:
                return in_count

        return 0
