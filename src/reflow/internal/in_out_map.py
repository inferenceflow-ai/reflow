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

    def record_batch(self, in_event_count: int, out_event_count: int)->None:
        if in_event_count < 0:
            raise ValueError(f'in_event_count cannot be negative')

        if out_event_count < 0:
            raise ValueError(f'out_event_count cannot be negative')

        self.processed_in_events +=  in_event_count
        self.delivered_out_events += out_event_count
        self.event_mappings.append((self.processed_in_events, self.delivered_out_events))

    def acknowledgeable_in_events(self, delivered_out_events: int, trim=True)->int:
        found = False
        i, in_count, out_count = 0,0,0
        for i in range(len(self.event_mappings) - 1, -1, -1):
            in_count, out_count = self.event_mappings[i]
            if delivered_out_events >= out_count:
                found = True
                break

        if not found:
            return 0

        # i now points to the entry corresponding to the highest input event that can be acknowledged
        # in_count and out_count point to the contents of that entry
        if trim:
            del self.event_mappings[0:i+1]
            self.delivered_out_events -= out_count
            self.processed_in_events -= in_count
            for n in range(len(self.event_mappings)):
                ii, oo = self.event_mappings[n]
                self.event_mappings[n] = (ii - in_count, oo - out_count)

        return in_count
