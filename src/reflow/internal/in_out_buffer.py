from typing import Any, List

from reflow.internal import Envelope


class InOutBuffer:
    def __init__(self):
        self.event_mappings = []
        self.unsent_out_events = []
        self.processed_in_events = 0
        self.produced_out_events = 0

    def clear(self):
        self.event_mappings = []
        self.unsent_out_events = []
        self.processed_in_events = 0
        self.produced_out_events = 0

    def record_1_1(self, envelope: Envelope[Any]):
        self.unsent_out_events.append(envelope)
        self._record_in_out(1, 1)

    def record_1_1s(self, envelopes: List[Envelope[Any]]):
        for envelope in envelopes:
            self.record_1_1(envelope)

    def record_filtered_event(self):
        self._record_in_out(1, 0)

    def record_split_event(self, envelopes: List[Envelope[Any]]):
        self.unsent_out_events.extend(envelopes)
        self._record_in_out(1, len(envelopes))

    def record_no_producer_event(self, envelope: Envelope[Any]):
        self.unsent_out_events.append(envelope)
        self._record_in_out(0, 1)

    def record_batch(self, in_event_count: int, envelopes: List[Envelope[Any]])->None:
        if in_event_count < 0:
            raise ValueError(f'in_event_count cannot be negative')

        self.unsent_out_events.extend(envelopes)
        self._record_in_out(in_event_count, len(envelopes))

    def _record_in_out(self, in_event_count: int, out_event_count: int)->None:
        self.processed_in_events +=  in_event_count
        self.produced_out_events += out_event_count
        self.event_mappings.append((self.processed_in_events, self.produced_out_events))

    def record_delivered_out_events(self, delivered_out_events: int, trim=True)->int:
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
            del self.unsent_out_events[0:delivered_out_events]
            del self.event_mappings[0:i+1]
            self.produced_out_events -= delivered_out_events
            self.processed_in_events -= in_count
            for n in range(len(self.event_mappings)):
                ii, oo = self.event_mappings[n]
                self.event_mappings[n] = (ii - in_count, oo - delivered_out_events)

        return in_count
