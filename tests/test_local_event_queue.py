import pytest

from reflow.internal import wrap, unwrap
from reflow.internal.event_queue import DequeueEventQueue


@pytest.mark.asyncio
async def test_single_subscriber_basic():
    q = DequeueEventQueue(6)
    await q.enqueue(wrap(["a","b","c"]))

    result = await q.get_events("frodo", 2)
    assert unwrap(result) == ['a', 'b']

    await q.enqueue(wrap(["d","e","f","g"]))
    remaining = await q.remaining_capacity()
    assert remaining == 0

    enqueued = await q.enqueue(wrap(["h","i"]))
    assert enqueued == 0

    result = await q.get_events("frodo", 1000)
    assert unwrap(result) == ['a', 'b', 'c', 'd', 'e', 'f']

    await q.acknowledge_events("frodo", 4)
    result = await q.get_events("frodo", 1000)
    assert unwrap(result) == ['e', 'f']

    remaining = await q.remaining_capacity()
    assert remaining == 4

