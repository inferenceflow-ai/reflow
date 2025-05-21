import pytest

from reflow.internal.event_queue import LocalEventQueue


@pytest.mark.asyncio
async def test_single_subscriber_basic():
    q = LocalEventQueue(6)
    await q.enqueue(["a","b","c"])

    result = await q.get_events("frodo", 2)
    assert result == ['a', 'b']

    await q.enqueue(["d","e","f","g"])
    remaining = await q.remaining_capacity()
    assert remaining == 0

    enqueued = await q.enqueue(["h","i"])
    assert enqueued == 0

    result = await q.get_events("frodo", 1000)
    assert result == ['a', 'b', 'c', 'd', 'e', 'f']

    await q.acknowledge_events("frodo", 4)
    result = await q.get_events("frodo", 1000)
    assert result == ['e', 'f']

    remaining = await q.remaining_capacity()
    assert remaining == 4

