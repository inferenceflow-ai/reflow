import pytest

from reflow.internal import wrap, unwrap
from reflow.internal.event_queue_client import EventQueueClient
from reflow.internal.event_queue_server import EventQueueServer


@pytest.mark.asyncio
async def test_single_subscriber_basic():
    with EventQueueServer(6, ["ipc://sockets/basic_test"]):
        with EventQueueClient("ipc://sockets/basic_test") as client:
            await client.enqueue(wrap(["a","b","c"]))

            result = await client.get_events("frodo", 2)
            assert unwrap(result) == ['a', 'b']

            await client.enqueue(wrap(["d","e","f","g"]))
            remaining = await client.remaining_capacity()
            assert remaining == 0

            enqueued = await client.enqueue(wrap(["h","i"]))
            assert enqueued == 0

            result = await  client.get_events("frodo", 1000)
            assert unwrap(result) == ['a', 'b', 'c', 'd', 'e', 'f']

            await client.acknowledge_events("frodo", 4)
            result = await client.get_events("frodo", 1000)
            assert unwrap(result) == ['e', 'f']

            remaining = await client.remaining_capacity()
            assert remaining == 4

