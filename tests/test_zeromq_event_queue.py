import asyncio
from typing import Any, Awaitable

import pytest

from reflow.internal import wrap, unwrap
from reflow.internal.zero_client import ZeroClientEventQueue
from reflow.internal.zero_server import ZeroServerEventQueue


async def run(server: ZeroServerEventQueue[Any], coro: Awaitable[Any])->Any:
    result = await asyncio.gather(
        coro,
        server.process_request()
    )
    return result[0]


@pytest.mark.asyncio
async def test_single_subscriber_basic():
    with ZeroServerEventQueue(6, "ipc://sockets/basic_test") as server:
        with ZeroClientEventQueue("ipc://sockets/basic_test") as client:
            await run(server, client.enqueue(wrap(["a","b","c"])))

            result = await run(server, client.get_events("frodo", 2))
            assert unwrap(result) == ['a', 'b']

            await run(server, client.enqueue(wrap(["d","e","f","g"])))
            remaining = await run(server, client.remaining_capacity())
            assert remaining == 0

            enqueued = await run(server, client.enqueue(wrap(["h","i"])))
            assert enqueued == 0

            result = await run(server, client.get_events("frodo", 1000))
            assert unwrap(result) == ['a', 'b', 'c', 'd', 'e', 'f']

            await run(server, client.acknowledge_events("frodo", 4))
            result = await run(server, client.get_events("frodo", 1000))
            assert unwrap(result) == ['e', 'f']

            remaining = await run(server, client.remaining_capacity())
            assert remaining == 4

