from __future__ import annotations

import pytest

from wavelink.enums import NodeStatus
from wavelink.node import Node, Pool


@pytest.fixture(autouse=True)
def reset_pool_nodes() -> None:
    getattr(Pool, "_Pool__nodes").clear()


@pytest.mark.asyncio
async def test_stale_node_close_with_eject_does_not_remove_current_same_identifier_node() -> None:
    stale_node = Node(identifier="shared", uri="http://localhost:2333", password="not-used")
    current_node = Node(identifier="shared", uri="http://localhost:2334", password="not-used")

    stale_node._status = NodeStatus.DISCONNECTED
    current_node._status = NodeStatus.CONNECTED
    getattr(Pool, "_Pool__nodes")[current_node.identifier] = current_node

    try:
        await stale_node.close(eject=True)

        assert Pool.nodes.get("shared") is current_node
        assert Pool.get_node("shared") is current_node
        assert current_node.status is NodeStatus.CONNECTED
    finally:
        getattr(Pool, "_Pool__nodes").clear()
        await stale_node._session.close()
        await current_node._session.close()
