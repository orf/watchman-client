import asyncio
import time
from unittest.mock import ANY
import pytest

from watchman_client import WatchmanError, WatchmanServerDisconnected


@pytest.mark.asyncio
async def test_info(watchman_server, watchman_test_client):
    assert watchman_test_client.info.sockname == watchman_server


@pytest.mark.asyncio
async def test_capabilities(watchman_test_client):
    capabilities = await watchman_test_client.capabilities()
    assert capabilities.keys() == {"version", "capabilities"}


@pytest.mark.asyncio
async def test_errors(watchman_test_client, tmp_path):
    with pytest.raises(WatchmanError):
        await watchman_test_client.query("foobar")


@pytest.mark.asyncio
async def test_subscription(watchman_test_client, tmp_path):
    root = await watchman_test_client.watch_root(tmp_path)
    subscription = await watchman_test_client.subscribe(root, "foobar", expression=[])
    (tmp_path / "foo").write_text("foobar")
    await watchman_test_client.flush_subscriptions(subscription)
    assert watchman_test_client.active_roots == [tmp_path]
    sub_data = watchman_test_client.get_subscription_data(subscription)
    assert len(sub_data) == 2
    assert len(sub_data[1]["files"]) == 1
    assert sub_data[1]["files"][0]["name"] == "foo"

    (tmp_path / "foo").write_text("foobar2")
    await watchman_test_client.flush_subscriptions(subscription)
    sub_data = watchman_test_client.get_subscription_data(subscription)
    assert len(sub_data) == 1
    assert len(sub_data[0]["files"]) == 1
    assert sub_data[0]["files"][0]["name"] == "foo"


@pytest.mark.asyncio
async def test_nested_subscription(watchman_test_client, tmp_path):
    await watchman_test_client.watch_root(tmp_path)
    (tmp_path / "foo").mkdir()
    (tmp_path / "bar").mkdir()
    root_1 = await watchman_test_client.watch_root(tmp_path / "foo")
    root_2 = await watchman_test_client.watch_root(tmp_path / "bar")
    sub_1 = await watchman_test_client.subscribe(root_1, "foo", expression=[])
    sub_2 = await watchman_test_client.subscribe(root_2, "bar", expression=[])
    (tmp_path / "foo" / "file_1").write_text("foo")
    (tmp_path / "bar" / "file_2").write_text("bar")
    await watchman_test_client.flush_subscriptions(root_1)
    await watchman_test_client.flush_subscriptions(root_2)

    print(watchman_test_client._subscriptions)
    data = watchman_test_client.get_subscription_data(sub_1)
    assert data[1]["root"] == str((tmp_path / "foo"))
    assert data[1]["files"][0]["name"] == "file_1"

    data = watchman_test_client.get_subscription_data(sub_2)
    assert data[1]["root"] == str(tmp_path / "bar")
    assert data[1]["files"][0]["name"] == "file_2"


@pytest.mark.asyncio
async def test_log(watchman_test_client):
    await watchman_test_client.query("log-level", "error")
    await watchman_test_client.query("log", "error", "foo")
    await watchman_test_client.query("version")
    matching_logs = [
        message for message in watchman_test_client.logs if "foo" in message
    ]
    assert len(matching_logs) == 1


@pytest.mark.asyncio
async def test_files_changed(watchman_test_client, tmp_path):
    await watchman_test_client.query("watch-project", tmp_path)
    await watchman_test_client.query("subscribe", tmp_path, "foobar", {})
    (tmp_path / "foo").write_text("foobar")

    iterator = watchman_test_client.files_changed()
    (root, sub_name, changes) = await iterator.__anext__()
    assert root == tmp_path
    assert sub_name == "foobar"
    assert changes["files"] == [
        {"exists": True, "mode": ANY, "name": "foo", "new": True, "size": ANY}
    ]


@pytest.mark.asyncio
async def test_concurrent_access(watchman_test_client, tmp_path):
    # Multiple tasks using the client requires a lock, as the readers cannot be used by >1 task.
    # This test triggers the race condition nicely.

    root = await watchman_test_client.watch_root(tmp_path)
    subscription = await watchman_test_client.subscribe(root, "foobar", expression=[])
    (tmp_path / "foo").write_text("foobar")

    async def version_task():
        while True:
            assert await watchman_test_client.query("version", timeout=1) == {
                "version": watchman_test_client.info.version
            }
            await asyncio.sleep(0.1)

    running_task = asyncio.create_task(version_task())
    start_time = time.time()
    while time.time() < start_time + 5 and not running_task.done():
        await watchman_test_client.flush_subscriptions(subscription, timeout=1)
        await asyncio.sleep(0.1)

    if running_task.done():
        raise running_task.exception()

    running_task.cancel()
    try:
        await running_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_server_disconnect(watchman_test_client):
    await watchman_test_client.query("shutdown-server")
    with pytest.raises(WatchmanServerDisconnected):
        await watchman_test_client.query("version")
