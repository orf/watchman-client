import asyncio
import time
from unittest.mock import ANY
import pytest


@pytest.mark.asyncio
async def test_info(watchman_server, watchman_test_client):
    assert watchman_test_client.info.sockname == watchman_server


@pytest.mark.asyncio
async def test_capabilities(watchman_test_client):
    capabilities = await watchman_test_client.capabilities()
    assert capabilities.keys() == {"version", "capabilities"}


@pytest.mark.asyncio
async def test_subscription(watchman_test_client, tmp_path):
    await watchman_test_client.query("watch-project", tmp_path)
    await watchman_test_client.query("subscribe", tmp_path, "foobar", {})
    (tmp_path / "foo").write_text("foobar")
    await watchman_test_client.pump(tmp_path)
    assert watchman_test_client.active_roots == [tmp_path]
    sub_data = watchman_test_client.get_subscription_data(tmp_path, "foobar")
    assert len(sub_data) == 2
    assert len(sub_data[1]["files"]) == 1
    assert sub_data[1]["files"][0]["name"] == "foo"

    (tmp_path / "foo").write_text("foobar2")
    await watchman_test_client.pump(tmp_path)
    sub_data = watchman_test_client.get_subscription_data(tmp_path, "foobar")
    assert len(sub_data) == 1
    assert len(sub_data[0]["files"]) == 1
    assert sub_data[0]["files"][0]["name"] == "foo"


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
    result = await iterator.__anext__()
    assert result == (tmp_path, "foobar", [
        {"exists": True, "mode": ANY, "name": "foo", "new": True, "size": ANY}
    ])


@pytest.mark.asyncio
async def test_concurrent_access(watchman_test_client, tmp_path):
    # Multiple tasks using the client requires a lock, as the readers cannot be used by >1 task.
    # This test triggers the race condition nicely.
    await watchman_test_client.query("watch-project", tmp_path)
    await watchman_test_client.query("subscribe", tmp_path, "foobar", {})
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
        await watchman_test_client.pump_all(timeout=1)
        await asyncio.sleep(0.1)

    if running_task.done():
        raise running_task.exception()

    running_task.cancel()
    try:
        await running_task
    except asyncio.CancelledError:
        pass
