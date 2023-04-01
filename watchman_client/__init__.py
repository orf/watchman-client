import collections
import subprocess
from functools import lru_cache
import shutil
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
import asyncio
from typing import Optional, Tuple, List, TypeAlias, Union, Dict, Set
from os import PathLike

JSON: TypeAlias = Union[dict[str, "JSON"], list["JSON"], str, int, float, bool, None]


@dataclass()
class WatchmanInfo:
    executable_path: Path
    sockname: Path
    version: str


@lru_cache()
def executable_path() -> Path:
    return Path(shutil.which("watchman"))


async def get_watchman_info(unix_socket: Optional[PathLike] = None) -> WatchmanInfo:
    executable = executable_path()
    args = ["get-sockname"]
    if unix_socket:
        args += ["-u", str(unix_socket)]
    proc = await asyncio.create_subprocess_exec(
        executable,
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode:
        raise RuntimeError(
            f"Watchman CLI failed with exit code {proc.returncode}: {stderr}"
        )
    loaded = json.loads(stdout)
    return WatchmanInfo(
        executable_path=executable,
        sockname=Path(loaded["sockname"]),
        version=loaded["version"],
    )


def parse_response(response: bytes) -> (bool, dict):
    response = json.loads(response)
    if "unilateral" in response:
        unilateral = response["unilateral"]
    else:
        # Fall back to checking objects
        # https://github.com/facebook/watchman/blob/47ad309b172e90540168ab0bd254f4f4966e99ee/watchman/python/pywatchman/__init__.py#L1075
        unilateral = any(key in response for key in ("log", "subscription"))
    return unilateral, response


class WatchmanError(Exception):
    pass


class Client:
    _connection: Optional[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]
    _subscriptions: Dict[Path, Dict[str, List[JSON]]]
    _unix_socket: Optional[PathLike]
    _lock: asyncio.Lock
    logs: collections.deque
    info = Optional[WatchmanInfo]

    def __init__(self, unix_socket: Optional[PathLike] = None):
        self._connection = None
        self._subscriptions = collections.defaultdict(
            lambda: collections.defaultdict(list)
        )
        self._unix_socket = unix_socket
        self._lock = asyncio.Lock()
        self.logs = collections.deque(maxlen=25)
        self.info = None

    async def __aenter__(self):
        await self.get_connection()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def active_roots(self) -> List[Path]:
        return list(self._subscriptions.keys())

    @property
    def active_subscriptions(self) -> List[Tuple[Path, str]]:
        return [
            (root, name)
            for root in self.active_roots
            for name in self._subscriptions[root].keys()
        ]

    async def get_connection(self) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if self._connection is None:
            self.info = await get_watchman_info(self._unix_socket)
            self._connection = await asyncio.open_unix_connection(self.info.sockname)
        return self._connection

    def handle_unilateral_response(self, response: Dict[str, JSON]):
        if "subscription" in response:
            subscription_name = response["subscription"]
            root = Path(response["root"])
            self._subscriptions[root][subscription_name].append(response)
            # self._subscriptions_by_root[root][subscription_name] = sub_list
            # sub_list.append(response)
        elif "log" in response:
            self.logs.append(response["log"])

    def get_subscription_data(
        self, root: PathLike, name: str, *, remove=True
    ) -> List[JSON]:
        root = Path(root)
        if root not in self._subscriptions:
            return []
        if name not in self._subscriptions[root]:
            return []

        if remove:
            return self._subscriptions[root].pop(name)
        return self._subscriptions[root][name]

    async def receive_response(
        self, timeout: Optional[float] = None
    ) -> Dict[str, JSON]:
        async with self._lock:
            reader, _ = await self.get_connection()
            while True:
                async with asyncio.timeout(timeout):
                    response_bytes = await reader.readline()
                unilateral, response = parse_response(response_bytes)
                if unilateral:
                    self.handle_unilateral_response(response)
                    continue
                if "error" in response:
                    raise WatchmanError(response["error"])
                return response

    async def send_command(
        self, command: List[JSON], *, timeout: Optional[float] = None
    ) -> Dict[str, JSON]:
        _, writer = await self.get_connection()
        writer.write(f"{json.dumps(command)}\n".encode())
        await writer.drain()
        return await self.receive_response(timeout)

    async def capabilities(self) -> Dict[str, JSON]:
        return await self.query("list-capabilities")

    async def query(
        self, command: str, *args: JSON, timeout: Optional[float] = None
    ) -> Dict[str, JSON]:
        return await self.send_command([command, *args], timeout=timeout)

    async def pump(
        self,
        root: PathLike,
        *,
        sync_timeout: int = 100,
        timeout: Optional[float] = None,
    ):
        await self.query(
            "flush-subscriptions",
            str(root),
            {"sync_timeout": sync_timeout},
            timeout=timeout,
        )

    async def pump_all(
        self, *, sync_timeout: int = 100, timeout: Optional[float] = None
    ):
        for root in self._subscriptions:
            await self.pump(root, sync_timeout=sync_timeout, timeout=timeout)
