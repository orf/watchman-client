import collections
from functools import lru_cache
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
import asyncio
from typing import Optional, Tuple, List, TypeAlias, Union, Dict, Any
from os import PathLike

JSON: TypeAlias = Union[
    dict[str, "JSON"], list["JSON"], str, int, float, bool, None, PathLike
]
RELATIVE_ROOT_PREFIX = "!relative-root:"


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


class WatchmanServerDisconnected(WatchmanError):
    pass


class JsonEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Path):
            return str(o)
        return super().default(o)


@dataclass
class WatchedRoot:
    root: Path
    relative_path: Optional[Path]

    @property
    def full_path(self):
        if self.relative_path is None:
            return self.root
        return self.root / self.relative_path


@dataclass
class Subscription:
    root: WatchedRoot
    user_supplied_name: str
    name: str


class Client:
    _connection: Optional[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]
    _subscriptions: Dict[Path, Dict[str, List[JSON]]]
    _unix_socket: Optional[PathLike]
    _receive_task: Optional[asyncio.Task]
    _files_changed: asyncio.Event

    _sync_message_lock: asyncio.Lock
    _sync_message_condition: asyncio.Condition
    _last_sync_message: Optional[JSON]

    logs: collections.deque
    info: Optional[WatchmanInfo]

    disconnected: bool

    def __init__(self, unix_socket: Optional[PathLike] = None):
        self._connection = None
        self._subscriptions = collections.defaultdict(
            lambda: collections.defaultdict(list)
        )
        self._unix_socket = unix_socket
        self._receive_task = None
        self._files_changed = asyncio.Event()

        self._sync_message_lock = asyncio.Lock()
        self._sync_message_condition = asyncio.Condition()
        self._last_sync_message = None

        self.logs = collections.deque(maxlen=25)
        self.info = None

        self.disconnected = False

    async def __aenter__(self) -> "Client":
        await self.get_connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._receive_task.cancel()
        try:
            await self._receive_task
        except asyncio.CancelledError:
            pass

    async def get_connection(self) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if self._connection is None:
            self.info = await get_watchman_info(self._unix_socket)
            self._connection = await asyncio.open_unix_connection(self.info.sockname)
            self._receive_task = asyncio.create_task(self.receive_task())
        return self._connection

    def _handle_unilateral_response(self, response: Dict[str, JSON]) -> bool:
        if "subscription" in response:
            subscription_name = response["subscription"]
            root = Path(response["root"])
            if subscription_name.startswith(RELATIVE_ROOT_PREFIX):
                decoded = json.loads(subscription_name[len(RELATIVE_ROOT_PREFIX) + 1 :])
                subscription_name = decoded["name"]
                root = root / decoded["relative"]
                response["root"] = str(root)
                response["subscription"] = subscription_name
            self._subscriptions[root][subscription_name].append(response)
            return True
        elif "log" in response:
            self.logs.append(response["log"])
        return False

    async def receive_task(self, timeout: Optional[float] = None):
        reader, _ = await self.get_connection()
        while True:
            response_bytes = await reader.readline()
            if response_bytes == b"" or not response_bytes.endswith(b"\n"):
                # Server disconnected!
                response = WatchmanServerDisconnected("Server disconnected")
                self.disconnected = True
            else:
                unilateral, response = parse_response(response_bytes)
                if unilateral:
                    if self._handle_unilateral_response(response):
                        self._files_changed.set()
                    continue
                if "error" in response:
                    response = WatchmanError(response["error"])
            async with self._sync_message_condition:
                self._last_sync_message = response
                self._sync_message_condition.notify()

            # Break the loop if the server has disconnected
            if isinstance(response, WatchmanServerDisconnected):
                # Wake up all watchers, who should then check for disconnection
                self._files_changed.set()
                return

    async def receive_sync_response(self) -> JSON:
        if not self._sync_message_lock.locked():
            raise RuntimeError("Sync message lock is not acquired")
        async with self._sync_message_condition:
            await self._sync_message_condition.wait()
            if isinstance(self._last_sync_message, Exception):
                raise self._last_sync_message
            return self._last_sync_message

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

    def get_subscription_data(
        self, subscription: Subscription, *, remove=True
    ) -> List[JSON]:
        root_path = subscription.root.full_path
        return self._get_sub_data(root_path, subscription.user_supplied_name)

    def _get_sub_data(self, path: Path, name: str, *, remove=True) -> List[JSON]:
        if path not in self._subscriptions:
            return []
        if name not in self._subscriptions[path]:
            return []

        if remove:
            return self._subscriptions[path].pop(name)
        return self._subscriptions[path][name]

    async def send_command(
        self, command: List[JSON], *, timeout: Optional[float] = None
    ) -> Dict[str, JSON]:
        _, writer = await self.get_connection()
        async with self._sync_message_lock:
            writer.write(f"{json.dumps(command, cls=JsonEncoder)}\n".encode())
            try:
                await writer.drain()
            except ConnectionError as e:
                raise WatchmanServerDisconnected() from e
            return await self.receive_sync_response()

    async def query(
        self, command: str, *args: JSON, timeout: Optional[float] = None
    ) -> Dict[str, JSON]:
        return await self.send_command([command, *args], timeout=timeout)

    async def _flush(
        self,
        root: PathLike,
        *,
        subscriptions: Optional[List[str]] = None,
        sync_timeout: int = 100,
        timeout: Optional[float] = None,
    ):
        args = {"sync_timeout": sync_timeout}
        if subscriptions:
            args["subscriptions"] = subscriptions
        await self.query(
            "flush-subscriptions",
            str(root),
            args,
            timeout=timeout,
        )

    async def flush_all_subscriptions(
        self, *, sync_timeout: int = 100, timeout: Optional[float] = None
    ):
        for root in self._subscriptions:
            await self._flush(root, sync_timeout=sync_timeout, timeout=timeout)

    async def flush_subscriptions(
        self, root: Union[WatchedRoot, Subscription], timeout: Optional[float] = None
    ):
        if isinstance(root, WatchedRoot):
            await self._flush(root.root, timeout=timeout)
        else:
            await self._flush(
                root.root.root, subscriptions=[root.name], timeout=timeout
            )

    async def watch_root(self, root: PathLike) -> WatchedRoot:
        response = await self.query("watch-project", root)
        root = Path(response["watch"])
        relative_path = (
            Path(response["relative_path"]) if "relative_path" in response else None
        )
        return WatchedRoot(root=root, relative_path=relative_path)

    async def subscribe(
        self,
        root: WatchedRoot,
        name: str,
        *,
        expression: Optional[List] = None,
        fields: Optional[List[str]] = None,
        since: Optional[str] = None,
        **kwargs,
    ) -> Subscription:
        arguments = {"dedup_results": True}
        if expression:
            arguments["expression"] = expression
        if fields:
            arguments["fields"] = fields
        if since:
            arguments["since"] = since
        if root.relative_path:
            arguments["relative_root"] = root.relative_path
            encoded_name = json.dumps(
                {"relative": root.relative_path, "name": name}, cls=JsonEncoder
            )
            sub_name = f"{RELATIVE_ROOT_PREFIX}:{encoded_name}"
        else:
            sub_name = name

        # Allow overriding by passing in kwargs
        arguments.update(kwargs)
        await self.query("subscribe", root.root, sub_name, arguments)
        return Subscription(root=root, user_supplied_name=name, name=sub_name)

    async def root_clock(self, root: WatchedRoot):
        response = await self.query("clock", root.root)
        return response["clock"]

    async def capabilities(self) -> Dict[str, JSON]:
        return await self.query("list-capabilities")

    async def files_changed(self):
        while True:
            await self._files_changed.wait()

            if self.disconnected:
                raise WatchmanServerDisconnected()

            for (root, sub_name) in self.active_subscriptions:
                sub_data = self._get_sub_data(root, sub_name)
                for item in sub_data:
                    if item["files"]:
                        yield root, sub_name, item

            self._subscriptions.clear()
            self._files_changed.clear()
