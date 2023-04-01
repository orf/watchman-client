import asyncio
import watchman_client
from pathlib import Path


async def main():
    cwd = Path.cwd()
    async with watchman_client.Client() as client:
        await client.query("watch-project", cwd)
        root_clock = await client.root_clock(cwd)
        await client.query("subscribe", cwd, "all-files", {
            "expression": [
                "allof",
                ["anyof", ["type", "f"], ["type", "l"]]
            ],
            "since": root_clock,
            "dedup_results": True
        })
        async for (root, name, item) in client.files_changed():
            files = item.pop("files")
            print(f"{root=} {name=} {item=}")
            print("Changed files:")
            for file in files:
                print(f' - {file}')

asyncio.run(main())
