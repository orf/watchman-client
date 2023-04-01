# watchman-client

This is a Python client for the [watchman file watching service](https://facebook.github.io/watchman/).

## Quickstart

To monitor files you first need to call `watch_root` that contains the files you want to monitor. You then `subscribe` 
to updates to files, passing in [any kind of filter](https://facebook.github.io/watchman/docs/file-query).

After this you can call `files_changed()` to receive a stream of file change events.

```python
from watchman_client import Client
from pathlib import Path

async with Client() as client:
    directory = Path.cwd() 
    root = await client.watch_root(directory)
    await client.subscribe(root, "all-files", expression=[])
    
    async for root_path, subscription_name, items in client.files_changed():
        for file in items["files"]:
            print(f'Changed: {root_path / file["name"]}')
```

## Example CLI

Run `python -m watchman_client` to launch an example CLI that watches for changes in the current directory.