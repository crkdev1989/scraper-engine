from __future__ import annotations

import csv
import json
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any


def write_csv(path: Path, rows: list[dict[str, Any]], preferred_columns: list[str] | None = None) -> None:
    columns = list(preferred_columns or [])
    for row in rows:
        for key in row.keys():
            if key not in columns:
                columns.append(key)

    with _open_atomic_writer(path, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            normalized = {
                key: flatten_csv_value(value)
                for key, value in row.items()
            }
            writer.writerow(normalized)


def write_json(path: Path, payload: Any) -> None:
    with _open_atomic_writer(path) as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def flatten_csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    if isinstance(value, Iterable):
        items = [
            flatten_csv_value(item)
            for item in value
            if item not in (None, "")
        ]
        return "; ".join(item for item in items if item)
    return str(value)


def _open_atomic_writer(path: Path, newline: str | None = None):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline=newline,
        delete=False,
        dir=path.parent,
        prefix=f"{path.name}.",
        suffix=".tmp",
    )
    temp_path = Path(temp_handle.name)

    class _AtomicWriter:
        def __enter__(self):
            return temp_handle

        def __exit__(self, exc_type, exc, tb):
            temp_handle.close()
            if exc_type is None:
                temp_path.replace(path)
                return False
            temp_path.unlink(missing_ok=True)
            return False

    return _AtomicWriter()
