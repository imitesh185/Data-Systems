from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping


class DeltaLakeSink:
    """Materializes demo state as real Delta tables through delta-rs."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def replace_table(
        self,
        table: str,
        rows: Iterable[Mapping[str, Any]],
        partition_by: list[str] | None = None,
    ) -> None:
        import pyarrow as pa
        from deltalake import write_deltalake

        records = [dict(row) for row in rows]
        if not records:
            return

        write_deltalake(
            self.root / table,
            pa.Table.from_pylist(records),
            mode="overwrite",
            partition_by=partition_by or [],
            schema_mode="overwrite",
        )

    def history(self, table: str) -> list[dict[str, Any]]:
        from deltalake import DeltaTable

        path = self.root / table
        if not path.exists():
            return []
        return DeltaTable(path).history()

    def version(self, table: str) -> int | None:
        from deltalake import DeltaTable

        path = self.root / table
        if not path.exists():
            return None
        return DeltaTable(path).version()

