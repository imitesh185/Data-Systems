"""Engine-agnostic pieces of the validate stage: the circuit breaker and the
order of operations both engines follow for one micro-batch.

    rules -> quarantine (idempotent append) -> gate -> silver MERGE -> commit

Quarantine is written before the gate so a rejected batch can be inspected.
Replaying the batch (same batch id) skips that append, so accepting the batch
later neither loses nor duplicates quarantined rows. A tripped gate leaves
silver untouched and the checkpoint uncommitted: the next run retries exactly
the same batch.
"""

from __future__ import annotations

from sparknerve.metadata import TableSpec


class QualityGateError(RuntimeError):
    audit_verbatim = True

    def __init__(self, table: str, rows: int, quarantined: int, threshold: float):
        self.table = table
        self.rows = rows
        self.quarantined = quarantined
        self.threshold = threshold
        ratio = quarantined / rows if rows else 0.0
        super().__init__(
            f"Quality gate tripped on '{table}': {quarantined}/{rows} rows invalid ({ratio:.0%}) > "
            f"threshold {threshold:.0%}. Silver untouched; the batch stays uncommitted and will be retried."
        )


def check_gate(table: TableSpec, rows_read: int, rows_quarantined: int, threshold: float | None = None) -> None:
    """Trip when the invalid share of a large-enough batch exceeds the threshold.
    `threshold` overrides the metadata for one run (an operator accepting a batch)."""
    limit = table.quarantine_threshold if threshold is None else threshold
    if rows_read >= max(table.gate_min_rows, 1) and rows_quarantined / rows_read > limit:
        raise QualityGateError(table.name, rows_read, rows_quarantined, limit)
