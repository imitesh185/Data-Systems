"""Runtime settings and the lake layout, shared by the Spark jobs and the demo."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class LakeLayout:
    root: str

    def bronze(self, pipeline: str, table: str) -> str:
        return f"{self.root}/bronze/{pipeline}/{table}"

    def silver(self, pipeline: str, table: str) -> str:
        return f"{self.root}/silver/{pipeline}/{table}"

    def quarantine(self, pipeline: str, table: str) -> str:
        return f"{self.root}/quarantine/{pipeline}/{table}"

    def watermark(self, pipeline: str, table: str) -> str:
        return f"{self.root}/_control/{pipeline}/{table}/watermark"

    def checkpoint(self, pipeline: str, table: str, stage: str) -> str:
        return f"{self.root}/_checkpoints/{pipeline}/{table}/{stage}"

    @property
    def audit(self) -> str:
        return f"{self.root}/audit/stage_runs"


def app_id(pipeline: str, table: str, sink: str) -> str:
    """Delta txnAppId of an idempotent writer (the version is the batch/sequence)."""
    return f"sparknerve.{pipeline}.{table}.{sink}"


@dataclass(frozen=True)
class JdbcConnection:
    url: str
    user: str
    password: str

    @classmethod
    def from_env(cls, name: str) -> JdbcConnection:
        prefix = f"SPARKNERVE_CONN_{name.upper()}"
        url = os.getenv(f"{prefix}_URL")
        if not url:
            raise RuntimeError(
                f"Connection '{name}' is not configured: set {prefix}_URL, {prefix}_USER and {prefix}_PASSWORD")
        return cls(url=url, user=os.getenv(f"{prefix}_USER", ""), password=os.getenv(f"{prefix}_PASSWORD", ""))


@dataclass(frozen=True)
class Settings:
    lake: LakeLayout
    pushgateway: str | None

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            lake=LakeLayout(os.getenv("SPARKNERVE_LAKE", "./lake").rstrip("/")),
            pushgateway=os.getenv("SPARKNERVE_PUSHGATEWAY") or None,
        )
