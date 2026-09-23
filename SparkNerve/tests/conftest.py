from datetime import datetime

import pytest

from demo.engine import DemoEngine
from simulator.retail import RetailDB, SqliteBackend
from sparknerve.metadata import load_pipelines

START = datetime(2026, 9, 20, 9, 0, 0)


@pytest.fixture(scope="session")
def pipeline():
    return load_pipelines()["retail_sales"]


@pytest.fixture
def source():
    db = RetailDB(SqliteBackend(), seed=11, start=START)
    db.seed()
    return db


@pytest.fixture
def engine(tmp_path, pipeline, source):
    return DemoEngine(tmp_path / "lake", pipeline, source)
