"""DeltaSync: CDC replication from MySQL to Delta Lake with exactly-once results."""

from deltasync.engine import DemoEngine
from deltasync.models import CdcEvent, Operation

__all__ = ["CdcEvent", "DemoEngine", "Operation"]
__version__ = "1.0.0"

