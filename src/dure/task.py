from __future__ import annotations

import enum


class TaskType(str, enum.Enum):
    PROBE = "PROBE"
    VERIFY = "VERIFY"
    APPLY_DEPLOYMENT = "APPLY_DEPLOYMENT"
    START_DEPLOYMENT = "START_DEPLOYMENT"
    STOP_DEPLOYMENT = "STOP_DEPLOYMENT"
    RESTART_DEPLOYMENT = "RESTART_DEPLOYMENT"


class TaskStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
