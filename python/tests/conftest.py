"""Shared pytest fixtures.

Strips all TURBOFAN_*/AWS_BATCH_* env vars before each test so prior-
test state can't leak. Tests set the vars they need explicitly.
"""

import pytest

TURBOFAN_VARS = (
    "TURBOFAN_EXECUTION_ID",
    "TURBOFAN_STEP_NAME",
    "TURBOFAN_STAGE",
    "TURBOFAN_PIPELINE",
    "TURBOFAN_BUCKET",
    "TURBOFAN_BUCKET_PREFIX",
    "TURBOFAN_INPUT",
    "TURBOFAN_SIZE",
    "TURBOFAN_PARENT_INDEX",
    "TURBOFAN_PREV_STEP",
    "TURBOFAN_PREV_STEPS",
    "TURBOFAN_SCHEMAS_PATH",
    "TURBOFAN_STORAGE_PATH",
    "TURBOFAN_NVME_MOUNT_PATH",
    "TURBOFAN_ALLOCATED_RAM_MB",
    "TURBOFAN_MAX_RETRY_SECONDS",
    "AWS_BATCH_JOB_ARRAY_INDEX",
    "AWS_BATCH_JOB_ID",
    "AWS_BATCH_JOB_ATTEMPT",
    "ECS_CONTAINER_METADATA_URI_V4",
)


@pytest.fixture(autouse=True)
def isolate_turbofan_env(monkeypatch):
    for name in TURBOFAN_VARS:
        monkeypatch.delenv(name, raising=False)
    # boto3 needs SOME credentials present even when talking to moto.
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-2")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
