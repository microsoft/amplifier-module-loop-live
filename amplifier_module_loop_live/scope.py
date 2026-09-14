"""Task-local ownership shared by optional hosts and native provider extensions."""

from contextvars import ContextVar

HOST_ADAPTER = ContextVar("amplifier_live_host", default=None)
LIVE_OWNER = ContextVar("amplifier_live_turn", default=None)
NATIVE_REQUEST = ContextVar("amplifier_live_native_request", default=None)
JOB_CALL = ContextVar("amplifier_live_job_call", default=None)
