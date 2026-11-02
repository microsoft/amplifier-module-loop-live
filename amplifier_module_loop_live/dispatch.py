"""Live compatibility adapters over the shared streaming execution authority."""

from contextlib import nullcontext

from amplifier_module_loop_streaming.dispatch import (
    FORBIDDEN,
    ApprovedDispatch as SharedApprovedDispatch,
    AttributedCoordinator,
    AttributedHooks,
    CallScope,
    DispatchLease,
    GuardedTool as SharedGuardedTool,
    Run,
)


class GuardedTool(SharedGuardedTool):
    """Keep Live's tool-declared independent reads without copying execution checks."""

    async def execute(self, arguments):
        concurrent = getattr(self.tool, "allows_concurrent_execution", None)
        if callable(concurrent) and concurrent(arguments) is True:
            # Only serialization changes. The canonical guard still rechecks
            # authority and records execution/result observations for this call.
            guard = SharedGuardedTool(
                self.tool, nullcontext(), self.observation, self.authority
            )
            return await guard.execute(arguments)
        return await super().execute(arguments)


class ApprovedDispatch(SharedApprovedDispatch):
    """Retain Live's read-concurrency policy while inheriting leases and recovery."""

    def wrap_tools(self, tools, observed_name=None, observation=None, authority=None):
        wrapped = super().wrap_tools(tools, observed_name, observation, authority)
        return {
            name: GuardedTool(tool.tool, tool.lock, tool.observation, tool.authority)
            if isinstance(tool, SharedGuardedTool) else tool
            for name, tool in wrapped.items()
        }


__all__ = [
    "FORBIDDEN",
    "ApprovedDispatch",
    "AttributedCoordinator",
    "AttributedHooks",
    "CallScope",
    "DispatchLease",
    "GuardedTool",
    "Run",
]
