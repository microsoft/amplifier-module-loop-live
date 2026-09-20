"""Active-call dispatch leases for portable programmatic orchestration.

The ordinary streaming engine remains the execution and hook authority. This
adapter adds scope, nested attribution, and structured outcomes, never a second
unchecked tool execution path.
"""

import asyncio
import json
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from uuid import uuid4

from amplifier_core.message_models import ToolCall
from amplifier_module_loop_streaming import StreamingOrchestrator

_RUN = ContextVar("approved_dispatch_run", default=None)
_CALL = ContextVar("approved_dispatch_call", default=None)
FORBIDDEN = frozenset({"tool_exec", "delegate", "task", "live_job"})


@dataclass
class Run:
    owner: object
    coordinator: object
    identity: str
    active: bool = True


@dataclass
class CallScope:
    run: Run
    call: object
    tools: dict
    hooks: object
    parallel_group_id: str | None
    active: bool = True


class GuardedTool:
    """Serialize actual execution, after the engine has processed approvals."""

    def __init__(self, tool, lock, observation=None, authority=None):
        self.tool, self.lock, self.observation = tool, lock, observation
        self.authority = authority

    def __getattr__(self, name):
        return getattr(self.tool, name)

    async def execute(self, arguments):
        async with self.lock:
            if self.authority:
                self.authority()
            if self.observation is not None:
                self.observation["started"] = True
            result = await self.tool.execute(arguments)
            if self.observation is not None:
                self.observation["raw_success"] = getattr(result, "success", None)
            return result


class AttributedHooks:
    def __init__(self, hooks, source, observation):
        self.hooks, self.source, self.observation = hooks, source, observation

    async def emit(self, event, data):
        enriched = {
            **data,
            "parent_tool_call_id": self.source["parent_tool_call_id"],
            "dispatch_source": dict(self.source),
        }
        result = await self.hooks.emit(event, enriched)
        if event == "tool:post":
            self.observation["post_seen"] = True
            returned = result.data.get("result") if result and result.data else None
            self.observation["post_result"] = (
                returned if returned is not None else data.get("result")
            )
        elif event == "tool:error":
            self.observation["engine_error"] = True
        return result

    def __getattr__(self, name):
        return getattr(self.hooks, name)


class AttributedCoordinator:
    def __init__(self, coordinator, observation):
        object.__setattr__(self, "coordinator", coordinator)
        object.__setattr__(self, "observation", observation)

    def __getattr__(self, name):
        return getattr(self.coordinator, name)

    def __setattr__(self, name, value):
        setattr(self.coordinator, name, value)

    async def process_hook_result(self, result, event, name):
        result = await self.coordinator.process_hook_result(result, event, name)
        if result.action == "deny":
            self.observation["denied"] = True
        return result


class DispatchLease:
    def __init__(self, dispatcher, scope):
        self.dispatcher, self.scope = dispatcher, scope
        self.closed = False
        allowlist = dispatcher.owner.config.get("programmatic_allowed_tools")
        self.tools = tuple(
            name
            for name in scope.tools
            if name not in FORBIDDEN
            and getattr(scope.tools[name], "name", name) not in FORBIDDEN
            and (allowlist is None or name in allowlist)
        )
        self.context = {
            "session_id": str(scope.run.coordinator.session_id),
            "run_id": scope.run.identity,
            "parent_tool_call_id": scope.call.id,
        }

    def close(self):
        self.closed = True

    def _valid(self, expected_scope=None):
        run = _RUN.get()
        scope = _CALL.get()
        if (
            self.closed
            or not self.dispatcher.enabled
            or not self.scope.active
            or not self.scope.run.active
            or run is not self.scope.run
            or scope is not (expected_scope or self.scope)
            or run.owner is not self.dispatcher
            or run.coordinator is not self.scope.run.coordinator
        ):
            raise RuntimeError(
                "Dispatch lease is not active in this session/run/parent call"
            )
        if run.coordinator.cancellation.is_immediate:
            raise RuntimeError("Immediate host cancellation is active")
        activation = run.coordinator.get_capability("live.activation")
        if activation is not None and not activation.current_valid:
            raise RuntimeError("Host activation is no longer valid")

    async def wait_cancelled(self):
        """Observe the real host token even across an async foreign-runtime bridge."""
        while True:
            if self.scope.run.coordinator.cancellation.is_immediate:
                return "host_cancel_requested"
            try:
                self._valid()
            except RuntimeError:
                return "lease_revoked"
            await asyncio.sleep(0.05)

    async def call(self, name, arguments, *, request_id=None):
        self._valid()
        if name not in self.tools or name in FORBIDDEN:
            raise ValueError("Tool is not available through this lease")
        if not isinstance(arguments, dict) or arguments.get("async") is True:
            raise ValueError("Background tool dispatch is unsupported")
        call = ToolCall(id=str(uuid4()), name=name, arguments=arguments)
        source = {
            **self.context,
            "tool_call_id": call.id,
            "tool_name": name,
            "request_id": request_id,
        }
        observation = {}
        tools = self.dispatcher.wrap_tools(
            self.scope.tools, observed_name=name, observation=observation
        )
        hooks = AttributedHooks(self.scope.hooks, source, observation)
        coordinator = AttributedCoordinator(self.scope.run.coordinator, observation)
        # Always take the existing ordinary engine path. In particular, never
        # call a mounted tool directly when hooks or authority are unavailable.
        try:
            with self.dispatcher.call_scope(
                call, tools, hooks, self.scope.parallel_group_id
            ) as child_scope:
                tools = self.dispatcher.wrap_tools(
                    tools,
                    observed_name=name,
                    observation=observation,
                    authority=lambda: self._valid(child_scope),
                )
                _, _, content = await StreamingOrchestrator._execute_tool_only(
                    self.dispatcher.owner,
                    call,
                    tools,
                    hooks,
                    self.scope.parallel_group_id or self.scope.call.id,
                    coordinator,
                )
        except asyncio.CancelledError:
            await self.scope.hooks.emit(
                "tool:dispatch_cancelled",
                {
                    "tool_name": name,
                    "tool_call_id": call.id,
                    "parent_tool_call_id": self.scope.call.id,
                    "dispatch_source": source,
                    "outcome": "unknown",
                    "effects": "not_rolled_back",
                },
            )
            raise
        self._valid()
        if observation.get("denied"):
            status, success, output = (
                "denied",
                False,
                "Tool execution denied by host policy",
            )
        else:
            post = observation.get("post_result")
            confirmed = post.get("success") if isinstance(post, dict) else None
            if observation.get("engine_error"):
                status, success = "failed", False
            elif observation.get("post_seen") and confirmed is True:
                status, success = "completed", True
            elif observation.get("post_seen") and confirmed is False:
                status, success = "failed", False
            else:
                status, success = "unknown", None
            try:
                output = json.loads(content)
                # The core hook transport may copy result dictionaries, making
                # the ordinary engine serialize the full post-hook ToolResult.
                # Unwrap only when it is exactly the captured confirmed envelope.
                if (
                    isinstance(post, dict)
                    and isinstance(post.get("success"), bool)
                    and output == post
                ):
                    output = post.get("output")
            except (ValueError, TypeError):
                output = content
        return {
            "status": status,
            "success": success,
            "output": output,
            "source": source,
        }


class ApprovedDispatch:
    version = 1

    def __init__(self, owner):
        self.owner = owner
        self.locks = {}

    @property
    def enabled(self):
        return self.owner.config.get("programmatic_dispatch", False) is True

    @contextmanager
    def run_scope(self, coordinator):
        run = Run(self, coordinator, str(uuid4()))
        token = _RUN.set(run)
        try:
            yield
        finally:
            run.active = False
            _RUN.reset(token)

    @contextmanager
    def call_scope(self, call, tools, hooks, parallel_group_id):
        run = _RUN.get()
        scope = (
            CallScope(run, call, tools, hooks, parallel_group_id)
            if run and run.owner is self
            else None
        )
        token = _CALL.set(scope)
        try:
            yield scope
        finally:
            if scope:
                scope.active = False
            _CALL.reset(token)

    def bind(self):
        scope = _CALL.get()
        if (
            not self.enabled
            or scope is None
            or scope.call.name != "tool_exec"
            or not scope.active
            or not scope.run.active
            or scope.run.owner is not self
            or scope.run.coordinator is None
        ):
            raise RuntimeError("No active authorized tool_exec call")
        lease = DispatchLease(self, scope)
        lease._valid()
        return lease

    def wrap_tools(self, tools, observed_name=None, observation=None, authority=None):
        wrapped = {}
        for name, tool in tools.items():
            raw = tool.tool if isinstance(tool, GuardedTool) else tool
            allowlist = self.owner.config.get("programmatic_allowed_tools")
            if name != "tool_exec" and (
                name in FORBIDDEN
                or getattr(raw, "name", name) in FORBIDDEN
                or (allowlist is not None and name not in allowlist)
            ):
                wrapped[name] = raw
                continue
            lock = self.locks.setdefault(id(raw), asyncio.Lock())
            wrapped[name] = GuardedTool(
                raw,
                lock,
                observation if name == observed_name else None,
                authority if name == observed_name else None,
            )
        return wrapped
