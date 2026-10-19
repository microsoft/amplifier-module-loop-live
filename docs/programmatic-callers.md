# Explicit controller callers

`tools.dispatch` continues to run every inner call through the ordinary streaming
engine, its pre/post hooks, approvals, cancellation and execution locks. The host
may set `programmatic_callers: [tool_exec, ui_use]` alongside
`programmatic_dispatch: true` to permit a mounted bounded controller to bind an
active-call lease. The default remains `[tool_exec]`; ambient capabilities do not
authorize another caller. Reusable behaviors do not select an orchestrator.

The caller must have an explicitly permitted active call identity. Callers, delegate, task and
live_job cannot be nested targets. Changing the caller policy revokes outstanding
leases. `programmatic_allowed_tools` still restricts targets. No raw execution
fallback is allowed when dispatch is absent. Consumers distinguish denial, failed
execution and unknown outcomes and do not retry an unconfirmed effect.

`model.current()` returns the currently selected ordinary provider using this
loop's normal selection and host pin. Portable consumers can use its public
`complete` protocol for bounded selection or text writing. The capability does
not select a decision backend, change routing policy, or start a model request.
