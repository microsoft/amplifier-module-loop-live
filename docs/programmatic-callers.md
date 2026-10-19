# Explicit controller callers

`tools.dispatch` continues to run every inner call through the ordinary streaming
engine, its pre/post hooks, approvals, cancellation and execution locks. The host
may set `programmatic_callers: [tool_exec, ui_use]` alongside
`programmatic_dispatch: true` to permit a mounted bounded controller to bind an
active-call lease. Without explicit caller configuration, a portable module may
declare its caller and bounded target list in `tools.dispatch.registrations` at
mount. Explicit host disable/caller/target restrictions take precedence. This
trusted module declaration is separate from model output or ambient credentials.
Reusable behaviors do not select an orchestrator.

The caller must have an explicitly permitted active call identity. Callers, delegate, task and
live_job cannot be nested targets. Changing the caller policy revokes outstanding
leases. `programmatic_allowed_tools` still restricts targets. No raw execution
fallback is allowed when dispatch is absent. Consumers distinguish denial, failed
execution and unknown outcomes and do not retry an unconfirmed effect.

The implementation now lives in loop-streaming; this module reuses it rather than
maintaining a second execution engine. `model.complete(ChatRequest)` provides
bounded, attributed ordinary inference with provider request policy, input-budget
checks, call counting and cancellation. The legacy `model.current()` accessor is
retained for compatibility; UI-use no longer calls a provider through that raw
accessor. The service selects no decision backend and changes no routing policy.
