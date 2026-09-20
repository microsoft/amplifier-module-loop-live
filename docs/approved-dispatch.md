# Approved programmatic dispatch

The loop publishes `tools.dispatch` version 1. It is disabled by default: enable
`programmatic_dispatch: true` in loop configuration for a reviewed deployment.
While disabled, ordinary dispatch and concurrent calls behave exactly as before;
lease binding fails closed. No bundle defaults are changed by this adapter. A mounted tool named `tool_exec`
can call `bind()` while it is executing through the ordinary loop tool path.
The capability fails closed when idle or called by another tool.

A lease exposes `context` (`session_id`, `run_id`, `parent_tool_call_id`), `tools`
(permitted names), `call(name, arguments, request_id=...)`, and idempotent `close()`.
The loop's `programmatic_allowed_tools` configuration can restrict names further.
Recognized recursion/delegation/background APIs are excluded (`tool_exec`,
`delegate`, `task`, `live_job`, or arguments containing `async: true`). Other tools
that expose delegation under different names require a reviewed host allowlist.
This initial adapter supports ordinary finite tool execution only.

Nested calls enter the declared streaming-engine dependency's ordinary
`_execute_tool_only` path. It owns pre-hooks, coordinator permission processing,
cancellation tracking, tool execution, post-hooks, and output modification. The
adapter observes structured ToolResult outcomes instead of guessing success from
returned strings. Outcomes are `completed`/true, `denied`/false, `failed`/false,
or `unknown`/null, with `output` containing only the actual post-hook result.

Each nested call gets a host-generated child ID. Its pre/post/error events include
`parent_tool_call_id` and `dispatch_source` with session, run, parent, child, tool,
and optional client request IDs. The request ID is correlation data, never
authorization. Cancelled nested calls emit `tool:dispatch_cancelled` with
`outcome: unknown` and `effects: not_rolled_back`; they do not emit a fake successful
post-tool result.

Leases expire with their parent call, execute scope, host activation, or explicit
closure. Cross-parent/run/session use is rejected. Authority is checked again
after a delayed approval and after waiting for a tool execution lock, immediately
before the actual operation starts. Revocation does not roll back side effects
already made by tools.

When enabled, eligible actual execution is serialized per mounted tool object,
including aliases and ordinary sibling calls; approvals happen before this lock
is acquired. Unsupported delegation tools and names excluded by the configured
allowlist retain their ordinary concurrency, since nested programs cannot call them. Distinct
tools can execute concurrently. There is no global lock spanning approval prompts.
The dispatcher does not create additional ordinary tool invocations.

Tests exercise the real coordinator and ordinary tool engine, including a complete
finite fixture-provider session. These are contract/fixture tests, not live model
or application UI acceptance. No bundle composition is changed here.

The optional `lease.wait_cancelled()` waits for the actual coordinator's immediate
cancellation token or lease/run/activation revocation. Clients should race this
signal with worker output and their deadline. This covers hosts whose foreign
runtime bridge does not propagate cancellation from an outer Python awaiter into
the underlying tool task. It does not infer cancellation merely because an awaiter
disappeared; hosts must signal their real cancellation token or cancel the owned
orchestrator task. Finite `coordinator.request_cancel(immediate=True)` and live
runtime `Input("stop", target="cancel")` are the supported paths verified by the
local cross-module fixture. Graceful cancellation retains existing semantics.
