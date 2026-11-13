# Host ownership for a live execution

A live execution retains its host's exclusive session writer through busy and
idle states. `session.idle` is an observation, not a release or reacquisition
boundary. Stop and join the execution and its owned work before releasing the
writer. Storage, lock acquisition, transcript saving and conflict detection
remain host policies; this module introduces no checkpoint or transcript writer.

Hosts opting into activation fencing provide:

- `live.activation`: an object with `bind(token) -> context_token`,
  `reset(context_token)`, `current() -> token`, and `current_valid -> bool`.
  `bind` must reject missing, released, or superseded tokens. `current` must
  return the caller's task-local token, including a stale token when called by
  stale work; it must not silently substitute the host's latest acquisition.
- `runtime.capture_activation = activation.current`, set before starting the
  execution task. Inbox entries retain their producer's ownership at enqueue
  time. This is still necessary for effectful callbacks, even when the host
  retains one activation for the execution lifetime. Bind that lifetime token
  before creating the execution task so shutdown/cancellation saving uses the
  owner's original authority, not the last callback's token. Startup validates
  this binding before announcing readiness or executing work.
- `Input(..., activation=token)` for each admitted input. Acquire ownership and
  bind that same token in the submitting task before calling `runtime.submit`.
  Activation is private transport authority, excluded from input identity
  comparisons, representations, runtime events, and provider messages.

Status-only `child_event` entries emit `child.updated` without binding activation,
appending context, saving or starting a turn. Missing, foreign or stale producer
tokens on these observations do not fail a completed generation or lend authority
to later work. A child report, child `session.closed`, job return or turn
completion is **not** status-only: it can deliver follow-up input or checkpoint
state and must retain the original producer's valid token. Generated service
inputs use that exact token, never the newest acquisition. Event bindings are
task-local and reset after handling. Actual persistence failures still fail.
If binding a dequeued callback fails, `command.rejected` records its
`callback_kind`, available input/child/report/job/call identities,
`delivery: not_admitted`, `reason: invalid_activation` and `outcome: unknown`
before the original error propagates. This observation does not apply the result,
append follow-up context, verify external effects or authorize a retry.

Explicit inputs retain their own activation check at execution, request-boundary
delivery and native steering. Tool dispatch and host persistence must likewise
validate current ownership at the mutation boundary. Stale executable work fails
closed and is never automatically replayed. Stop needs no activation, never
renews one, and final saving through `live.checkpoint` is skipped when ownership
is invalid. That callback's name does not prescribe checkpoint storage.

An invalid input token fails explicitly; it does not leave the manager waiting
indefinitely. Retrying an already accepted input with a fresh ownership token returns its
original receipt without enqueueing it again. Changing the command content
under that identity still fails.

Before starting a replacement execution and admitting new input, the host
must check whether another host changed the saved transcript or configuration
and reload as appropriate. The loop cannot detect changes to a host-owned store.
The host must likewise gate asynchronous writes and persistence callbacks, and
await shutdown before releasing an active acquisition.

Without these optional capabilities, existing live hosts keep their original
lifecycle. Without a live runtime, ordinary finite execution is unchanged.

## Retired parking callback

The loop no longer looks up or invokes `live.park`. Hosts using the former
between-turn release contract must retain ownership until joined shutdown and
start a replacement execution after acquiring a new writer; registering the old
callback does not preserve parking. There is no compatibility negotiation or
implicit reacquisition. Independent hosts remain responsible for their actual
locking and persistence behavior. The optional CLI path does not register
`live.activation`, `capture_activation` or `live.park`; its adapter-owned session
lifecycle is unchanged, not evidence of shared-writer fencing.
