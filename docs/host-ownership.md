# Host ownership between live turns

A long-lived loop can stay mounted while its host releases exclusive session
write ownership between turns. Storage, lock acquisition, transcript saving,
conflict detection, and whether to retain a warm session remain host policies.
This module introduces no checkpoint files or transcript/event writers.

Hosts opting into this mechanism provide all of the following:

- `live.activation`: an object with `bind(token) -> context_token`,
  `reset(context_token)`, `current() -> token`, and `current_valid -> bool`.
  `bind` must reject missing, released, or superseded tokens. `current` must
  return the caller's task-local token, including a stale token when called by
  stale work; it must not silently substitute the host's latest acquisition.
- `runtime.capture_activation = activation.current`, set before starting the
  execution task. Inbox entries capture their producer's ownership at enqueue
  time, including provider completions and child callbacks.
- `Input(..., activation=token)` for each admitted input. Acquire ownership and
  bind that same token in the submitting task before calling `runtime.submit`.
  Activation is private transport authority, excluded from input identity
  comparisons, representations, runtime events, and provider messages.
- `live.park`: an async callable accepting `activation=token`. The loop awaits
  it after announcing idle, with no pending inputs or live jobs. The initial
  empty loop can park with `None`; the host must handle that startup state.
  A host normally finishes durable writes and releases ownership here.
  Inputs or callbacks can arrive while this callback is awaited; recheck pending
  work under the host's admission synchronization before releasing ownership.

The loop validates an event's captured token before processing it. Generated
service inputs retain ownership from their child/job event. A stale callback
fails closed rather than borrowing a newer writer's token. It never replays
the old work. A stop command remains accepted while parked, and final saving
through the existing `live.checkpoint` callback is skipped when ownership is
invalid. The callback's historical name does not prescribe checkpoint storage.

If an input token is invalid even though its producer token is valid, the turn
reports failure and closes; it does not leave the manager waiting indefinitely.
Retrying an already accepted input with a fresh ownership token returns its
original receipt without enqueueing it again. Changing the command content
under that identity still fails.

After reacquiring exclusive ownership and before admitting new input, the host
must check whether another host changed the saved transcript or configuration
and reload or retire the warm session as
appropriate. The loop cannot detect changes to a host-owned store. The host
must likewise gate asynchronous writes and persistence callbacks, and await
shutdown before releasing an active acquisition.

Without these optional capabilities, existing live hosts keep their original
lifecycle. Without a live runtime, ordinary finite execution is unchanged.
