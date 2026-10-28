# Live execution suspension

Maintenance is opt-in and uses loop-streaming's public versioned continuation
interface. The application owns the host-wide hold and exact authorization; the
loop owns accepted input, generations, jobs and original completion attribution.
It does not introduce another scheduler.

The host installs loop-streaming's `execution.suspension` and `execution.resume`
capabilities, plus:

- `live.suspend(envelope)`: asynchronously persist the exact private envelope and
  matching canonical history, returning `True` only after durability.
- `live.suspended_execution`: the saved envelope when remounting explicitly.
- `live.maintenance_admit(epoch)`: asynchronously validate current host authority
  immediately before resuming owned work.

`Runtime.request_pause(epoch)` closes new input admission immediately. Existing
identified receipt replays remain readable. The loop waits for started operations
to settle; cooperating delegates can suspend through their original tool owner.
Once all owned execution reaches a boundary, the loop records version 1 and emits
`session.suspended`. `release_paused(epoch)` checkpoints and exits with status
`paused`, not completion/cancellation. Release never escalates to process killing.
Provider-native opaque execution advertises no support and refuses this request.

The envelope preserves the original generation, accepted/pending/steering input,
root continuation, exact background job/call/generation origins, and separately
attributed completed results waiting for delivery. A delegate that finishes while
pausing retains its original result input; it is never dispatched again. Persistent
workers retain their lifetime owner even after their current turn finishes.

A fresh host remounts unchanged canonical history and the envelope without normal
interruption repair rewriting it. It still validates history, configuration and
current authority before dispatch. `resume_paused(epoch)` consumes only the
approved continuation and paused jobs. The original delegate's `resume_execution`
owns child result completion. Saved stopped/claimed/unknown work is not replayed.
Ordinary crash recovery without an acknowledged pause remains evidence-only.

Durable suspension of background jobs additionally requires `live.jobs`. The
loop refuses to acknowledge a checkpoint with paused background jobs unless its
ledger contains the matching job/call/continuation records. Fresh remount rejects
missing, extra, or inconsistent paused ledger jobs (including their generation)
before restoring runtime state. Paused ledger rows without an outer envelope
also refuse mounting; they remain held for host inspection, not redispatch.
Ledger-free ordinary execution and suspension with no background jobs remain
supported. The outer envelope must have version 1 and a valid maintenance epoch;
the host still owns binding that private envelope to the correct session.

New background ledger records preserve `parallel_group_id` through resumption.
Older records without that optional field retain `None`; recovery does not invent
an original correlation identity. Shared maintenance admission authorizes the
set of owned work; each engine/tool still enforces its own continuation claim.
`session.resumed` reports that handoff, not successful completion of every claim.

Hosts must distinguish waiting, acknowledged suspension, unsupported engines and
uncertain persistence/hand-off. An arbitrary hanging tool cannot safely be
cancelled by this protocol. Separate emergency process stopping can lose remote
outcome information and is not a resumable checkpoint.
