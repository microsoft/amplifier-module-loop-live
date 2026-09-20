# Live execution contracts

Eligible tools expose `async: true|false`. Set it explicitly to select background
execution or await completion. Hosts configured with `background_delegate: true`
retain their existing default. `background_tools` is an allowlist of tool
names whose implementations the host has checked for concurrent use. Only
delegate is eligible by default. The original tool and approval hooks still run.

`live_job` offers list/read/wait/cancel. Results are paged, and wait uses runtime
events, with a maximum 60 second timeout and an early wake on accepted input.
Cancellation is a request; it does not reverse external effects. The host's
existing job ledger remains authoritative across restart; uncertain work is never
replayed. The four-job limit applies to this loop, not a global agent-tree budget.

Providers' `llm:stream_block_delta` text events become `assistant.delta`. Thinking
blocks and tool JSON are excluded. A host may register `live.public_stream` to
suppress utility traffic. `context.compacting` suppresses summary traffic.
Providers that only produce completed content still produce completed messages;
the loop does not fabricate token streaming for them.

`context.active_operations` exposes a small current-job manifest. Boundary context
management can retain it alongside continuation notes and current instructions.
The host remains responsible for canonical history and durable operations.

The authored operating guidance is in `instructions.py`. It is a transparent
Amplifier policy, informed by public agent-harness patterns, not a copy or claim
about ChatGPT Work's private system instructions.

Still separate work: durable input deduplication beyond one runtime process,
tree-wide budgets, a portable child collaboration service, managed process I/O,
asynchronous clarification UI, and provider-native
steering/collaboration. Request-boundary steering remains the portable guarantee.
