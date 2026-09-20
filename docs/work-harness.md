# Experimental portable work harness

Compose `behaviors/work-local.yaml` over a configured Foundation bundle and resolve
loop-live, context-managed and tool-transcript to the matching development
checkouts. This profile is opt-in. It neither selects a provider nor replaces a
running installation. Unified's host must use the matching development runtime.

Eligible tools expose `async: true|false`. The default profile requires opt-in;
older bundles with `background_delegate: true` retain their default, and an
explicit false waits for completion. `background_tools` is an allowlist of tool
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

The matching Unified host implements `inherit_effective_model: true`: a child
inherits the parent's explicit UI model and effort unless a provider preference,
agent provider declaration, or model role supplies an override. Saved children
retain the inherited selection on resume. Ordinary bundle defaults still inherit
through the existing Foundation mount-plan composition.
