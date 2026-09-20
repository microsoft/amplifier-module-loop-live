# Install and evaluate the Work profile

This opt-in bundle combines live execution, boundary compaction, transcript
retrieval, filesystem/search/bash tools and bounded self-delegation. It supplies
no provider or model. The host supplies configured credentials, approvals,
canonical history and the live input/output transport.

## Use in Unified

Select the repository's `bundle.md` as a conversation's bundle. A local checkout
uses an absolute file path. A published revision uses:

```text
git+https://github.com/bkrabach/amplifier-module-loop-live@<reviewed-profile-commit>#subdirectory=bundle.md
```

For an existing configured bundle, compose this behavior last:

```yaml
includes:
  - bundle: <your-existing-bundle>
  - bundle: git+https://github.com/bkrabach/amplifier-module-loop-live@<reviewed-profile-commit>#subdirectory=behaviors/work-local.yaml
```

The root includes five tools, including `delegate` and `read_transcript`. The
behavior alone changes only the loop/context and adds transcript retrieval; it
preserves the existing tools and provider configuration. Module sources use
reviewed commit IDs, including loop-live `de307c3` (merged scheduling fix plus
recovery provenance) and context-managed
`5b0816e`. A host source override can still supersede a source: inspect Unified's
effective configuration when comparing results.

Unified v0.11.1 supplies the tested HTTP/SSE host contract; the adapter now pins
candidate `b8eaa37` on v0.11.9, with voice intent, recovered-history presentation,
and legacy provider restoration fixes. These candidate changes are in Unified
PR #73; the published release alone does not contain them. This candidate also
includes the v0.11.9 resource-retention hotfix; do not use earlier preview pins
for durable canvas work. The profile also pins
the merged loop-live scheduling fix required by the real browser checks. Unified supplies passive
conversation history and child model inheritance. These are host capabilities,
not new tools installed into arbitrary hosts by this bundle. Plain finite hosts
still run finite turns; installing YAML alone does not create a concurrent UI.

The root profile is intentionally small. Compose additional ecosystem behavior
bundles explicitly when needed. Children use the parent's effective provider and
model unless a supported explicit override is supplied. `async: true` opts an
eligible delegate call into background execution. Children cannot recursively
delegate by default. Four pending jobs is a per-loop limit, not a tree-wide cap.

## Reproduce live acceptance

Clone this repository and run from its root. GitHub access to private repositories
is required. The separate adapter environment keeps provider SDKs and Unified out
of the core loop package. Its manifest and lock pin the reviewed host/runtime.

```sh
uv sync --project adapters/unified --locked
uv run --project adapters/unified --locked python adapters/unified/acceptance.py \
  --allow-live --provider <configured-instance-id> --profile work \
  --scenario interaction --output /absolute/private/new-run-directory
```

`--settings` defaults to `~/.amplifier/settings.yaml`. The runner reads only the
selected OpenAI or Anthropic instance and its adjacent keys file. It preserves
the configured model and reasoning effort, bounds output to 8192 tokens, and
creates separate shared settings, app data, ownership state and a synthetic
workspace. The output directory must be new and outside this repository. Existing
services and real session histories are not mounted. **This makes paid model
calls and stores private transcripts/configuration. Do not commit its output.**

Scenarios:

- `interaction`: a real child runs a gated command; a correction and side question
  arrive while it waits. Check the actual result, one tool execution, child model,
  input deduplication, text/SSE progress, passive history and separate client drafts.
- `compaction`: import explicitly synthetic historical turns, constrain the request
  budget and send a correction during visible boundary compaction. Verify objective,
  reference and correction retention, successful summarization, original history
  retrieval, and suppression of summary text from public streaming.
- `cancellation`: cancel a real pending job through the model-visible tool. Verify
  the requested and settled events, no retry, and preservation of earlier effects.
  This does not certify process-tree termination for arbitrary tools.
- `--scenario interaction --input-mode voice-backend`: send the correction through
  Unified's real `VoiceCall.execute`, close the call while the child remains pending,
  and verify the result still arrives. This tests the voice-to-work adapter, **not
  microphone input, speech recognition, WebRTC, audio playback or barge-in**.

Use `--profile baseline` with the same provider, scenario and inputs for a matched
context-policy comparison. Baseline changes only context-managed to context-simple;
both profiles use the same live loop, instructions and tools. This is a context
ablation, not a comparison against every previous Amplifier default or evidence of
ChatGPT quality parity. Single runs are acceptance checks, not a quality benchmark.
Repeat fixed tasks before drawing statistical conclusions; provider caches and
different numbers of calls can affect cost and timing.

Each private `report.json` records checks, effective context/loop, package revisions,
model/effort, elapsed time, live text deltas, SSE observations and provider-reported
usage/cost. `events.json` contains public events, never raw reasoning. Canonical
transcripts stay in the isolated shared root. Missing price information must remain
unknown, not zero. Preserve failed runs alongside successful ones.

## Reproduce browser and synthetic-audio acceptance

```sh
uv sync --project adapters/unified --locked --group browser
uv run --project adapters/unified --locked --group browser python -m playwright install chromium
uv run --project adapters/unified --locked --group browser python adapters/unified/browser_acceptance.py \
  --allow-live --provider <configured-instance-id> \
  --packaged-worker \
  --output /absolute/private/new-browser-run
```

Use `--scenario compaction` for visible compaction and a correction during the
pause. The default interaction uses two real browser clients, independent drafts,
a real pending child, visible public streaming, an offline browser during task
completion, reconnection, and reload. Screenshots are saved for visual review.
Authentication uses the isolated host's control token scoped to its local origin;
this does not exercise PAM login. The app's security policy remains enabled.
`--packaged-worker` uses Unified's normal isolated dependency environment. Without
it, the worker runs in the adapter environment used by the earlier matrix.

On macOS, add `--voice-audio` to feed explicitly synthetic speech through Chromium's
microphone into the app's real voice connection. This uses `say` and `afconvert`,
requires a configured voice credential, and makes additional paid voice calls.
It checks recognized speech reaching the working session, outbound/inbound audio
packets and received audio energy, and accepted work surviving call end. It never
records the physical microphone. The default `--voice-phrasing direct` uses the
same correction as the text test. `--voice-phrasing forwarded` retains a naturally
phrased forwarding request that exposed a correction-handling failure and now
passes with the pinned host fix; see the validation report. Neither variant proves physical speaker output or spoken
barge-in. Do not compare its elapsed time with text latency: the microphone file
intentionally begins with 30 seconds of silence for connection establishment.

Physical microphone/speaker use, interruption of spoken output, and additional
browser/device combinations still require separate acceptance.

## Start a persistent local preview

The preview uses the normal packaged worker, authentication and filesystem tools.
Its private settings, histories and workspace are separate from existing apps.
It makes real provider calls. The chosen model remains unchanged; the primary
provider's output is bounded to 8192 tokens for this test environment.

```sh
uv run --project adapters/unified --locked python adapters/unified/preview.py init \
  --directory /absolute/private/new-preview --provider <configured-instance-id> \
  --also-provider <another-configured-instance-id> --port 8956
uv run --project adapters/unified --locked python adapters/unified/preview.py serve \
  --directory /absolute/private/new-preview
```

Omit `--also-provider` for a single-provider preview. Open the printed localhost
address and authenticate normally. Restarting `serve` preserves conversations;
`init` refuses to overwrite an existing directory. New chats default to the Work
profile. The synthetic workspace contains `delivery.txt` and a 45-second
`collect_delivery.py` exercise. Ask for one background helper to run that script,
then ask a side question or change the report while it waits. Closing the voice
call should leave accepted work running. Explicitly stop a task to cancel it.

The profile does not yet add durable compaction checkpoints, portable child-agent
coordination, a shared asynchronous-question ledger, or managed process I/O.
