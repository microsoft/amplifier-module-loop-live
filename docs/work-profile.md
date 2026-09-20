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
reviewed commit IDs, including merged loop-live `7a2a9b9` and context-managed
`5b0816e`. A host source override can still supersede a source: inspect Unified's
effective configuration when comparing results.

Unified v0.11.1 supplies the tested HTTP/SSE host contract; the adapter now pins
v0.11.2 with the fresh-browser startup fix. Unified supplies passive
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

## Finish the audio and browser acceptance

In an isolated Unified instance with the profile selected, use two authenticated
browser windows and the same synthetic workspace. Start a slow delegated task,
connect Live voice, ask a side question, correct the task, interrupt spoken output,
then end the call. Verify typed input remains available, progress is readable,
accepted work finishes once, and the final result appears in both windows. Repeat
through reconnect and visible compaction. Record the browser, voice model,
timestamps and observed failures separately from the automated API evidence.

The profile does not yet add durable compaction checkpoints, portable child-agent
coordination, a shared asynchronous-question ledger, or managed process I/O.
