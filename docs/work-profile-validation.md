# Work profile acceptance — September 19, 2026

The pinned profile is installable and the portable interaction works with real
OpenAI and Anthropic providers in Unified. This establishes the tested execution
contracts. It does not establish ChatGPT quality parity, broad reliability, or
microphone/browser acceptance.

## Tested setup

- Unified v0.11.1, merged `6e9db3e07516af13c352292fed7843eed363cff2`.
  Initial interaction runs used candidate `9ba0ca5`; the merge changes only a
  validation document relative to that candidate, not application code.
- Core 1.6.1; Foundation `695f875`; loop-live `7a2a9b9`;
  loop-streaming `603aa6e`; context-simple `2bc8b15`;
  context-managed/tool-transcript `5b0816e`.
- Python 3.13 on macOS. A fresh installation using the committed adapter manifest
  and lock was verified, followed by real-provider runs from that environment.
- Configured model/effort preserved: `gpt-5.6-terra` / high and
  `claude-opus-5` / xhigh. Credentials and provider endpoints are not published.
- Real Foundation children, provider calls, bash tool, isolated worker process,
  authenticated HTTP and SSE. No simulated model responses.
- Disposable settings, app data, ownership state, workspace and synthetic history.
  The existing Mac service and real chat histories were not mounted or restarted.

## Results

| Scenario | Model | Result | Evidence |
|---|---|---|---|
| Converse while child waits | Terra | Passed | Side answer in 1.978 s; verified child stdout; correction retained; one command execution; inherited model; live text and SSE |
| Converse while child waits | Opus | Passed | Side answer in 2.877 s; same execution checks |
| Boundary compaction | Terra | Passed | Visible semantic compaction; correction retained once; originals retrievable; summary text excluded from public deltas |
| Boundary compaction | Opus | Passed | Visible pause and completed compaction recorded; objective, reference and correction retained; originals retrievable |
| Cancel background job | Terra | Passed | Request and settled cancellation events; no retry; prior filesystem effect remains |
| Voice-to-work adapter | Terra | Passed | Real `VoiceCall.execute` while child waits; closing voice leaves accepted work running; actual child result arrives |
| Read another chat passively | Terra | Passed | Separate synthetic prior chat found/read; caller selection and draft unchanged; no extra worker mounted |
| Ordinary context baseline | Terra | Passed | Same retained facts and correction, HTTP/SSE reconnect and draft isolation |
| Ordinary context baseline | Opus | Passed on repeat | One earlier reconnect history-read failure is retained below |

The final locked-environment voice adapter/cross-chat run passed 16 checks. Its
side answer arrived in 3.844 s while the child remained gated. Fresh text streaming
was visible in eight SSE snapshots. This is transport evidence, not a visual
review of the browser or a voice-audio latency measurement.

Offline validation: **49 tests passed** in the isolated and locked environments.
The new tests exercise real Foundation composition from an unrelated workspace,
preservation of existing providers/tools, and a comparison that changes only
context policy. Source distribution and wheel both built. Ordinary CI never
makes provider calls; the new profile CI job checks composition without a private
Unified installation.

## Comparison limits

Baseline uses the same loop, instructions, tools, model and effort, replacing
context-managed with context-simple. Both use a synthetic 24,000-token cap; Work's
summary trigger is lowered to 12% for the test. The shipped profile remains 70%.
The correction is delivered after the first request starts: during semantic
compaction for Work and during foreground inference for baseline.

Both policies retained the tested facts. **No quality advantage has been
demonstrated by these small cases.** Semantic compaction added a model call and a
visible pause. The locked Terra run spent 13.152 s compacting. Model decisions,
provider caching and preparation time varied, so these are acceptance timings,
not a controlled speed/cost ranking. Reports retain input/output/cache tokens,
call counts and provider-reported cost; missing pricing remains unavailable.

The next useful evaluation is a fixed multi-task corpus with repeated same-model
runs, including facts that fall outside protected recent turns and real recovery
after restart. Adding more modules should follow observed failures in that corpus.

## Failures retained

1. The initial child fixture used `python`, absent from the shell PATH. The child
   truthfully returned exit 127. The fixture now uses its explicit interpreter.
2. An initial compaction fixture addressed the old client selection after
   importing synthetic history, so it never exercised the intended context. It
   now identifies the new imported conversation explicitly.
3. The first cross-chat fixture imported a chat and then asserted that the
   preceding explicit import had not navigated. The setup now restores the
   caller's selection/draft before testing passive reads.
4. One Opus baseline run completed the model checks but the first reconnect GET
   returned “Could not read the saved chat.” The saved transcript subsequently
   loaded with zero diagnostics; two merged-version repeats passed. A concurrent
   read/save race is a hypothesis, not an established cause. The runner now
   captures underlying reader exceptions and records any retry recovery separately
   from initial success. The failed run is not counted as a pass.

Private raw reports, canonical transcripts and failure evidence remain outside
Git. Only this sanitized summary is included here.

## Still unverified

Microphone input, speech recognition, WebRTC, playback, spoken interruption,
visual browser acceptance, broad model task quality, durable compaction resume,
and crash recovery of all pending external operations. The voice backend test
does not substitute for those audio checks. The documented manual procedure in
[work-profile.md](work-profile.md) covers the remaining user-facing acceptance.
