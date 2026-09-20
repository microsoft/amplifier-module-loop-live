# Work profile acceptance — September 19, 2026

The pinned profile is installable and the portable interaction works with real
OpenAI and Anthropic providers in Unified. This establishes the tested execution
contracts. Real Chromium interaction and compaction now pass as well. This does
not establish ChatGPT quality parity, broad reliability, or physical microphone
and speaker acceptance. The earlier voice intent failure is retained below; the
same forwarded request passes with the new host framing.

## Tested setup

- Unified v0.11.1, merged `6e9db3e07516af13c352292fed7843eed363cff2`.
  Initial interaction runs used candidate `9ba0ca5`; the merge changes only a
  validation document relative to that candidate, not application code.
- The preceding adapter lock targeted released v0.11.2,
  `93e5d5d48ecb61401b84d248ffa9a64fc5f8fee9`, which fixes fresh-browser startup.
  Its 49 offline tests and a fresh real-provider Terra compaction run passed all
  14 checks, including public streaming, successful summarization, private summary
  text, input retention and HTTP/SSE reconnect. Compaction took 6.769 s in that run.
- The preceding adapter pinned Unified v0.11.4 `9112dc3` and merged loop-live
  `11a730a`. Browser runs used fix commit `171414e`, whose loop/test changes are
  identical to the separately merged scheduling correction (PR #5).
- Core 1.6.1; Foundation `695f875`; initial API matrix loop-live `7a2a9b9`;
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

The locked-environment voice adapter/cross-chat run passed 16 checks. Its
side answer arrived in 3.844 s while the child remained gated. Fresh text streaming
was visible in eight SSE snapshots. This is transport evidence, not a visual
review of the browser or a voice-audio latency measurement.

Offline validation: **50 tests passed** after the scheduling regression was added
(49 in the preceding isolated and locked environments).
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

## Browser and audio continuation

Two real Chromium clients on the same isolated host exercised real Terra responses.
The text interaction passed all 11 checks: a side answer in 2.769 s while the child
was gated; corrected final result; actual stdout verified; one command execution;
completion while browser A was offline; final response visible in both clients;
separate drafts; live public text in both windows; reload preservation; and no
browser, HTTP or visible UI errors. Screenshots were visually inspected.

The browser compaction scenario passed all 9 checks. Its visible “Making room”
status remained compatible with entering a correction, and the report objective,
reference and corrected color survived. Both clients showed public streaming,
and the second client's draft remained unchanged.

The browser uncovered a real ownership scheduling race: after removing a pending
input, loop-live scheduled a turn before marking its generation active. A host
control could briefly see an idle session and release the turn's activation. The
fix publishes generation start before scheduling the task. A deterministic test
fails against the previous code and passes the fix; stale activation guards remain
in place. This was newly exposed by browser coverage, not introduced by v0.11.4.
The focused correction was separately reviewed and merged in loop-live PR #5.

Real `gpt-live-1` audio connected with a synthetic Chromium microphone. Speech was
recognized and forwarded to the same Terra session, which answered while its
child was pending. The complete forwarded-phrasing run passed 14 of 15 checks:
audio packets traveled in both directions, received audio energy was nonzero,
ending the call left accepted work running, the child executed once, and both
browsers received its result after reconnect. **Correction retention failed:**
“tell Amplifier ... delegate this update” was interpreted as a request for another
child, and the original BLUE color remained. The canonical request contains the
recognized correction. This is an intent-handling failure, not a successful voice
acceptance or a transport error. It remains reproducible with
`--voice-phrasing forwarded`.

A separate run using the direct correction from the text scenario passed all 15
checks, including ORANGE retention and call end without canceling accepted work.
It recorded 2,388 outbound and 2,315 inbound audio packets with nonzero received
audio energy. Its 42.847 s interval starts at the call click and includes the
fixture's 30 seconds of initial silence, so it is not a speech-response latency
measurement. The successful direct request does not erase the forwarded-request
failure or establish general voice instruction reliability.

Test-helper failures are also retained separately: string polling conflicted with
the app's CSP; a Markdown `42.` list marker was invisible to a text-only locator;
and an absent RTP statistic was represented as null. The helper fixes preserve
CSP and recognize actual rendered output; none changes the app's results.

## Packaged-worker candidate and persistent preview

The first persistent preview pinned Unified candidate `03f6c85` (PR #73, based on v0.11.8)
and loop-live `de307c3`. The latter adds service provenance for job recovery to
the merged scheduling fix. The host adds explicit voice delivery context,
read-only projection support for legacy recovery records, and unique
same-family/same-model restoration of legacy provider IDs. Model and reasoning
effort remain pinned; ambiguous mappings are rejected.

All three new browser runs used the normal packaged worker environment, real
providers, real Foundation children and the production frontend:

| Scenario | Model | Result |
|---|---|---|
| Forwarded synthetic voice correction | Terra | 15/15 checks passed, including ORANGE retention, side answer during child work, real audio packets, call end, offline completion, reconnect, streaming and drafts |
| Correction during visible compaction | Terra | 9/9 checks passed, including original objective, reference and correction retention |
| Background child and typed correction | Opus | 11/11 checks passed, including verified stdout, exactly one execution, streaming, independent drafts and reconnect |

The forwarded-voice run reuses the exact synthetic utterance from the earlier
failed run. That failed evidence is retained; this acceptance pass does not
establish general voice reliability or measure speech-response latency.

A persistent preview uses the same installed candidate, normal authentication,
packaged worker and a private synthetic workspace. Its real browser check passed
7/7 checks: login required, actual file read, legacy `openai` selection restored
to `terra` with the same model/effort, Work as the default bundle, response retained
after reload, no browser errors and no alerts. Authentication for automation used
an origin-scoped local control token; the actual PAM login remains a manual check.
This fixture creates its own legacy selection; no real user session was modified.

The Unified candidate passed 1,036 Python tests (11 opt-in skips), an additional
12-check actual-worker/control run, 154 frontend tests, the production build and a
Chromium recovery-presentation check. That browser check verifies collapsed
service updates while identical user-typed text stays editable and attributed to
the user. Core plus Foundation/adapter composition passed 50 tests; source and
wheel packages built. These are correctness and integration checks, not a model
quality benchmark.

The adapter now supersedes that preview pin with `b8eaa37` on v0.11.9. It includes
the reviewed `10a1bd5` canvas-retention hotfix (merged in Unified PR #75). The
voice, provider-selection, history-projection and worker source files are
unchanged from the candidate used for the three paid browser scenarios above.
The combined source passed 1,042 Python tests (11 skips), all 154 frontend tests,
the recovered-history browser check and package build. The isolated preview was
backed up and updated only while idle; canonical transcript hashes and all saved
client drafts/attachments matched across the update. Earlier v0.11.8 candidates
should not be used for durable canvas work.

The running upgraded preview retained a synthetic canvas body through 75 seconds
of normal client actions (crossing the maintenance interval), then through a
second restart. Two independent client drafts also survived. Reopening the
artifact succeeded through the API. Visual reopening is being checked separately.
Existing user transcripts and client draft/attachment records remained unchanged.

## Still unverified

Physical microphone and speaker behavior, spoken interruption, other browser/device
combinations, broad model task quality, durable compaction resume, and crash
recovery of all pending external operations. Synthetic audio transport and visual
browser checks do not prove those properties. See
[work-profile.md](work-profile.md) for reproduction and remaining acceptance.
