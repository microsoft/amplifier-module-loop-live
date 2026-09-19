# Validation

## Version 0.2.0 — September 19, 2026

Against extraction base `bb9f5966d285ee4a93f4d84309aacf4bd9a09b5a`,
the complete portable suite passes 39 tests. Coverage adds identified generation
completion, delivered versus accepted inputs, provisional/final response
separation, active delegates, and task-local ownership across parked sessions.
Regression witnesses cover invalid input ownership failing explicitly and
duplicate receipts surviving host reacquisition without replay.

Source and wheel builds pass. The wheel installs and imports in a fresh
environment outside the checkout without CLI/application adapters or provider
SDK imports. CI repeats tests and the installed-wheel check on Linux and macOS
with Python 3.11 and 3.13. These checks use real Core/context/streaming modules
with fixture providers; no paid model or microphone test is implied.

## Local extraction validation — September 14, 2026

This is local development evidence, not an upstream compatibility certification.

The first tested implementation was module `f091ad6`, customized Amplifier CLI
`57adce0` (based on upstream `772bdb4`), and Amplifier Converge `88a5417`.
The runtime uses amplifier-core 1.6.1 and loop-streaming `2feefe4`.

## Executed checks

- 17 core tests passed in an independent environment containing neither the
  CLI adapter, Amplifier Converge, nor provider packages.
- 109 Amplifier Converge runtime tests passed after switching imports to the
  extracted packages, including native transport fixtures, children, approvals,
  attachment handling, and cancellation.
- Two customized CLI command tests passed. The application TypeScript check,
  155 browser/server tests, 32 launcher tests, browser build, and launcher
  wheel/source distribution build also passed.
- A private DTU installed the standalone CLI and packaged application into
  separate Python environments from Git mirrors. The CLI environment had no
  `amplifier_live` application package.
- The standalone CLI prepared the configured `anchors-amp-dev` bundle with
  spawning, resume, partial sessions, model routing, and mention handling.
- Real Astra and Luna CLI calls returned their requested markers and closed
  cleanly. Astra reported native steering; Luna reported request-boundary mode.
- The actual browser composer connected to a native Astra manager with 45 tools
  and 57 agents and received its requested reply. No page errors or horizontal
  overflow were observed in that check.
- A browser work request created one finite `anchors:explorer` worker, which
  used Terra and read a seeded file. During that work, Astra accepted a service
  observation and a user side question. Sending the identical service input
  twice produced one native acceptance. The public response answered the side
  question and reported the file marker; the worker and operation completed.

The browser harness needed two corrections: wait for the asynchronous diagnostics
checkbox update, and select the work operation rather than the preceding
tool-disabled chat-classification operation. Those failed harness attempts are
retained privately and are not counted as passing interaction checks.

## Limits

The DTU had no cached ChatGPT OAuth login. Automatic OAuth login was disabled in
its private settings; ChatGPT remained an explicitly reported unavailable
provider. Copilot login and physical microphone/speaker behavior were not
validated. The trial did not certify all configured providers, every bundle
tool, machine-loss recovery, or other upstream versions.

Existing provider extensions and computer-use repairs remain in the optional
CLI adapter. Stable provider capabilities and supported loop-streaming extension
points remain future ecosystem work. `amplifier-agent` retains its existing
one-shot contract; it was not modified by this extraction.

Private profiles, credentials, transcripts, and screenshots are excluded from
this repository. The DTU is a user validation environment, not a deployment.
