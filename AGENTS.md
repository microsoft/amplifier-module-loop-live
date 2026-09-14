# Amplifier loop-live development

The core package must not import Amplifier CLI, Amplifier Converge, provider SDKs,
or host adapters. Optional application integration belongs under `adapters/`.
Preserve ordinary finite execution when the host supplies no live runtime.
Preserve input/call/session identities, approval boundaries, and truthful
cancellation. Interrupted work is never automatically replayed.

Use **Amplifier Converge** whenever naming that application. Keep credentials,
transcripts, user settings, and private validation artifacts outside Git.
Record tested revisions and distinguish fixture evidence from live model tests.
Do not change provider protocol fields without checking current official docs.
