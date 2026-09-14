# Extraction provenance

Initial source: `bkrabach/amplifier-converge-live`, commit
`5089e67` (the maintained, privacy-reviewed repository).

The production bundle loop, runtime inbox, and job ledger were moved out of
`runtime/live-amplifier/amplifier_live`. CLI preparation, worker configuration,
terminal handling, and existing provider extensions are packaged separately
under `adapters/cli`. The browser, gateway, detached application host, demo loop,
and tool-disabled voice consultation remain in Amplifier Converge.

The core still subclasses the pinned upstream `StreamingOrchestrator`; this is
an explicit compatibility dependency, not a claim of a stable upstream extension
API. The CLI adapter retains the existing OpenAI transport and local computer-use
compatibility repairs. Those are host-specific experimental extensions; the core
does not install or import them.

No transcripts, credentials, settings files, local test workspaces, or historical
private Git history are part of this extraction. Public distribution and upstream
submission are separate future steps.
