# Amplifier live CLI adapter

This optional package hosts `loop-live` using Amplifier CLI's settings, bundle
preparation, routing, hooks, session spawning, and transcript persistence. It is
also consumed by Amplifier Converge's isolated session processes. Neither
application's browser or HTTP transport is part of the core orchestrator.

The adapter currently requires Python 3.13 and includes the configured provider
extensions used by the prototype. This dependency set is intentionally separate
from the core package.

## Install the CLI and live adapter

Use an isolated tool installation when evaluating alongside an existing CLI:

```sh
# Keep Foundation's current source consistent across the CLI and adapter.
cat > loop-live-overrides.txt <<'PINS'
amplifier-foundation @ git+https://github.com/microsoft/amplifier-foundation@main
PINS

UV_TOOL_DIR="$PWD/.tools" UV_TOOL_BIN_DIR="$PWD/.bin" uv tool install \
  --python 3.13 --overrides loop-live-overrides.txt \
  --with 'amplifier-module-loop-live @ git+https://github.com/microsoft/amplifier-module-loop-live@main' \
  --with 'amplifier-core @ git+https://github.com/microsoft/amplifier-core@main' \
  --with 'amplifier-loop-live-cli @ git+https://github.com/microsoft/amplifier-module-loop-live@main#subdirectory=adapters/cli' \
  --with-executables-from amplifier-loop-live-cli \
  'amplifier-app-cli @ git+https://github.com/microsoft/amplifier-app-cli@main'

.bin/amplifier-live-manager --workspace /path/to/project --inspect
.bin/amplifier-live-manager --workspace /path/to/project
```

The CLI's ordinary `amplifier` commands and the adapter's
`amplifier-live-manager` entry point share this isolated environment. Canonical
CLI main does not provide the experimental `amplifier live` command. The adapter
entry point invokes the same live manager directly.

Normal GitHub authentication must be available. Keep install sources at `main`
and record their resolved revisions when retaining validation evidence.

`--inspect` prepares the configured environment and writes redacted reports
without making a model turn. `--pin-provider` selects a configured root provider
instance. `--selection` can specify model and effort; worker routing remains
configured separately. `--bundle` overrides the active bundle for this session.

The provider mounts retain their authentication behavior. On a fresh machine,
configure OAuth providers before mounting them, or disable their
`login_on_mount` option in that environment to avoid an interactive login during
inspection. An unavailable provider is reported as a load failure; another
provider is not silently substituted for an explicitly selected one.

## Live controls

- Enter ordinary messages while work runs; `/steer TEXT` submits a correction.
- `/service SOURCE TEXT` sends an attributed, untrusted observation.
- `/jobs` lists background jobs; `/cancel ID` requests cancellation.
- `/approve ID OPTION` answers one approval. `I approve` works when exactly one
  compatible approval is pending.
- `/quit` stops the session and its owned work. Side effects are not rolled back.
- `--resume UUID` restores saved context and job evidence without replaying work.

Native OpenAI/Astra transport remains an optional session-local extension.
Conventional providers accept updates at request boundaries. Existing
computer-use compatibility repairs are retained in this adapter, not installed
globally by the core module. They should become separately reviewed upstream
changes before a stable community release.

Saved PNG computer screenshots are expanded for both request-budget inspection
and completion. The tested OpenAI model families count image patches separately
from text; their base64 transport bytes must not masquerade as text tokens. The
estimate retains a conservative text byte ceiling and image headroom. Other
formats/models retain the upstream estimate. This is an optional adapter repair,
not a change to the core loop or to the pixels/request sent to the provider.

Native Responses transport now comes from the configured OpenAI provider's
optional `native.NativeResponsesProvider` module. The CLI binds its current
live owner, private diagnostics, image budget, and computer-result policy; it
no longer maintains a second wire protocol. Existing native enablement behavior
is preserved for eligible models. Install the matching provider source during
review; both packages continue to follow their existing `@main` sources after
merge. An already wrapped driver retains ordinary transport rather than bypassing
its instance-level wrapper. A queued correction is reported applied only after
its successor response. Accepted-then-failed steering stops for explicit recovery
instead of inserting the same canonical input twice.

The ordinary fallback also covers explicit model selection and registers the
driver's ordinary cleanup. Existing instance-level completion wrappers remain
intact. To preserve an installation's current native steering capability, merge
the companion provider implementation before this adapter; the older-provider
fallback intentionally provides ordinary transport, not native steering.

At the actual native request boundary, the CLI bridges the provider-owned request
identity into the loop's existing job-ledger scope. This preserves the original
native call item when saved jobs are recovered. The bridge requires the current
provider's own active scope, clears inherited attribution for ordinary/utility
calls, and restores surrounding contexts on completion or cancellation. The core
loop does not import the provider or CLI to obtain that identity.
