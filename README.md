# Amplifier loop-live

An event-driven orchestrator for Amplifier. Hosts can submit identified user
messages and external observations while work runs, receive progress events,
and manage background delegation. Without a live runtime, execution returns a
normal finite result. Provider-native steering is optional; other providers
receive updates at request boundaries.

This is an experimental extraction from Amplifier Converge. It follows the
dependency on loop-streaming's implementation and is not yet a stable community
API. The optional CLI adapter has separate dependencies and packaging under
`adapters/cli`; importing the core never imports that adapter or either app.

## Install

Install into an Amplifier host environment (amplifier-core is a peer dependency):

```sh
uv pip install 'git+https://github.com/microsoft/amplifier-module-loop-live'
```

The repository is private during development; normal GitHub authentication is
required. The module has no executable. The CLI adapter supplies the terminal
experience; Amplifier Converge supplies its own browser and session host.

## Bundle configuration

```yaml
session:
  orchestrator:
    module: loop-live
    source: git+https://github.com/microsoft/amplifier-module-loop-live@main
    config:
      background_delegate: true
```

Keep the module source on `main`; consuming hosts stage and validate updates and
record installed revisions for rollback. Put this configuration in the host's
root bundle or standalone execution composition, alongside its context module.
Reusable behaviors leave orchestrator selection to that host. The former
`behaviors/live.yaml` selector has been removed; consumers of that path should
use the configuration above. YAML selects the loop; the application supplies its
live runtime. An ordinary host without that runtime keeps finite execution.

## Host contract

Create `amplifier_module_loop_live.runtime.Runtime`, register it as the
coordinator capability `live.runtime`, and own exactly one `session.execute()`
task. Submit `Input` values of kind `user`, `steer`, `service`, `cancel_job`, or
`stop`. A service must have a distinct source and cannot authorize actions.
Observe runtime events to distinguish acceptance, delivery, tool reports, and
verified outcomes. Stop and await the execution task before cleaning up a session.

Optional capabilities are `live.host` (a `HostAdapter`), `live.jobs` (a private
`JobStore`), `live.checkpoint`, and `live.attachments.encode`. The host owns
approval policy, transport authentication, persistence location, and shutdown.
Runtime history is bounded observation, not a replay log. Saved interrupted jobs
are evidence and are never automatically dispatched again.

If a provider, hook, or host guard cancels an active turn, the live owner settles
through the same cancellation cleanup as Stop. It emits `generation.detached`
with cancellation unconfirmed and `session.closed` with cancelled status; it
does not report success or replay completed tools. Hosts must still reconcile
external effects before resuming interrupted work.

`JobStore.recover(transcript)` compares each saved call and result with the
checkpoint. A returned job whose exact evidence is already present reconciles
silently; the absence of an old display notice is not a recovery event. Missing
calls/results and queued receipts are repaired, conflicting results fail closed,
and newly uncertain outcomes are reported. The returned `recovered` list contains
jobs with actual restoration/change or newly reported uncertainty, not every
job loaded from the ledger.

Recovery notices retain their stable job/call identities and service provenance.
Their optional `metadata.recovery` object contains `version: 1`, `job_id`,
`call_id`, `status`, `outcome`, and `reason` (`restored_evidence`,
`changed_evidence`, or `uncertain_outcome`) for host presentation. The private
ledger remembers which result's uncertainty was announced, so compaction that
removes only the notice stays quiet. That fingerprint is not a checkpoint
acknowledgment: restoring an older checkpoint or losing tool evidence still
requires reconciliation, even if the same result was announced earlier.

Live responses carry generation and delivered-input identities, separate from
provisional assistant content and native steering acceptance. See the
[generation event contract](docs/generation-events.md) and [anchored steering](docs/anchored-steering.md) before using completion
events to resolve a host request or send a notification.

Hosts sharing session storage may supply optional `live.activation` fencing.
Ownership lasts through idle until the host stops and joins the execution task;
idle does not release the writer. The former `live.park` callback is no longer
invoked. See [host ownership](docs/host-ownership.md) for admission and migration.
The loop does not choose a storage format or acquire a lock on the host's behalf.

The initial native extension still uses `native_bundle_live`, `steer_live`, and
`close_live` on the selected provider. These are experimental, explicitly
advertised capabilities; selecting a model by name alone does not supply them.

## Try the terminal experience

The customized CLI branch adds a separate `amplifier live` command. See the
[CLI adapter installation guide](adapters/cli/README.md). Its terminal currently
has its own small command set; this does not replace the ordinary CLI REPL.

## Development and verification

```sh
uv sync --group dev
uv run pytest -q
uv build
```

Core tests run without the CLI adapter or any provider package. They exercise
finite execution, dynamic prompts, queued steering, background delegation with
approval hooks, input identity/backpressure, and interruption recovery. The
optional adapter and application integration are tested separately by Amplifier
Converge's runtime suite.

### Job ledger storage boundary

`JobStore` uses a stable `owner.lock`: nonblocking `flock` on macOS/Linux and a
retained, nonblocking byte-zero CRT lock on Windows. Close or process exit releases
ownership. The lock file is never truncated, replaced or unlinked. Saved records
are explicitly UTF-8. Pending work after process loss remains uncertain and is
never automatically replayed.

With a compatible loop-streaming, a completed background job also retains its
explicit, reference-only `SettledToolResult`. Version-2 job records bind that
carrier to the original call/name and fingerprint; version-1 text records remain
readable without parsing report strings as JSON. Recovery checks both text and
settled metadata, including every duplicate output, and refuses conflicting
evidence rather than overwriting it.

`live_job` read/wait pages slice only `report` text. The separate
`result_metadata` and `result_fingerprint` fields retain the original job's
evidence independently of the page offset; list remains summary-only. These
fields do not give the new control call ownership of the original media.
Service inputs keep job evidence nested under `live_job_result` with service
provenance. Only the matching original tool row receives media metadata during
receipt synchronization. That replacement requires the host's
`session.durable_checkpoint` before another provider dispatch; the media owner
must independently validate the exact save acknowledgment before hydration.
Neither a ledger write nor a service notice establishes a canonical media commit.
Native service shortcuts are bypassed for structured job evidence so they cannot
discard that provenance. Native image transport, provider preparation, storage
authorization, and pixel removal still require their independently qualified
owners; streaming inheritance or a saved reference is not proof of vision.

The host's `live.attachments.encode(command, coordinator)` may return legacy
content directly or asynchronously return an explicit `{"content": ...,
"metadata": {...}}` mapping. Both initial-turn and request-boundary attachment
delivery retain the mapping's metadata alongside the admitted `amplifier_input`
identity and unrelated owner namespaces. Conflicting provenance is refused, not
overwritten. Ref-bearing attachments are checkpointed after canonical append
through loop-streaming's shared `checkpoint_settled_results` helper and before
the engine can count or dispatch; encoding never hydrates media in the loop.

The host must supply a trusted private storage location. On POSIX, new directories
and files request modes `0700` and `0600`; existing storage is not repaired. On
Windows, these mode bits do **not** supply private ACLs. The host must establish
the private directory and safe inheritance **before** constructing `JobStore` or
writing any payload. `JobStore` does not validate or repair Windows ACLs and is
not a security boundary against an untrusted writer with access to its directory.

Each update writes and flushes a same-directory temporary file before atomic
replacement. POSIX retains file `fsync`, `os.replace`, and directory `fsync`.
Windows uses file `fsync` and `MoveFileExW(REPLACE_EXISTING | WRITE_THROUGH)`,
without cross-volume copy or an `O_DIRECTORY` emulation. These request OS flushing;
Windows directory-metadata durability and arbitrary filesystem/hardware power-loss
behavior are not qualified. A refused Windows replacement retains the old file
and in-memory row, raises its Windows error, and cleans up the owned temporary
file; it is not retried. If cleanup also fails, the original error is retained
with a cleanup note. POSIX directory-flush failure can occur after replacement:
that is an uncertain committed state, not permission to repeat a dispatch.

The focused `windows-job-ledger` CI job targets ordinary CPython 3.13 x64 with a
non-editable wheel outside the checkout and binary Core 2.0.2. It requires native
import, contention, close/crash/termination release, Unicode recovery, and refused
replacement checks without skips. Source/model tests on POSIX are not native
Windows acceptance; this lane does not qualify application integration, ACL
provisioning, a live provider, network storage or power-loss recovery.

See [provenance](PROVENANCE.md) for the extraction boundary and upstream dependencies.

### Host continuation admission

The mounted loop advertises `live.continuation_guard_supported: true`. A host may
register async `live.continuation_guard() -> bool`. The loop calls it immediately
before each automatic goal continuation, including after an awaited goal
evaluator. False clears the goal and returns the last response without submitting
a new model request. Exceptions fail closed. The initial turn for an explicit
input always runs; this guard does not cancel current work, replay input, grant
permissions, or create a second scheduler. Hosts own durable pause/resume state.
## Contributing

> [!NOTE]
> This project is not currently accepting external contributions, but we're actively working toward opening this up. We value community input and look forward to collaborating in the future. For now, feel free to fork and experiment!

Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit [Contributor License Agreements](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
