# Amplifier loop-live

An event-driven orchestrator for Amplifier. Hosts can submit identified user
messages and external observations while work runs, receive progress events,
and manage background delegation. Without a live runtime, execution returns a
normal finite result. Provider-native steering is optional; other providers
receive updates at request boundaries.

This is an experimental extraction from Amplifier Converge. It retains a pinned
dependency on loop-streaming's implementation and is not yet a stable community
API. The optional CLI adapter has separate dependencies and packaging under
`adapters/cli`; importing the core never imports that adapter or either app.

## Install

Install into an Amplifier host environment (amplifier-core is a peer dependency):

```sh
uv pip install 'git+https://github.com/bkrabach/amplifier-module-loop-live'
```

The repository is private during development; normal GitHub authentication is
required. The module has no executable. The CLI adapter supplies the terminal
experience; Amplifier Converge supplies its own browser and session host.

## Bundle configuration

```yaml
session:
  orchestrator:
    module: loop-live
    source: git+https://github.com/bkrabach/amplifier-module-loop-live@main
    config:
      background_delegate: true
```

Pin a reviewed revision in an application lockfile. The optional behavior in
`behaviors/live.yaml` can be composed with a bundle that supplies context,
providers, tools, and hooks. YAML selects the loop; the application supplies its
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

The initial native extension still uses `native_bundle_live`, `steer_live`, and
`close_live` on the selected provider. These are experimental, explicitly
advertised capabilities; selecting a model by name alone does not supply them.
