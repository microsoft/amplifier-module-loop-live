# Correlating manager responses

A content block is provisional output. It may precede a tool call, a steering correction, or another provider response. Hosts must not resolve a request from the first `assistant.message`.

The optional live runtime now publishes a bounded manager-generation contract:

- `generation.started`: `generation_id`, `initial_input_id`, and optional `call_id`.
- `assistant.message`: `generation_id`, `input_ids`, `accepted_input_ids`, and provisional `text`.
- `generation.finished`: `generation_id`, `input_ids`, `accepted_input_ids`, `text`, `active_job_ids`, and `disposition: manager_turn_finished`.
- `generation.failed`: matching generation/input identities and `error_type`.
- `generation.detached`: matching identities and existing unconfirmed cancellation outcome.

`input_ids` contains commands actually delivered at a request boundary or confirmed by `steering.applied`. Merely queued inputs do not belong to the completed generation. Native `steering.accepted` appears in `accepted_input_ids` until an adapter emits `steering.applied`; accepting a correction does not prove that its successor response has used it. Existing native adapters that emit only acceptance must add an applied event at their confirmed successor boundary before hosts can resolve those requests.

Finished `text` is the final ordinary assistant message from the Amplifier context, without tool calls. It excludes provisional messages that the base streaming engine may concatenate into its returned string. Empty text is valid and does not mean work completed successfully.

A generation stays open through its final checkpoint while accepted request-boundary steering remains. Such input can require another engine invocation within the same generation, without another `generation.started` or replay of earlier work. Terminal publication closes admission before awaiting context reads.

Ordinary `Input("user", ...)` addresses the conversation, not a captured
generation. During an active native request the loop first calls `steer_live`
when eligible. `False` is a definitive no-effect refusal and uses the existing
request-boundary queue. `True` transfers ownership to the provider, not proof of
remote acceptance or application; the provider may own its own definite-refusal
fallback. Exceptions or an indeterminate return never enqueue an alternate.
Later input cannot overtake an earlier boundary batch, including while context
insertion and request preparation await. The barrier releases at the engine's
prepared-request boundary, not when the pending deque becomes empty.

At normal completion, the existing completion marker can yield once to already
accepted inbox commands after the final checkpoint. Pending ordinary user or
unanchored steer input can then continue the same generation, without replaying
the original prompt or tools. Job/service callbacks alone do not require that
continuation. Work arriving after closure starts a normal new generation;
explicit generation-anchored steering still never falls forward.

Ordinary `input.queued` and `input.delivered` carry `delivery` (`new_turn` or
`request_boundary`), `source`, and the current `generation_id` when one exists.
On shutdown, local unconsumed input emits `input.held` and is removed, never
implicitly replayed. An interrupted context insertion or native attempt emits
`input.unknown` with its route instead of claiming no effect. Provider-owned
native pending work is not relabeled held by the loop. Stop fences new work
admission while identical accepted IDs still acknowledge without replay.

A finished manager generation is **not** completion of all delegated work. `active_job_ids` identifies still-running local delegate jobs. Their results reenter the manager through service observations. A host should show the manager's answer plus pending work, and continue processing later generations. Do not replay a job to obtain its result.

This contract changes no finite-session behavior, imports no app/CLI/provider SDK, and adds no provider protocol fields. Existing consumers can ignore the added metadata. Fixtures cover steering during a request, provisional text before a background tool, final text selection, native acceptance versus application, correlated failure, and cancellation without a success event.
