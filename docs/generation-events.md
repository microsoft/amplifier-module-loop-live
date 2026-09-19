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

A finished manager generation is **not** completion of all delegated work. `active_job_ids` identifies still-running local delegate jobs. Their results reenter the manager through service observations. A host should show the manager's answer plus pending work, and continue processing later generations. Do not replay a job to obtain its result.

This contract changes no finite-session behavior, imports no app/CLI/provider SDK, and adds no provider protocol fields. Existing consumers can ignore the added metadata. Fixtures cover steering during a request, provisional text before a background tool, final text selection, native acceptance versus application, correlated failure, and cancellation without a success event.
