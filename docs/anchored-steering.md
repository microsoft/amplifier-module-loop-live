# Steering one active generation

Hosts that must never turn late steering into a new request can opt into the
`live.steering` coordinator capability. Ordinary `Input("steer", ...)` keeps its
existing behavior; this contract is additive and requires a live runtime.

The capability is `{version: 1, mode: "request_boundary" | "unavailable",
cancellable: false, submit: runtime.submit_steering}`. A host captures the active
`runtime.generation["id"]`, then calls:

```python
receipt = await capability["submit"](
    Input("steer", "Use the corrected path", id=stable_input_id), generation_id
)
```

The receipt contains `accepted`, `inputId`, `generationId`, and `disposition`.
`queued` proves admission only. `applied` proves successful insertion into the
active generation's context; it does not prove a provider read or complied with
the correction. `held` proves the input was not injected and will not start a
later turn. `unknown` means context mutation was interrupted and its effect
cannot be confirmed. Held and unknown input must never be replayed implicitly.
The host owns durable receipts across process loss. The runtime retains at most
2,000 accepted identities and refuses new work when full rather than evicting
identity evidence. Repeating an identical ID in the same live runtime returns
its existing disposition; changing its content or anchor is refused.

Successful context insertion emits `input.delivered` and `steering.applied`,
with `input_id`, `target_generation_id`, and `delivery: "request_boundary"`.
`steering.held` and `steering.unknown` include the same input/target identities and
a reason. Context input metadata also retains `target_generation_id`.
Generation terminal events keep the existing distinction between delivered and
accepted-but-undelivered input IDs. A stale or unavailable generation is rejected
before admission. Once admitted, a finishing generation holds late input; it
never converts that input into a fresh turn.

Provider-native `steer_live` is deliberately unavailable through this contract
until it can supply a matching attribution and withdrawal/effect protocol.
There is no replace/retract API: cancellation must not claim to undo context
already written. A cancelled request-boundary context write reports unknown if
its completion cannot be established.

Tests use actual Amplifier Core/context/loop modules with deterministic provider
and tool fixtures. They cover normal application, late arrival during checkpoint,
cancellation before injection, interruption after a partial context effect,
identity reuse, and unsupported/stale generation rejection. They make no live
provider or account acceptance claim.
