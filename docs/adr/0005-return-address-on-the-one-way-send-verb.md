# Return address rides the one-way send verb; request verbs reply only to the caller's inbox

With one `callback_topic` slot on the wire, "await a reply future" and "deliver
the reply elsewhere" are mutually exclusive: the client's reply dispatcher
consumes exactly one topic (its own inbox), so a future is resolvable **iff**
the callback is that inbox. We encoded this invariant in the method surface
instead of validating it at runtime: `Client.send(reply_to=...)` is the only
place a custom return address can be set (EIP Return Address — the reply is
someone else's to consume; no future), while `start`/`execute` always reply to
the client's own inbox and lost their per-call `reply_topic` parameter, which
had silently registered a future nothing could ever resolve.

Rejected: a flag on one method (`invoke_node(..., future=False) -> str |
InvocationHandle`) — a boolean that flips the return type needs `Literal`
overload soup, collapses to a union through any wrapper, leaves
`output_type`/`reply_topic` as runtime-rejected or silently-ignored params,
and has no precedent in mature messaging SDKs (aiokafka `send`/`send_and_wait`,
MassTransit `Send`/`Publish`/`Request`, Temporal `start`/`execute`, FastStream
`publish(reply_to=)`/`request`). The verbs were renamed to carry the contract:
`emit_to_node` → `send` (it dispatches an addressed, correlated command — not
an event broadcast, which is what "emit" means in messaging vocabulary; the
node-side `Emit` action it claimed to mirror is unwired dead code), and
`invoke_node`/`execute_node` → `start`/`execute` (Temporal's pairing; "invoke"
vs "execute" are near-synonyms that don't telegraph which one waits — cf. AWS
Lambda's `InvocationType` confusion). No deprecated aliases (pre-1.0
hard-break convention). Decided 2026-06-10; full design in
`docs/designs/client-send-api-spec.md`.
