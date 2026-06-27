---
status: proposed
---

# `send()` may reply to the caller's own inbox, observed via the firehose; supersedes ADR-0005's bar

[ADR-0005](0005-return-address-on-the-one-way-send-verb.md) forbade `send` from setting its
return address to the caller's own inbox. The reason was sound *at the time*: with a future-only
reply dispatcher, a reply arriving for a `send` (which registers no future) had nowhere to
resolve and was silently dropped — so the method surface encoded "`send` replies *elsewhere* (a
third-party Return Address) or is fire-and-forget; only the request verbs reply to the inbox."
The [client caller-surface redesign](../designs/client-caller-surface-spec.md) adds a **hub +
firehose** that consumes the inbox **independently of any per-run future**, so a futureless reply
is now *observed* (via `client.events()`), not dropped. The premise of the restriction is gone.

Decision: `send()` routes its terminal to the caller's **own inbox** (like `start`/`execute`),
registering no per-run handle; the reply lands on the inbox and is observed on the firehose,
demuxed by `correlation_id`. `send` means "dispatch without awaiting," not "reply elsewhere." This
supersedes **only** ADR-0005's bar on `send`→own-inbox; the verb names (`send`/`start`/`execute`)
and the "a reply future resolves iff its callback is the inbox" invariant still stand.

Consequences (deliberate drops from the client surface): the per-call `reply_to` third-party
Return Address and a *true* fire-and-forget mode (callback `None`, no reply produced) are **not**
on the new client surface. Cross-process decoupling is covered instead by
`connect(inbox_topic="shared")`; "I don't need the result" is covered by a non-awaited `send`
plus simply not reading the firehose. Wire-level fire-and-forget — a *node* dispatching with no
return address — is unaffected; it stays a node concept, not a client verb. The design is
specified in the caller-surface spec; this ADR records the contract change, ahead of
implementation. Decided 2026-06-24.
