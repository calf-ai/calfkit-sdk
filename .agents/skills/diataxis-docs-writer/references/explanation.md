# Explanation — understanding-oriented

> Explanation is a discursive treatment of a subject that permits **reflection**.
> It is **understanding-oriented**.

Explanation deepens and broadens understanding. It brings clarity, context, and
light. It answers *"Why…?"* and *"Can you tell me about…?"* — and it joins things
together into a web of understanding.

Its perspective is **higher and wider** than the other three. It doesn't take the
user's eye-level view (how-to) or the close-up view of the machinery
(reference). Its scope is a *topic* — an area of knowledge, bounded in some
reasonable way. The concept of **reflection** is central: explanation comes
*after* doing and knowing, depends on them, yet sheds new light on them. It's the
one kind of documentation you might read *away* from the product — "the only kind
you might read in the bath."

Explanation is often the least *urgent* of the four and so gets neglected — but
it is not less *important*. Without it, a practitioner's knowledge stays "loose,
fragmented and fragile," and their practice is *anxious*. Understanding is what
makes a craft safely your own.

## Key principles

**Make connections.** You're weaving a web of understanding. Connect this topic
to other things — even things outside the immediate subject — when it helps the
reader hold it all together.

**Provide context and the *why*.** Explain why things are as they are: design
decisions, historical reasons, technical constraints, trade-offs that were made.
Draw out implications. Mention concrete examples. This is where the reasoning
that has no place in a tutorial or reference finally gets to live.

**Talk *about* the subject.** Explanation is *around* its topic. A good test: you
should be able to put an implicit (or explicit) *"About"* in front of the title —
*About the caching model*, *About connection-pool sizing*. If you can't, it may
not really be explanation.

**Admit opinion, perspective, and alternatives.** All real human work is
invested with judgement, and understanding always comes *from* a standpoint —
which means other standpoints exist. So explanation can and should weigh
alternatives, counter-examples, and competing approaches: *"Some teams prefer X
because Y; that works well when …, but it costs you …"* Think of it as a
**discussion**, one that can even consider contrary opinions.

**Keep it closely bounded.** Explanation's danger is the opposite of reference's:
it tends to *absorb* other things. Intent on covering the topic, you'll feel the
urge to fold in step-by-step instructions or full technical specs. Don't — those
have their own homes (how-to, reference), and pulling them in both clutters the
explanation and hides them from where users expect them. Decide where the topic
reasonably ends and stop there. Because explanation is open-ended, it helps to
anchor it to a real or imagined *why* question and let that bound the scope.

## The language of explanation

| Pattern | Why |
|---|---|
| *"The reason for x is that, historically, y…"* | Explain. |
| *"W is better than z when …, because …"* | Offer judgements and opinions where appropriate. |
| *"An x here is analogous to a w in system z. However, …"* | Context and analogy that aid understanding. |
| *"Some users prefer w (because z). That can be a good approach, but …"* | Weigh up alternatives. |
| *"An x interacts with a y as follows, which is why …"* | Unfold the machinery's internals to explain *why* it behaves as it does. |

If you can imagine reading it **in the bath**, or it's the kind of thing a
friend would say when you ask *"can you tell me more about this?"* over a coffee
— you're in explanation territory.

## Skeleton to adapt

```markdown
# About <topic> / Understanding <topic> / Why <topic> works the way it does

<Open the topic. What question(s) is this reflecting on?>

## <Context / background>
<Why it exists, the history, the constraints, the problem it solves.>

## <How the pieces relate>
<Connections to other parts of the system and to wider ideas.>

## <Choices and alternatives>
<Approaches considered, trade-offs, opinions, when you'd choose differently.>

See also: <how-to guides> for doing it, <reference> for the exact details.
```

Explanation doesn't have to be titled "Explanation." Good alternatives:
*Discussion*, *Background*, *Concepts*, *Topics*, *About …*.

## Self-check

- Does it genuinely illuminate *why*, rather than restate *what* or *how*?
- Can you put "About" in front of the title and have it make sense?
- Is it bounded — has it resisted absorbing full step-by-step instructions or complete specs (which should be linked, not inlined)?
- Does it make connections and, where useful, weigh alternatives and admit a perspective?
- Is it something a user would read *while reflecting*, not while mid-task?

## What this is *not*

If the reader would turn to it *while working* to look something up, it's
**reference**, not explanation — the boundary is easy to slip across. See
[reference.md](reference.md) and the reference-vs-explanation distinction in
[distinctions.md](distinctions.md).
