# Quality — the self-review pass

Diátaxis is an approach to *quality* in documentation. It helps to split quality
into two kinds, because they're pursued differently — and because Diátaxis can
only directly help with one of them.

## Functional quality (the constraints)

These are the objective, measurable standards every piece of documentation must
meet:

- **Accuracy / faithfulness** — does every statement conform to the thing it describes? This includes *not inventing what isn't there*: no functions, parameters, return values, flags, errors, config keys, endpoints, or behaviors that the real implementation doesn't have. Documenting a feature that doesn't exist is the most corrosive failure in documentation — users build on it and it collapses under them. **The code is the source of truth** — ground claims and examples in the actual source, not in a spec, ticket, or feature description (which drift from reality); when they disagree with the code, the code wins, and the discrepancy is worth flagging.
- **Completeness** — does it cover what it should?
- **Consistency** — same patterns, terms, structure throughout?
- **Usefulness** — does it actually serve the need?
- **Precision** — exact, unambiguous?

These are **independent** of each other (docs can be accurate but incomplete, or
complete but useless) and any lapse is immediately visible to the user. They
demand discipline and domain skill, and they're never "done" — every release
means re-checking against the code.

**Diátaxis cannot give you functional quality.** It can't make you accurate. But
it *exposes* lapses that were previously hidden: e.g. making reference mirror the
code's structure reveals missing entries; pulling explanation out of a tutorial
reveals a spot where the reader was silently left to figure something out. Use
Diátaxis as the lens that surfaces these — then fix them with ordinary care.

## Deep quality (the liberation)

Beyond functional correctness, excellent documentation has qualities you can't
measure but can recognize:

- *feeling good to use*
- *having flow*
- *fitting human needs*
- *being beautiful*
- *anticipating the user*

These are **subjective** (assessed against the human, not the world),
**interdependent** (flow and anticipating-the-user are aspects of each other),
and **conditional on functional quality** — no one experiences inaccurate or
inconsistent docs as beautiful; the moment they hit a lapse, the spell breaks.
You don't *measure* deep quality, you *judge* it. And users can feel it even when
they can't name it — like clothing that simply *feels good to wear*.

**Diátaxis directly helps create deep quality**, because its four categories are
defined by user needs, so following them helps docs *fit* those needs. And it
protects **flow** by preventing the rhythm-breaking disruptions that happen when,
say, a digression into explanation interrupts a how-to guide. It won't *guarantee*
beauty or replace UX and visual design — but it lays down the conditions that make
deep quality possible.

## The review pass

Run this after drafting, in order — functional first (deep quality depends on it):

**Functional**
- Is every factual claim accurate against the current code/product?
- **Faithfulness:** is every statement — and every *example* — grounded in the real implementation, with nothing invented? Did you verify signatures, parameters, behavior, and names against the actual **code** (the source of truth — not the spec or description, which can be stale)? Where a detail is genuinely unknown, is it marked or omitted rather than filled with a plausible guess?
- Is it complete enough to serve its need (without padding)?
- Is it internally consistent — terms, formatting, structure?

**Type discipline (where Diátaxis does its work)**
- Does each page do **exactly one** of the four jobs? Run the compass on anything that feels off.
- Is anything blurred in from a neighbouring type — explanation inside a tutorial/how-to, instruction or opinion inside reference, steps or full specs inside explanation? Pull it out and **link** instead.
- Does the title honestly say what the page is (e.g. "How to …" for a how-to)?

**Deep quality**
- Does it **flow** — no digressions that break the reader's rhythm or purpose?
- Does it **anticipate the user** — the right thing in reach at the right moment?
- Does each landing page read like an overview, not a bare list?
- Step back: does this *feel good* to use? Would you be happy to read it as the user?

## Remember the working method

You don't have to perfect everything at once. Diátaxis is iterative: choose one
thing, assess it against these standards, make one improvement, consider it
complete, and move on. Documentation is **never finished but always complete** —
ship the improvement now.
