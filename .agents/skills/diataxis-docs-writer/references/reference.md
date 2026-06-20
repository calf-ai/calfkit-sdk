# Reference — information-oriented

> Reference guides are **technical descriptions** of the machinery and how to
> operate it. Reference material is **information-oriented**.

Reference is the propositional knowledge a user consults *while working* — the
facts they need to do things correctly: accurate, complete, reliable, free of
distraction and interpretation. Where tutorials and how-to guides are led by the
*user's needs*, reference is led by the *product it describes*.

One does not *read* reference; one *consults* it. It is like a map or a marine
chart — it tells you about the territory without your having to go check, and it
is **neutral**: the same chart serves the navigator plotting a course and the
magistrate examining the wreck. Reference doesn't care what you're doing with it.

Users need reference because they need **truth and certainty** — a firm platform
to stand on while they work. That's why it must be authoritative and free of
doubt.

## Key principles

**Describe — and only describe.** Neutral description is the whole job, and it's
unexpectedly hard, because neutral description is *not* how we naturally
communicate. The natural urge is to instruct, explain, opine, reassure. All of
those run counter to reference, which demands accuracy, precision, completeness,
and clarity. When description feels "too thin to be useful" and you're tempted to
add a how-to step or a *why*, resist — and link to the how-to or explanation
instead. Style and form should be **austere, objective, factual**.

**Mirror the structure of the machinery.** A map is usable because its structure
corresponds to the territory. Reference should do the same: its arrangement
should reflect the structure of the product — if a method belongs to a class in
a module, the docs should show that same relationship — so the user can move
through code and docs in parallel. This isn't about forcing an unnatural shape;
it's about letting the code's own logical arrangement organize the docs. (A
useful side effect: mirroring the code's structure makes *gaps* in the docs
obvious.)

**Be consistent; adopt standard patterns.** Reference is useful precisely because
it's predictable. Put what the user needs where they expect it, in a format they
recognize, every time. This is the one place in writing where you should *not*
show off range of vocabulary or style — sameness is a feature. Every parameter
documented with the same fields in the same order; every error in the same shape.

**Provide examples.** A short example of usage illustrates a command or API
concisely and shows its context — without sliding into instruction or
explanation. (Watch the boundary: examples are fun to develop, and an example
that grows to say *why* or *what if* has quietly become explanation. Keep it an
illustration.)

**Auto-generate where you can.** API/class/function reference can often be
generated from the source (docstrings, type signatures, OpenAPI specs), which
keeps it faithfully accurate as the code changes. Prefer this for the mechanical
parts — but note that auto-generated reference is *not* the whole of
documentation, a common and costly assumption.

## The language of reference guides

| Pattern | Why |
|---|---|
| *"`DEFAULT_LOGGING` inherits Python's defaults; it's defined in `django/utils/log.py`."* | State facts about the machinery and its behaviour. |
| *"Sub-commands: `add`, `remove`, `list`, `sync`."* | List commands, options, flags, fields, limits, error messages. |
| *"You must supply `--region`. Do not set `--zone` unless `--region` is also set."* | Warnings and constraints, stated plainly where they apply. |
| Tables of parameters, return values, status codes, config keys | Standard, scannable, consistent structure. |

Reference is allowed to be **boring and unmemorable** — that's a sign you got it
right. If a passage is interesting to *read* rather than to *consult*, it has
probably drifted into explanation.

## Skeleton to adapt

```markdown
# <Name of the thing: command, endpoint, class, config file>

<One-sentence factual statement of what it is.>

## Synopsis / signature
    <exact signature, command form, or request shape>

## Parameters
| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| ...  | ...  | ...      | ...     | <neutral, factual> |

## Returns / responses / fields
<Same consistent treatment.>

## Errors
| Code / name | Meaning |
|-------------|---------|

## Example
    <a short, illustrative usage example — no teaching, no why>

See also: <how-to guides that use this>, <explanation of why it works this way>.
```

## Self-check

- Is it **only** description — no steps, no teaching, no opinion, no persuasion?
- Could the same page serve a user doing the opposite task (is it genuinely neutral)?
- Does its structure mirror the structure of the code/product?
- Is it consistent — same fields, order, and format throughout?
- Is it complete and accurate against the current code? (Auto-generate the mechanical parts where possible.)
- Are examples illustrations only, not smuggled-in explanation?

## What this is *not*

If it tells the user *how to accomplish a task*, it's a **how-to guide**. If it
explains *why* or explores alternatives and trade-offs, it's **explanation** —
and the two are easy to blur. See [explanation.md](explanation.md) and the
reference-vs-explanation distinction in [distinctions.md](distinctions.md).
