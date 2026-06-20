# How-to guides — goal-oriented

> How-to guides are **directions** that guide the reader through a problem
> toward a result. They are **goal-oriented**.

A how-to guide helps an already-competent user get something done. It serves
their *work*, not their study — it navigates them across a real-world
problem-field from one side to the other.

Examples: *How to configure reconnection back-off*, *How to use fixtures in
pytest*, *How to roll back a failed migration*. By contrast, *How to build a web
application* is **not** a how-to guide — it's an open-ended sphere of skill, not
a specific goal.

A good list of how-to guides also advertises what your product can *do*; it's
often the most-read part of the docs.

## Key principles

**Address a real-world goal, written from the user's perspective — not the
machinery's.** Every how-to should answer to a human project. The common failure
is to define guides by *operations the tool can perform* rather than *things the
user needs to accomplish*. Compare:

- ❌ "To deploy the configuration, select the options and press **Deploy**." — describes the machine's motions; tells a competent user nothing they didn't know.
- ✅ "To deploy a read-replica that survives a zone outage, set …" — answers a real need, with the judgement the user actually lacks.

Tools appear in how-to guides as incidental bit-players — the means to the
user's end. A single guide often cuts across several tools, joined by what a
person needs to get done.

**Assume competence.** The user knows roughly what they want and can follow your
instructions. You don't need to tell them what a power switch is. Don't teach.

**Address real-world complexity; be adaptable.** A guide useful only for the
*exact* narrow case you imagined is rarely valuable. Real problems fork. Use
**conditional imperatives** so users can adapt: *"If you're on Postgres, do x; if
on MySQL, do y instead."* A how-to may have multiple entry and exit points and
may ask the user to apply judgement.

**Omit the unnecessary — usability over completeness.** Unlike a tutorial, a
how-to need not be end-to-end complete. Start and end at reasonable, meaningful
points and let the user join it to their own work. Resist the urge to explain or
to list every option "for completeness" — if it matters, link to the explanation
or reference.

**Describe a logical sequence, and seek flow.** The steps are ordered actions —
including thinking and judgement, not just keystrokes. Sometimes the order is
forced (step 2 needs step 1); sometimes you choose it because one step sets up
the user's environment or thinking for the next. Ground the sequence in how the
user actually works: minimize context-switching, don't make them hold a thought
open for ten steps, don't bounce them back to earlier concerns needlessly. At its
best a how-to *anticipates* the user — like a helper holding out the next tool
before they reach for it.

**Name it for exactly what it does.** Titles matter (to humans and search engines):

- ✅ *How to integrate application performance monitoring*
- ⚠️ *Integrating application performance monitoring* (maybe it's about whether to, not how)
- ❌ *Application performance monitoring* (what? why? how? a reference?)

## The language of how-to guides

| Pattern | Why |
|---|---|
| *"This guide shows you how to …"* | States the problem/task the guide solves. |
| *"If you want x, do y. To achieve w, do z."* | Conditional imperatives — adaptable to the user's case. |
| *"For the full list of options, see the [X reference]."* | Don't pollute the guide with completeness; link out. |
| *"You'll usually want to … but if …, instead …"* | Forks and judgement, openly acknowledged. |

## Skeleton to adapt

```markdown
# How to <accomplish a specific real-world goal>

This guide shows you how to <goal>. It assumes you already have <competence /
prerequisites>, stated briefly.

## Steps
1. <Action, framed by purpose.> If <condition>, <variation> instead.
2. <Next action.>
3. <...ending at a reasonable, meaningful stopping point.>

## Variations / troubleshooting (only if real)
- If <common real-world situation>, <how to adapt>.

See also: <reference for full options>, <explanation for why>.
```

## Self-check

- Does the title name a **specific goal** ("How to …"), not a tool or an open-ended area?
- Is it framed by what a *user* needs to accomplish, not by what the *machine* can do?
- Does it assume competence — no teaching, no power-switch-level instructions?
- Does it omit the unnecessary, linking out explanation and full option lists rather than inlining them?
- Does it handle real-world forks with conditional imperatives instead of pretending one rigid path fits all?
- Does the sequence *flow* — minimal context-switching, sensible order, no needless backtracking?

## What this is *not*

If the reader is *learning* rather than *working* — needs a safe, guided,
single-path experience — that's a **tutorial** (see [tutorials.md](tutorials.md)
and [distinctions.md](distinctions.md)). The difference is study vs. work, not
basic vs. advanced: a how-to can cover something basic, a tutorial something
advanced.
