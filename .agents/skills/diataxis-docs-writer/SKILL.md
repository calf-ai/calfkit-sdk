---
name: diataxis-docs-writer
description: >-
  Write or improve software documentation using the Diátaxis framework — four
  documentation types (tutorials, how-to guides, reference, explanation), each
  serving a different user need. Use it whenever you're about to write or
  revise documentation for a finished feature, implementation, API, library,
  CLI, service, or project — READMEs, /docs pages, guides, API/config
  reference, conceptual overviews — even when the user just says "document
  this" or "write docs." Especially reach for it right after a feature or PR
  lands and documentation is the next step, and whenever asked to organize,
  audit, or improve an existing doc set. It helps you pick the right kind of
  doc, write each in its correct mode, ground every claim in the actual code,
  and avoid blurring the four types — the most common documentation failure.
  Do not use it for in-code docstrings or type hints, debugging, summarizing
  text, code review, proposing or designing something new (e.g. an RFC), or
  looking up a library's existing docs.
---

# Diátaxis documentation

Good documentation is not one thing. It is **four** things, each answering a
different question a user has, each written in a different way. Most bad
documentation is bad not because the writing is poor but because two of these
four got mixed together — a tutorial clogged with explanation, a reference page
that wanders into opinion, a "how-to" that's really a half-finished lesson.

Your job with this skill is to keep them separate. Decide which kind you're
writing, commit to its discipline, and link out to the others rather than
bleeding into them.

> Diátaxis is a **guide, not a plan**. You don't start by building four empty
> sections and filling them in. You write each piece well, in its proper mode,
> and good structure emerges from that.

---

## The two axes (why there are exactly four)

Every craft involves two distinctions. Documentation must serve both sides of each:

- **Action ↔ Cognition** — is the content about *doing* (practical steps) or about *knowing* (theoretical facts and ideas)?
- **Acquisition ↔ Application** — does it serve the user *at study* (learning the craft) or *at work* (applying it to get something done)?

Cross those two axes and you get four quadrants — the four documentation types.
There can't be three or five; the two axes define the whole territory.

---

## Step 1 — Classify before you write (the compass)

Before writing a sentence, decide which type you're producing. Ask just two questions:

1. **Action or cognition?** (Is this about what the user *does*, or what they *know*?)
2. **Acquisition or application?** (Is the user *studying*, or *working*?)

| If the content informs… | …and serves the user's… | …then it is a… | It answers | Its form is |
|---|---|---|---|---|
| **action** | **acquisition** (study) | **Tutorial** | "Can you teach me to…?" | a lesson |
| **action** | **application** (work) | **How-to guide** | "How do I…?" | a recipe |
| **cognition** | **application** (work) | **Reference** | "What is…? / What are the options?" | dry description |
| **cognition** | **acquisition** (study) | **Explanation** | "Why…? / Tell me about…" | a discursive article |

Use the terms loosely at first — the point is to force a decision, not to win a
naming argument. Apply the questions at any zoom level: a whole document, a
section, or a single sentence that feels out of place.

**Documenting a finished feature usually needs more than one type.** A new API
endpoint might want a *reference* entry (the parameters and return values), a
*how-to* (accomplishing a real task with it), and maybe an *explanation* (why it
works the way it does). Don't try to serve all of these in one page. Identify
which types the feature actually needs right now, and write each separately.
Skip the types it doesn't need yet — better a complete how-to than four thin,
half-blurred pages.

When the type genuinely isn't obvious, or you suspect intuition is misleading
you, read **[references/distinctions.md](references/distinctions.md)**.

---

## Step 2 — Write to the type's discipline

Once you know the type, open its reference file and follow it. Each gives you
the principles, the sentence-level language patterns, a worked software example,
a skeleton to adapt, and a self-check. Read the one you need — don't work from
the summary below alone.

| Type | One-line discipline | Read |
|---|---|---|
| **Tutorial** | A lesson. Learning-oriented. The user learns *by doing*, on a single safe path you guarantee works. Ruthlessly minimize explanation. | [references/tutorials.md](references/tutorials.md) |
| **How-to guide** | Directions to a real-world goal. Goal-oriented. Assumes competence. Can branch. Omit the unnecessary. Title it "How to …". | [references/how-to-guides.md](references/how-to-guides.md) |
| **Reference** | Neutral technical description. Information-oriented. Austere, consistent, mirrors the structure of the code. Describe and *only* describe. | [references/reference.md](references/reference.md) |
| **Explanation** | Discursive discussion. Understanding-oriented. Answers *why*. Gives context, alternatives, opinions. Read away from the keyboard. | [references/explanation.md](references/explanation.md) |

### The cardinal rule: don't blur the types

Each type has a natural pull toward its neighbours, and resisting that pull is
most of the craft:

- Writing a **tutorial** or **how-to**, you'll feel the urge to *explain why*. Resist it — give the one-line version and link to an explanation.
- Writing **reference**, you'll feel the urge to *instruct* or *editorialize*. Resist it — link to a how-to or explanation.
- Writing **explanation**, you'll feel the urge to *fold in steps or full specs*. Resist it — they live in the how-to and reference.

The fix is almost always the same: **say the minimum, then link to the right
place.** A one-line "We use HTTPS here because it's more secure (see *About
transport security*)" keeps a tutorial on track far better than a paragraph.

The two confusions worth knowing cold — because they cause the most damage — are
**tutorial vs. how-to** (study vs. work) and **reference vs. explanation**
(consult-while-working vs. read-while-reflecting). Both are covered in
[references/distinctions.md](references/distinctions.md).

### Write only what's true — the code is the source of truth

Documentation is something users *trust and act on*, so an invented or stale
detail is worse than a missing one — it sends people confidently toward something
that isn't there. **Never document an implementation that doesn't exist.**

When your docs describe code — a function, its parameters, return values, flags,
errors, a config key, an endpoint — **verify every statement against the actual
code.** The code is the only source of truth that can be fully trusted. Specs,
tickets, design docs, the feature description you were handed, and even the
existing documentation can all be out of date or have drifted from reality:
implementations deviate when problems are hit mid-build, and written intentions
are rarely updated to match. So read the real signature, the real schema, the
real behavior in the source. When a description and the code disagree, **trust
the code** — and flag the discrepancy so a human can reconcile it. If all you
have is a description and you can't see the code, treat the description as an
*unverified claim*: document what you can ground, and clearly mark what you
couldn't verify rather than presenting it as fact.

This bites hardest in **examples**, which are tempting to embellish. An example
must demonstrate *real, verified* behavior — don't reach for an input that
asserts behavior nobody confirmed: how a function handles Unicode or empty input,
what a config does at its limits, a package's install command or name, an error a
call might raise. If a detail matters but you can't verify it against the code,
say so plainly ("behavior for X is unspecified") or leave it out — never paper
over the gap with a plausible-sounding guess. A confident hallucination is the
most damaging thing documentation can contain. Accuracy is the first obligation
of *functional quality*; see [references/quality.md](references/quality.md).

---

## Working mode: authoring vs. improving

**Authoring** new docs for a feature you (or the user) just built:

1. **Ground yourself in the real implementation first.** Read the actual code — the source, signatures, config schema, or API surface you're about to document. The code is the source of truth; don't work from the spec, the ticket, or the feature description alone, since those drift from what was actually built. You cannot write truthful docs for code you haven't looked at, and a plausible guess that turns out wrong is worse than no doc at all (see *Write only what's true* above).
2. List the user *needs* the feature creates — "learn it," "do task X with it," "look up its options," "understand why it's designed this way." Map each to a type via the compass.
3. Write each piece in its proper mode, reading the relevant reference file.
4. Match the project's existing docs conventions — file layout, format, tone. Default to Markdown if there's nothing to match. Look at how the repo already documents things before inventing a structure.
5. Run the per-type self-check, then the quality pass ([references/quality.md](references/quality.md)).

**Improving** an existing doc set — don't rewrite it top-down. Diátaxis works
iteratively, and improvements cascade naturally from small changes:

1. **Choose something** — a page, a section, even a single paragraph. Don't hunt for the worst problem; start with what's in front of you.
2. **Assess it** against the standards: *What user need does this serve? How well? Does its language and logic match its mode? What's blurred in from another type?*
3. **Decide one change** that produces an immediate improvement.
4. **Do it**, and consider it complete. Then repeat.

Resist two temptations when improving: tearing it all down to start over, and
creating empty `tutorial/`, `how-to/`, `reference/`, `explanation/` folders to
"fix the structure." Structure should emerge from improved content, not be
imposed on top of it.

---

## Organizing a documentation set

When you're shaping more than a single page — a `/docs` tree, a landing page, a
whole site — the four types become top-level sections, but the real world is
often more complex (multiple user types, platforms, or topic areas). For how to
structure landing pages, hierarchies, and these two-dimensional cases, read
**[references/structure.md](references/structure.md)**.

---

## Before you call it done

Read **[references/quality.md](references/quality.md)** for the self-review pass.
The short version: first secure *functional quality* (accuracy, completeness,
consistency) — Diátaxis can't give you these but it exposes where they're
missing — then check for *deep quality*: does each page do exactly one job, in
the right voice, with nothing blurred in from a neighbour, in a way that flows?

## Reference files

- [references/tutorials.md](references/tutorials.md) — learning-oriented lessons
- [references/how-to-guides.md](references/how-to-guides.md) — goal-oriented directions
- [references/reference.md](references/reference.md) — information-oriented description
- [references/explanation.md](references/explanation.md) — understanding-oriented discussion
- [references/distinctions.md](references/distinctions.md) — the compass in depth + the two confusions
- [references/structure.md](references/structure.md) — organizing a doc set, landing pages, complex hierarchies
- [references/quality.md](references/quality.md) — functional vs. deep quality, the self-review pass
