# Tutorials — learning-oriented

> A tutorial is an **experience** that takes place under the guidance of a
> tutor. It is always **learning-oriented**.

A tutorial is a lesson. The user learns *by doing* something meaningful, guided
step by step toward an achievable goal. Its purpose is **not** to help them get
a job done — it's to help them *learn*, and to leave them confident that they
can succeed with your product.

The key insight: **what the learner does is not the same as what they learn.**
They build a toy app, but what they actually acquire is familiarity with the
tools, the commands, the workflow, the names of things, and the feeling of using
them. The deliverable is disposable; the learning is the point.

This is why a tutorial is hard. In a real lesson the teacher is present to catch
mistakes. In documentation you are **required to be present but condemned to be
absent** — so the tutorial itself must do all the work of keeping the learner
safe, oriented, and succeeding.

## Key principles

**Don't try to teach.** This is the first rule and the hardest. Your job is to
give the learner an experience through which they learn — not to lecture. The
urge to impart understanding by explaining is strong; giving in to it breaks the
spell. Provide things to *do*; trust that learning follows.

**Show the learner where they're going.** Set expectations at the start so they
can picture the destination and see themselves building toward it. Say *"In this
tutorial we will build and run a small chat server. Along the way we'll meet
WebSockets, async handlers, and the test client."* Do **not** say *"In this
tutorial you will learn…"* — that's presumptuous and a poor pattern.

**Deliver visible results early and often.** The learner is doing strange new
things they don't fully understand. Let them connect cause and effect quickly
and repeatedly. Every step should produce a comprehensible, visible result,
however small.

**Maintain a narrative of the expected.** At every step the learner silently
worries *"did that work?"* Keep reassuring them: *"After a moment, the terminal
prints `Listening on :8000`."* Show actual or exact expected output. Warn of
surprises (*"This will print several hundred lines of logs — that's fine"*), and
where you know the common failure, flag it: *"If you don't see `connected`,
you probably skipped step 2."*

**Point out what they should notice.** Learners are too absorbed in doing to
observe. Close the loops for them: *"Notice the prompt has changed to `(venv)`."*
Observing is an active skill; prompt it.

**Target the feeling of doing, and permit repetition.** Skill becomes pleasure
when action, purpose, and result join up in a confident rhythm. Tie your steps
together so that feeling can form — and make steps repeatable where you can.
Learners genuinely re-run a working step just to see it work again; that
reaffirmation is foundational learning, not waste.

**Ruthlessly minimize explanation.** A tutorial is *not* the place for it. The
learner is focused on doing and getting results; explanation pulls their
attention away and blocks learning. Give the most minimal version — *"We use a
virtual environment so this project's packages stay isolated"* — and link to a
full explanation for when they're ready. Explanation is only pertinent at the
moment the *user* wants it, and that moment is later.

**Focus on the concrete; ignore options and alternatives.** Lead the learner
from *this* action to *this* result. Don't fan out into "you could also…" — every
alternative is cognitive load that dilutes the path. Minds extract general
patterns from concrete cases on their own; your job is the concrete case. A
tutorial follows a single line and offers no choices.

**Aspire to perfect reliability.** Confidence is built layer by layer and is
easily shaken. A learner who follows your steps and doesn't get the promised
result loses confidence in the tutorial, in you, and in themselves. Your
tutorial should be so well-built that it works for every user, every time. You
won't reach that by inspection alone — you reach it by watching real users run
it and fixing what trips them.

## The language of tutorials

| Pattern | Why |
|---|---|
| *"We will…"* / *"Now let's…"* | First-person plural: we're in this together; the learner isn't alone. |
| *"In this tutorial we will build X. Along the way we'll meet Y and Z."* | Sets the destination and what they'll encounter — without "you will learn." |
| *"First, do x. Now do y. Now that y is done, do z."* | Unambiguous, ordered, no room for doubt. |
| *"We use HTTPS here because it's safer (see [About transport security])."* | Minimal inline explanation, linked out. |
| *"The output should look something like: …"* | Clear expectations. |
| *"Notice that… Remember that… Let's check that…"* | Clues that confirm they're on track and prompt observation. |
| *"You've now built a working, authenticated chat server."* | Name and mildly admire what they accomplished. |

## Skeleton to adapt

```markdown
# Build a <small, concrete thing>

In this tutorial we will <build/run X>. Along the way we'll meet <A, B, C>.
You'll need <prerequisites: versions, accounts, install steps stated exactly>.

## Step 1 — <first concrete action with an immediate result>
<Exact command or action.>
You should see:
    <exact expected output>
Notice that <thing to observe>.

## Step 2 — <next action, building on step 1>
...

## What we built
You now have <the result>. <One or two sentences naming what they encountered.>
To go further, see <links to how-to guides and explanation>.
```

## Self-check

- Could a careful beginner follow this end-to-end and succeed **every time**? Have you actually run it?
- Is there a single path, with **no** options, alternatives, or "you could also"?
- Is every explanatory passage either one line or linked out?
- Does every step produce a visible result, with the expected output shown?
- Did you tell them where they're going — without saying "you will learn"?
- Have you removed anything that exists to satisfy *your* urge to teach rather than *their* need to do it?

## What this is *not*

If the reader is already competent and just needs to accomplish a specific task,
that's a **how-to guide**, not a tutorial — see
[how-to-guides.md](how-to-guides.md) and the distinction in
[distinctions.md](distinctions.md). A tutorial is not defined by being "basic":
an advanced topic taught as a guided learning experience is still a tutorial.
