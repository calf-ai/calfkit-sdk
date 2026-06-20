# The distinctions — using the compass when intuition fails

The map (the four-quadrant diagram) accords with intuition and is great for
*remembering* the four types. But intuition isn't always reliable, and sometimes
it hands you a confident answer that's wrong. The **compass** is the
course-correction tool for exactly those moments.

## The compass

When you're unsure what a piece of content is — or should be — reduce the
two-dimensional question to two one-dimensional ones:

1. **Action or cognition?** — practical steps and doing, or theoretical/propositional knowledge and thinking?
2. **Acquisition or application?** — the user's *study* (learning), or their *work* (getting something done)?

| If the content informs… | …and serves the user's… | …then it must be a… |
|---|---|---|
| action | acquisition | tutorial |
| action | application | how-to guide |
| cognition | application | reference |
| cognition | acquisition | explanation |

Use the terms flexibly — *action/cognition* ≈ doing/thinking, *acquisition/
application* ≈ study/work. Phrase the questions however helps:

- *Do I think I'm writing for x or y?*
- *Is the writing in front of me doing x or y?*
- *Does the user need x or y right now?*

And apply them at any zoom: a whole document, a section, or a single sentence
that feels out of place. The compass is most valuable precisely when you *think*
you know what you're doing but feel a nagging doubt — it forces you to stop and
re-check.

---

## Confusion 1: tutorial vs. how-to guide (the big one)

This is the **single most common conflation** in software documentation, and one
of the most harmful — because a broken tutorial fails exactly the newcomers you
most want to keep.

### Why they're easy to confuse

They genuinely have a lot in common. Both are **practical** — they contain
directions to follow. Both promise that if you follow the steps you'll reach a
successful conclusion. Both are ordered sequences of actions. Neither makes sense
to someone without their hands on the machinery. Close relations, easily mistaken
for one another.

### What actually separates them: study vs. work

| Tutorial (study) | How-to guide (work) |
|---|---|
| Helps the user **acquire competence** | Helps an **already-competent** user perform a task |
| Provides a **learning experience** — what matters is what the learner *does* and experiences | **Directs work** toward a result |
| Follows a **single, carefully-managed path** with required *encounters* | Aims at a result; **the path can't be managed** — it's the real world |
| **Familiarizes** the user with tools, terms, behaviour | **Assumes familiarity** with all of it |
| Takes place in a **contrived, safe environment** | Applies in the **real world**, with whatever it throws at you |
| **Eliminates the unexpected** | **Prepares for the unexpected** |
| **No choices or alternatives** — a single line | **Forks and branches** — "if this, then that" |
| **Must be safe**; you can always start over | **Can't promise safety**; sometimes one chance to get it right |
| **Responsibility lies with the teacher** (you) | **Responsibility lies with the user** |
| Is **explicit about basic things** (where to type, how long to wait) | Relies on those as **implicit knowledge** |
| **Concrete and particular** — the specific things set before the learner | **General** — many specifics are unknowable in advance |

All of these flow from one root distinction: **is the user at study, or at
work?** That's the question to ask.

### It is *not* basic vs. advanced

A tempting but wrong shortcut. How-to guides can and should cover *basic*
procedures (a checklist for filing the right paperwork is a how-to). And
tutorials can be *advanced* (an expert anaesthetist taking a hands-on course on
difficult intubations is still in a tutorial). The difference is the *need being
served* — study or work — not the difficulty.

### Why it matters

In a field like medicine, a manual that tried to *teach* while guiding a live
procedure would be deadly. Software rarely kills anyone, so we get away with the
conflation — but we still inflict steady low-level frustration, and we lose
users, every time we ship a "tutorial" that's secretly a how-to (or vice versa)
and so serves neither need well.

---

## Confusion 2: reference vs. explanation

Both live on the **cognition** (theory) half of the map — neither contains steps.
The difference is again **application vs. acquisition**: reference serves the user
*at work*, explanation serves the user *at study*.

### Rules of thumb (intuition, which is *mostly* enough)

- **If it's boring and unmemorable**, it's probably *reference*.
- **Lists and tables** (classes, methods, parameters, flags) are almost always *reference*.
- **If you can imagine reading it in the bath**, it's probably *explanation*.
- If it's the answer to *"can you tell me more about X?"* over a drink — *explanation*.

### When intuition isn't enough

It's easy to slip from one to the other, usually when reference material "starts
to become expansive." You add an illustrative example (fine), then grow the
example to say *why*, or *what if*, or how it came to be — and you've quietly
written explanation inside your reference. That's bad for both: the reference is
interrupted and obscured, and the explanation can't develop properly where it
sits.

### The real test

> Is this something the user would turn to **while working** — actually getting a
> task done? Then it's **reference**. Or is it something they'd reach for once
> they've **stepped away to think** about it? Then it's **explanation**.

Two genuinely different needs, reflecting whether the reader stands in a relation
of *work* or *study* to the craft at that moment.

---

## General point: blur causes structural rot

Each type has a natural affinity with its neighbours, and a natural tendency to
blur into them. When that's allowed, writing style and content migrate into the
wrong places; in the worst case tutorials and how-to guides collapse into each
other and neither need is met. Catching blur early — with the compass — is most
of what keeps documentation healthy.
