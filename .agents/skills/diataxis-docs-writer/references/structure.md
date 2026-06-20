# Organizing a documentation set

Diátaxis is **a guide, not a plan**. You do not start by creating four empty
sections. You write good content in the right mode, and the structure *emerges*.
But once you have enough content — or you're shaping a `/docs` tree or a site —
the four types do tend to become top-level sections, and these patterns help.

> **Don't** create empty `tutorial/`, `how-to/`, `reference/`, `explanation/`
> folders with nothing in them. The framework calls this out specifically: *"It's
> horrible."* Let structure form from improved content, from the inside out.

## The basic structure

For most products with clear boundaries, a simple arrangement works, each section
fronted by a **landing page**:

```text
Home                      <- landing page
    Tutorial              <- landing page
        Part 1
        Part 2
    How-to guides         <- landing page
        Install
        Deploy
        Scale
    Reference             <- landing page
        CLI
        Endpoints
        API
    Explanation           <- landing page
        Security overview
        Performance
        Best-practice notes
```

Add a layer of hierarchy when a section grows — e.g. group several install
guides under *Install*:

```text
    How-to guides         <- landing page
        Install           <- landing page
            Local
            Docker
            Virtual machine
        Deploy
        Scale
```

Match this to the project's existing conventions and toolchain (Sphinx, MkDocs,
Docusaurus, a flat `/docs` folder, the README). Look at how the repo already
organizes docs before imposing a new tree.

## Landing / contents pages

A landing page should **read like an overview**, not just dump a list of links.
You're always writing for a human, not satisfying a scheme. Introduce the
material with short headings and snippets of context:

```text
How-to guides
=============

Practical, goal-oriented directions for getting things done with <product>.

Installation
------------
Get <product> running in your environment.
  • Local installation
  • Docker
  • Virtual machines

Deployment and scaling
----------------------
Take <product> to production and grow it.
  • Deploy an instance
  • Scale your application
```

**The problem of lists:** humans struggle with lists longer than a handful of
items unless they have an inherent order (numeric, alphabetical). **About seven
items is a comfortable ceiling.** If a contents list runs longer, group it into
smaller, titled sub-lists.

## Two-dimensional problems (the hard case)

Sometimes the Diátaxis structure collides with *another* structure — and this is
where people get stuck. Common cases:

- One product used in very different contexts (e.g. on **land, sea, and air**) where a land user never touches the sea workflow.
- Documentation serving distinct **audiences**: end users, developers building on top, and contributors maintaining it.
- One product deployed across different **platforms/clouds**, each with its own commands, APIs, and constraints.

You could nest type-over-topic or topic-over-type:

```text
tutorial/                  for-users-on-land/
    on land                    tutorial/
    at sea          vs.        how-to/
    in the air                 reference/
how-to/                        explanation/
    ...                    at-sea/
                               ...
```

Either way you'll notice repetition and shared material, and there's no single
mechanical answer. The framework's guidance:

1. **Diátaxis isn't four boxes.** It posits four *kinds* of content to structure
   *around* — it does not require exactly four divisions in your hierarchy.
2. **Think user-first.** Document the product *as it is for the user* — in their
   hands and minds — not as its creators conceive it. Ask: is "the product on
   land / sea / air" really three different products for three different users?
   If so, let *that* be the organizing starting point.
3. **Let it be as complex as it needs to be.** If the simple four-section tree
   doesn't fit, that's fine — as long as there *is* arrangement by Diátaxis
   principles and the forms aren't muddled together. Reorder freely (e.g. let
   developer tutorials follow user tutorials, while contributor how-to guides sit
   entirely apart) when that matches how users see the product.

Even complex structures stay navigable when they're logical and built around real
user needs. Diátaxis doesn't remove this problem — but it brings it into focus
and forces you to address it deliberately.

## "Complete, not finished"

Documentation is never *finished* — it keeps adapting to the product and to
users. But at every stage it can be *complete*: useful, appropriate to its
current stage, structurally healthy, ready to grow. Like a plant at every stage
of growth, nothing is missing even though it will keep developing. Ship the small
improvement now rather than hoarding a big rewrite.
