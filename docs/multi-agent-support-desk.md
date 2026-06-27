# Build a multi-agent support desk

In this tutorial we will build and run a three-agent support desk: a `triage`
agent that **messages** a `billing` agent and **hands off** to a `refunds`
specialist. Along the way we'll meet `peers`, `Messaging`, `Handoff`, and watch a
single request travel between live agents that find each other at runtime.

You'll need:

- Python 3.10 or later, with calfkit installed: `pip install calfkit`.
- Docker, to run a local broker.
- An OpenAI API key.

## Step 1 — Start a broker

Every calfkit deployment talks over a broker. Start a local one with Docker:

```console
$ git clone https://github.com/calf-ai/calfkit-broker && cd calfkit-broker && make dev-up
```

This brings up a broker at `localhost:9092`. Leave it running, and open a new
terminal to continue. Set your OpenAI key there — the agents call a model:

```console
$ export OPENAI_API_KEY=sk-...
```

## Step 2 — Build and run the two specialists

Create `support_desk.py` with the two specialist agents. They're plain agents;
each gets a `description` so other agents can discover what it does.

```python
# support_desk.py
from calfkit import Agent
from calfkit.providers import OpenAIResponsesModelClient


def model():
    return OpenAIResponsesModelClient(model_name="gpt-5.4-nano")


billing = Agent(
    "billing",
    description="Answers account balance and billing questions.",
    system_prompt="You are the billing department. Answer account balance and billing questions concisely.",
    subscribe_topics="billing.input",
    model_client=model(),
)

refunds = Agent(
    "refunds",
    description="Handles refund requests.",
    system_prompt="You are the refunds department. Approve a reasonable refund request and state the decision concisely.",
    subscribe_topics="refunds.input",
    model_client=model(),
)
```

Run each specialist in its own terminal:

```console
$ ck run support_desk:billing
```

```console
$ ck run support_desk:refunds
```

Each prints its startup logs and then waits, serving. Notice that nothing connects
the two — each is an independent service.

## Step 3 — Add the triage agent with peers

Add a third agent to `support_desk.py`. This one declares `peers`: it may
**message** billing (consult and keep control) and **hand off** to refunds
(transfer the conversation).

```python
from calfkit import Messaging, Handoff   # add to the imports at the top

triage = Agent(
    "triage",
    description="Front desk that routes customer requests.",
    system_prompt=(
        "You are the front desk for a support team. Route each customer request: for an account "
        "balance or billing question, use the message_agent tool to ask the billing agent and relay "
        "its answer; for a refund request, hand off the conversation to the refunds agent."
    ),
    subscribe_topics="triage.input",
    model_client=model(),
    peers=[Messaging("billing"), Handoff("refunds")],
)
```

Run it in a third terminal:

```console
$ ck run support_desk:triage
```

triage starts, finds billing and refunds on the live directory, and waits for
requests. Notice we never gave triage their addresses — it discovered them by
name at runtime.

## Step 4 — Ask a billing question

Create `ask.py` to send a request to triage:

```python
# ask.py
import asyncio
from calfkit import Client


async def main():
    client = Client.connect("localhost:9092")
    result = await client.agent("triage").execute("What's the balance on my account?")
    print(result.output)
    await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
```

Run it:

```console
$ python ask.py
```

You'll see an answer about your balance. Notice that triage answered you: behind
the scenes it **messaged** billing, folded billing's reply into its own turn, and
stayed in control.

## Step 5 — Ask for a refund

Change the request in `ask.py` to a refund:

```python
    result = await client.agent("triage").execute("I'd like a refund for order 1234.")
```

Run it again:

```console
$ python ask.py
```

This time the answer comes from the refunds specialist. Notice the difference:
triage **handed off** the conversation to refunds, which answered you directly —
triage stepped out.

## What we built

You now have a working mesh of three live agents that find each other by name and
collaborate two ways: triage **consults** billing and keeps control, then
**transfers** a refund to the specialist who answers in its place. The agents were
never wired together in code — each declared its own reach with `peers`, and the
rest happened at runtime.

To go further:

- [How to let agents find and reach each other at runtime](agent-peers.md) — the full recipe: open (`discover`) scope, making an agent discoverable, what happens when a peer is offline, and handling peer faults.
- [Agent-mesh design spec](designs/agent-mesh-spec.md) — why messaging and handoff work the way they do.
