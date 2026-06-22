"""Topic provisioning (EXPERIMENTAL, opt-in) — create Kafka topics up front.

calfkit does not create topics by default; it relies on broker-side
auto-creation. On brokers where auto-create is **disabled** (hardened Kafka /
Redpanda, or Kafka-compatible brokers like Tansu), producers and consumers
silently stall on a topic that doesn't exist. The opt-in provisioner creates the
topics your nodes reference instead. It is a dev/CI convenience — review
partitions / replication-factor / ACLs before relying on it in production, where
topic creation is typically ops-governed.

This example exercises the provisioning surface against a realistic topology
(an agent + its tool + a consumer):

* a ``ProvisioningConfig`` with non-default partition / replication / topic-config
  / timeout knobs,
* ``topics_for_nodes`` — inspect exactly which topics a node set references
  WITHOUT contacting the broker. Note the agent contributes its **tool's input
  topic** (``tool.get_weather.input``) on top of its own inboxes, its publish
  topic, and every node's framework return inbox,
* programmatic creation with ``TopicProvisioner`` + reading the
  ``ProvisionReport`` (created / existing / unauthorized) and handling
  ``TopicProvisioningError``,
* idempotency — a second pass reports topics as *existing*, not created.

The common path — let a ``Worker`` provision on startup (no manual call)::

    client = Client.connect("localhost:9092", provisioning=PROVISIONING)
    worker = Worker(client, nodes=NODES)
    await worker.run()  # eagerly provisions NODES' topics, then serves

Requires a Kafka broker at ``localhost:9092`` and an ``OPENAI_API_KEY`` (the
agent's model client is constructed at import, as in the other agent examples).
Run::

    python topic_provisioning.py

The same one-off provisioning is available from the CLI (no Python)::

    ck topics provision --nodes topic_provisioning:NODES \\
        --bootstrap-servers localhost:9092 --partitions 3
"""

import asyncio

from calfkit.client import InvocationResult
from calfkit.nodes import Agent, agent_tool, consumer
from calfkit.providers import OpenAIResponsesModelClient
from calfkit.provisioning import (
    ProvisioningConfig,
    ProvisionReport,
    TopicProvisioner,
    TopicProvisioningError,
    topics_for_nodes,
)

BOOTSTRAP = "localhost:9092"


@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location."""
    return f"It's sunny in {location}"


agent = Agent(
    "weather_agent",
    system_prompt="You are a helpful assistant.",
    subscribe_topics="weather_agent.input",
    publish_topic="weather_agent.output",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
    tools=[get_weather],  # the agent publishes tool Calls to get_weather's inbox
)


@consumer(subscribe_topics="weather_agent.output")
async def log_results(result: InvocationResult) -> None:
    print(result.output)


# The nodes a deployment would serve. Exposed at module scope so the CLI can
# resolve them too: ``ck topics provision --nodes topic_provisioning:NODES``.
NODES = [agent, get_weather, log_results]

# A richer-than-default config. The defaults (partitions=1, rf=1, no configs) are
# fine for local/CI; tune these for real clusters.
PROVISIONING = ProvisioningConfig(
    enabled=True,  # master switch (off by default)
    num_partitions=3,  # raise to scale consumer-group parallelism on hot topics
    replication_factor=1,  # dev only — rf=1 is NOT durable; use >= 3 in prod
    topic_configs={  # applied to DATA topics only, never framework return inboxes
        "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # keep records 7 days
        "cleanup.policy": "delete",
    },
    create_timeout_ms=30_000,  # whole-operation budget (connect + create + retry)
)


async def provision(label: str) -> ProvisionReport:
    """Create every topic ``NODES`` reference — e.g. as a deploy / CI step."""
    # 1. See what will be created — pure computation, no broker contact.
    topics = topics_for_nodes(NODES)
    print(f"\n[{label}] {len(topics)} topic(s) referenced by these nodes:")
    for topic in topics:
        print(f"  - {topic}")

    # 2. A node's framework return inbox ("<id>.private.return") carries
    #    correlation-keyed RPC traffic, so user ``topic_configs`` (retention /
    #    compaction) must NOT apply to it. The Worker and the CLI compute this
    #    set for you; it is shown here to make the lower-level API explicit.
    framework_topics = {node._return_topic for node in NODES}

    # 3. ``from_connection`` builds the provisioner the same way the Worker and
    #    Client do internally (it also splits any ``security=`` object out of the
    #    connection kwargs for you).
    provisioner = TopicProvisioner.from_connection(server_urls=BOOTSTRAP, config=PROVISIONING)
    try:
        report = await provisioner.provision(topics, framework_topics=framework_topics)
    except TopicProvisioningError as exc:
        # Hard, non-retriable failure (e.g. replication_factor > broker count).
        # The error names the offending topic + Kafka error code.
        print(f"[{label}] provisioning failed for {exc.topic!r} (code={exc.code}): {exc}")
        raise

    print(f"[{label}] created={report.created} existing={report.existing} unauthorized={report.unauthorized}")
    return report


async def main() -> None:
    # First pass creates the topics; the second is idempotent (create-if-absent),
    # reporting everything as ``existing`` rather than ``created``.
    await provision("first run")
    await provision("second run")


if __name__ == "__main__":
    asyncio.run(main())
