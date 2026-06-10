# Two types for MCP toolbox host vs reference, not an ADK-style union constructor

Agents on distributed hosts must reference an MCP toolbox by name without
holding its connection params (issue #212). We added a public frozen
`MCPToolboxRef(name, include, strict)` beside the hosting `MCPToolbox` rather
than making one class whose connection-params union accepts an identity arm
(Google ADK's `MCPToolset` shape, which was seriously argued for).

Deciding fact: ADK's union arms are capability-invariant — every arm yields a
client with identical behavior in the same process — while a calfkit union's
arms would flip capability itself (deployable secret-bearing Kafka servant vs
inert lookup key), and those differences surface in public operations:
`add_nodes` must reject one arm, secrets exist in one arm, IDE surface
differs. When a constructor argument changes what an object is allowed to do,
that is a type boundary; the union would make deploy-a-reference
representable and late-failing where two types make it unconstructible. Also
matches the repo's peer-node pattern doc and the cross-framework
handle/servant norm (ActorRef, WorkflowHandle, Stub). Retained from the union
position: `tools=[toolbox]` stays primary via delegation, one docs page, ADK-
shaped host params, and a teaching error when a ref is deployed. Adjudicated
2026-06-10 (independent DX review with both positions briefed neutrally);
spec §8.4 records the summary. No `ref()` method — `select()` mints refs; a
universal `.ref` verb is deferred to any 1.0 all-node generalization.
