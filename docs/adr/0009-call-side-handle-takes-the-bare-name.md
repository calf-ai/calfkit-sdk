# Call-side handle takes the bare name; the hosting node takes the `Node` suffix

The two MCP toolbox types ([ADR-0002](0002-two-type-toolbox-reference-over-union-ctor.md))
were renamed: the identity-only call-side handle is now `MCPToolbox` (was
`MCPToolboxRef`) and the deployable hosting servant is now `MCPToolboxNode` (was
`MCPToolbox`). This **inverts** the convention recorded implicitly in 0002 and
in the [peer-node pattern doc](../dev/peer-node-pattern.md), where the bare name is
the servant and the `Ref`-suffixed name is the lightweight handle (the Akka
`ActorRef` shape).

Deciding fact: name the common case clean. The handle appears in every agent
declaration (`tools=[...]`) and is constructed by callers constantly; the
servant is constructed once per deployment. Giving the friction-free bare name
to the high-frequency call-side type — and the descriptive suffix to the
once-per-deploy machinery — fits the interface/implementation tradition (the
abstraction callers depend on gets the plain name; the concrete host is
qualified), which is the opposite axis from Akka's. The suffix choice is `Node`
because *every deployable unit in calfkit is a `BaseNodeDef`* (verified: every
`NodeKind` except the call-side `client` is a node), so `MCPToolboxNode` reads as
exactly what it is — "the node that hosts the toolbox" — and pairs with the
existing `ConsumerNode`.

The reference≠servant split from ADR-0002 is unchanged; only which name carries
the suffix differs. Behavior is unchanged: `MCPToolboxNode` keeps `select()` and
`resolve_tools()` (delegating to a freshly-minted `MCPToolbox` handle), so
`tools=[node]` still works. The wire `NodeKind` literal stays `"toolbox"` (a
protocol value, independent of the Python class name). Adjudicated 2026-06-17.
