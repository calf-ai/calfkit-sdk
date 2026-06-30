# Examples

## Agent team choreography

**Agent teams** — dynamic multi-agent choreography with `Messaging`, `Handoff`, and runtime discovery:

- **[Internal help desk](examples/help_desk/)** — a front desk that discovers expert teams at runtime (`discover=True`) and either messages them or hands off a task; deploy a new expert and it's reachable next turn, no code change.
- **[Newsroom](examples/newsroom/)** — an editor consults a researcher and a fact-checker (messaging), then hands off to a writer — both peer verbs in one run.
- **[Expense approval](examples/expense_approval/)** — a request climbs a `team_lead` → `director` → `vp` handoff chain until someone is authorized to clear it.
- **[Launch review](examples/launch_review/)** — a release manager fans out to engineering, security, and legal (messaging), then synthesizes a go/no-go itself.

## More examples

- **[Streaming intermediate work](examples/streaming/)** — a trip-planner agent whose preamble, tool calls, and tool results stream to the caller via `handle.stream()`, for a live view of a run's progress.
- **[Agent, tool & consumer](examples/quickstart/)** — a weather agent and its `get_weather` tool deployed as separate services and invoked over the broker, with a consumer node tapping the agent's output stream.
- **[Multi-agent panel](examples/multi_agent_panel/)** — three persona agents (`optimist`, `skeptic`, `pragmatist`) debate over one shared transcript, each automatically seeing the thread from its own point of view.
- **[MCP toolbox](examples/quickstart_mcp/)** — give an agent a live web-`fetch` tool from an MCP server, deployed as its own node and referenced by name — the agent's code never imports it.
