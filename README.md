# metabase-diagnostics
Diagnostic Tools for Troubleshooting Metabase

## Plan for @claude
- [ ] 1. Update the metabase-diagnostics/README.md with our specific goals and progress
- [ ] 2. Create diagnostic tool templates that I can reference and build upon
- [ ] 3. Document common troubleshooting patterns you encounter
- [ ] 4. Maintain a running TODO in the diagnostics directory for our ongoing work

## Tooling ideas for @claude
  For Metabase Diagnostics & Troubleshooting

  Clojure/LISP Best Practices:
  - Babashka for fast CLI tools (already noted in your project) - excellent for system diagnostics
  - REPL-driven development for interactive troubleshooting sessions
  - Spec/Malli for data validation in diagnostic tools
  - Component/Mount for stateful diagnostic services
  - Timbre/tools.logging for structured diagnostic logging

  Data Infrastructure Diagnostics:
  - Query performance analysis - connection pooling, query plan analysis
  - Connection health monitoring - database driver diagnostics
  - Data pipeline observability - sync job monitoring, transformation validation
  - Cache effectiveness - Redis/in-memory cache hit rates

  Cloud Infrastructure Monitoring:
  - Prometheus/Grafana integration for Metabase metrics
  - OpenTelemetry for distributed tracing across services
  - Health check endpoints for orchestration platforms
  - Resource utilization tracking (JVM heap, connection pools)

  Architectural Approaches Recommend

  Pre-Scale (Startup/Small Team):
  - Simple, composable tools over complex frameworks
  - File-based configuration and state when possible
  - Direct database queries for diagnostics
  - Monolithic diagnostic tool with clear separation of concerns

  At Scale (Enterprise/High Volume):
  - Event-driven diagnostic systems
  - Distributed tracing and observability
  - Infrastructure as Code for consistent environments
  - Multi-tenant diagnostic isolation

  Data Engineering Paradigms to Apply

  - Functional data transformations over imperative scripts
  - Schema-first diagnostic data models
  - Immutable data structures for reliable analysis
  - Stream processing for real-time diagnostics where needed

  When asked questions or make requests:
  1. Identify the scale/context you're operating in
  2. Recommend appropriate tools from the Clojure/data ecosystem
  3. Suggest architectural patterns that fit your constraints
  4. Provide concrete implementation approaches with code examples
  5. Consider operational concerns (monitoring, deployment, maintenance)


## MCP Servers TODO

### Slack MCP Server
- [ ] Add Slack MCP server to Claude Code
  - **Official MCP**: [korotovsky/slack-mcp-server](https://github.com/korotovsky/slack-mcp-server)
  - **Claude Code Example**: [How to Use Slack MCP Server](https://apidog.com/blog/slack-mcp-server/)
  - **Capabilities**: Send messages, manage channels, access workspace history, add reactions, reply to threads, retrieve user profiles. Supports stealth mode (no permissions), OAuth authentication, enterprise workspaces, and batch processing up to 50 messages.

### Linear MCP Server
- [ ] Add Linear MCP server to Claude Code
  - **Official MCP**: [Linear Official MCP Server](https://linear.app/docs/mcp)
  - **Claude Code Example**: [Setup Linear MCP in Claude Code](https://composio.dev/blog/how-to-set-up-linear-mcp-in-claude-code-to-automate-issue-tracking)
  - **Capabilities**: Create, update, delete issues and subissues, advanced search with filters, project management, team and label management, comment handling with markdown support. Uses OAuth 2.1 authentication with SSE/HTTP transports.

### GitHub MCP Server
- [ ] Add GitHub MCP server to Claude Code
  - **Official MCP**: [github/github-mcp-server](https://github.com/github/github-mcp-server)
  - **Claude Code Example**: [GitHub MCP Server Guide](https://github.blog/ai-and-ml/generative-ai/a-practical-guide-on-how-to-use-the-github-mcp-server/)
  - **Capabilities**: Repository management, browse/query code, search files, analyze commits, issue & PR automation, CI/CD workflow intelligence, code analysis, security findings review. Supports OAuth authentication with remote hosting.

### Gmail MCP Server
- [ ] Add Gmail MCP server to Claude Code
  - **Official MCP**: [GongRzhe/Gmail-MCP-Server](https://github.com/GongRzhe/Gmail-MCP-Server)
  - **Claude Code Example**: [Create Gmail Agent with MCP](https://medium.com/@jason.summer/create-a-gmail-agent-with-model-context-protocol-mcp-061059c07777)
  - **Capabilities**: Send emails with attachments, label management, batch processing up to 50 emails, search/read/delete emails, draft management, multi-account support. Requires Google Cloud Project with Gmail API and OAuth2 credentials.

### Google Calendar MCP Server
- [ ] Add Google Calendar MCP server to Claude Code
  - **Official MCP**: [nspady/google-calendar-mcp](https://github.com/nspady/google-calendar-mcp)
  - **Claude Code Example**: [Google Calendar MCP Integration](https://n8n.io/workflows/3569-build-an-mcp-server-with-google-calendar/)
  - **Capabilities**: Multi-calendar support, create/update/delete events, recurring events, free/busy queries, smart scheduling with natural language, conflict detection, timezone support. Installation via `npx @cocal/google-calendar-mcp`.

### Metabase Website MCP Server
- [ ] Add Metabase MCP server to Claude Code
  - **Official MCP**: [hyeongjun-dev/metabase-mcp-server](https://github.com/hyeongjun-dev/metabase-mcp-server)
  - **Claude Code Example**: [Metabase MCP Server](https://mcpmarket.com/server/metabase-1)
  - **Capabilities**: Access dashboards, questions/cards, databases as resources, execute queries, manage collections, comprehensive logging, support for session-based and API key authentication. Provides 70+ ready-to-use tools for analytics and BI.

### Grain MCP Server
- [ ] Add Grain MCP server to Claude Code
  - **Official MCP**: [Grain MCP Server](https://mcp.pipedream.com/app/grain)
  - **Claude Code Example**: [Weekly Pulse: Screen Recording](https://www.pulsemcp.com/posts/newsletter-remote-mcp-images-screen-recording)
  - **Capabilities**: Meeting recording with automated note-taking, transcript generation, intelligence notes, integration with Slack/HubSpot, customizable recording views (speaker-only or all participants), screen sharing modes (overlap/side-by-side).

### Docker MCP Server
- [ ] Add Docker MCP server to Claude Code
  - **Official MCP**: [Docker Hub MCP Server](https://www.docker.com/blog/introducing-docker-hub-mcp-server/)
  - **Claude Code Example**: [Build MCP Servers with Docker](https://www.docker.com/blog/build-to-prod-mcp-servers-with-docker/)
  - **Capabilities**: Container management with natural language, Docker Hub API integration, secure sandboxed isolation, digitally signed images, one-click setup for popular clients. Supports Docker Desktop integration and containerized MCP server deployment.

### Postgres MCP Server
- [ ] Add Postgres MCP server to Claude Code
  - **Official MCP**: [crystaldba/postgres-mcp](https://github.com/crystaldba/postgres-mcp)
  - **Claude Code Example**: [MCP with Postgres](https://punits.dev/blog/mcp-with-postgres/)
  - **Capabilities**: Read/write database access, schema inspection, performance analysis, workload analysis, query optimization recommendations, health checks (buffer cache, connections, constraints), execution plan analysis. Professional version includes transaction management.

### MongoDB MCP Server
- [ ] Add MongoDB MCP server to Claude Code
  - **Official MCP**: [MongoDB Lens MCP Server](https://mcpservers.org/)
  - **Claude Code Example**: [Awesome MCP Servers](https://github.com/wong2/awesome-mcp-servers)
  - **Capabilities**: Full-featured MongoDB database integration, collection querying and analysis, document management, aggregation pipeline support, schema inspection, index management. Multiple implementations available for different use cases.

### Pylon MCP Server
- [ ] Add Pylon MCP server to Claude Code
  - **Official MCP**: [Zapier Pylon MCP](https://zapier.com/mcp/pylon-1)
  - **Claude Code Example**: [Pylon Integrations](https://usepylon.com/integrations)
  - **Capabilities**: Data integration analytics, support workflow automation, Slack/Teams/email/CRM/ticketing integration, data warehouse sync (Snowflake, BigQuery), real-time analytics, seamless multi-tool connectivity. Free up to 300 tool calls per month.

## TODO
- [ ] create babashka starter based on deps.edn
- [ ] babashka CLI for Clojure library with commands and subcommands flags and options and arguments
- [ ] boilerplate for Clojure library

