# MDJaada plugin for Claude Code

Adds the `setup-mdjaada` skill: Claude installs the [MDJaada](https://mdjaada.com) agent on a Linux server
(systemd), a Kubernetes cluster, Docker, or from source, connects it to app.mdjaada.com or a self-hosted MDJaada,
checks that it reports, and helps with OpenTelemetry, node secrets, upgrades and troubleshooting.

```
/plugin marketplace add madhubolla2205/mdjaada-agent
/plugin install mdjaada@mdjaada
```

Then ask: "set up MDJaada monitoring". Your agent key never goes through the chat: steps that need it run in your
own terminal with a hidden prompt.

Without the plugin system, copy `skills/setup-mdjaada/` to `~/.claude/skills/` (or `.claude/skills/` in a project).

Apache License 2.0, like the rest of this repository.
