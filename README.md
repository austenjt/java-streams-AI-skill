# java-streams-AI-skill

An AI coding-assistant skill for the Java Stream API: stream creation, every intermediate/terminal operation, the full `Collectors` toolkit (`groupingBy` composition, `teeing`, custom collectors), parallel-stream pitfalls, and everything added through Java 25 LTS (sequenced collections, record-pattern deconstruction in pipelines, the virtual-threads-don't-power-parallel-streams distinction). Includes 15 worked hard-mode scenarios in [`advanced-scenarios.md`](plugins/java-streams-plugin/skills/java-streams/references/advanced-scenarios.md) — stateful-lambda races, custom `Spliterator`s, `reduce`'s combiner trap, and more.

This repo is both the skill's source and a **plugin marketplace** — it can be installed directly into Claude Code or GitHub Copilot CLI, or used as-is by just working in this repo.

## Option 1: Use it directly in this repo

No install step needed. [`.claude/skills/java-streams/`](.claude/skills/java-streams/) is a project-level Claude Code skill — clone the repo, open Claude Code inside it, and the skill auto-triggers on Java Streams tasks.

## Option 2: Install as a plugin

### Claude Code

```
/plugin marketplace add austenjt/java-streams-AI-skill
/plugin install java-streams@java-streams-ai-skill
```

If the install summary says `Run /reload-plugins to activate.`, run that command. To browse before installing, run `/plugin` for the interactive plugin manager. Full docs: [Create and distribute a plugin marketplace](https://code.claude.com/docs/en/plugin-marketplaces).

### GitHub Copilot CLI

```
copilot plugin marketplace add austenjt/java-streams-AI-skill
copilot plugin install java-streams@java-streams-ai-skill
```

To browse before installing: `copilot plugin marketplace browse java-streams-ai-skill`. Manage installed plugins with `copilot plugin list` / `copilot plugin update java-streams` / `copilot plugin uninstall java-streams`. Full docs: [Finding and installing plugins for GitHub Copilot CLI](https://docs.github.com/en/copilot/how-tos/copilot-cli/customize-copilot/plugins-finding-installing).

## Repo layout

```
.claude-plugin/marketplace.json          # marketplace manifest (shared by Claude Code + Copilot CLI)
plugins/java-streams-plugin/
  .claude-plugin/plugin.json             # plugin manifest
  skills/java-streams/
    SKILL.md                             # canonical skill entry point
    references/                          # core-api, collectors, lts-updates, advanced-scenarios, parallel-and-performance
    evals/evals.json                     # eval prompts used to test this skill
.claude/skills/java-streams/             # symlink to plugins/java-streams-plugin/skills/java-streams,
                                          # so this repo works as a project-level skill with no install step
AGENTS.md                                # pointer for other agent tools reading this repo
```

The skill content lives in exactly one place (`plugins/java-streams-plugin/skills/java-streams/`); the `.claude/skills/` path is a symlink to it, not a copy — this keeps the marketplace-installable version and the work-directly-in-this-repo version from drifting apart. The plugin directory has to be self-contained (Claude Code and Copilot CLI both copy the plugin directory itself when installing, so it can't reference files outside it), which is why the symlink points inward from `.claude/skills/` rather than the other way around.
