---
name: cli-output-style
description: >
  Match operator-facing CLI output to this repo's gold standard. Use when
  writing or changing console banners, sticky footers, spinners, progress
  bars, scrolling status lines, or per-run logs — traders, derived jobs,
  scrapers, proxy. Triggers: "status line", "sticky footer", "spinner",
  "console output", "banner", "match the scraper", "style guide", /cli-output-style.
---

# CLI output style

**Gold standard:** `raw_data/polygon_contract_events_v3/main.py` (the scraper). Copy that screen language. Do not invent a second dialect.

**Human-owned style guide:** `.github/copilot-instructions.md`. Agents do not edit it. If the scraper and that file disagree, follow the scraper and tell the human — propose a style-guide update; do not apply it.

This repo is one publication. New programs match existing ones. Do not add a parallel convention.

## Copy from the scraper

- Banner: `output:` (bold) or `account:` (bold), then `log:` padded to the same column, then a blank line. Full paths. Do not echo CLI flags.
- Sticky footer: Rich `Live` + `Progress`. Work in progress only; `transient=True`.
- Bounded work: spinner + description + bar + done/total + elapsed + ETA. Green = done, magenta = remaining.
- Unbounded work (watch loops): spinner + description + elapsed. **No** bar, **no** percentage.
- Scrolling lines: two-space indent, `-> `, `label: value`, two spaces between fields. Counts green. Example: `  -> blks: 12,345  evts: 99  0.4s`.
- Closing: blank line, `run complete` / `run failed`, indented `time:`.
- Logs: `logs/main-{YYYY-MM-DDTHHMMSS}Z.log` next to the script. Every screen line in the file (UTC). Secrets never printed in full.

## If you find a mismatch

1. Match the scraper in the code you are touching.
2. Do not rewrite the style guide.
3. Tell the human: what diverged, that the scraper is source of truth, and that `.github/copilot-instructions.md` should be updated if they want the written guide to match.
