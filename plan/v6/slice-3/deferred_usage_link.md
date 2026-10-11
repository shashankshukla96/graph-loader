# Deferred: one-line link from `docs/USAGE.md`

Per the First Mate's binding orchestrator constraints, this slice does
not edit `docs/USAGE.md` itself — the orchestrator adds this line at
merge time. This file records the exact, intended edit (Story 8 of
`plan/v6/slice-3/stories.md`).

## Intended edit

Append exactly one line at the very end of `docs/USAGE.md` (after §13
"Continuous integration", the file's current last section), with no
other change to the file (not its table of contents, not §8 "Outputs,
monitoring and verification"):

```markdown
For live metrics, Prometheus/Grafana discovery and example alert rules, see [`docs/MONITORING.md`](MONITORING.md).
```

## Sequencing

Land this after `docs/MONITORING.md` (this slice's Story 7) already
exists in the target branch, so the link is never dead even momentarily.

## Verification once applied

- `git diff docs/USAGE.md` for that commit shows exactly one added line,
  appended after the file's current final line, and zero removed or
  modified lines.
- The relative link `MONITORING.md` resolves from `docs/`.
