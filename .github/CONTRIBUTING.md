# Contributing

Thanks for looking at omp-relay! Small, focused PRs are the easiest to review.

## Ground rules

1. **Preserve the transport boundary.** OMP chooses the provider destination; the relay forwards that exact destination. Do not add static provider routing or inspect request bodies.
2. **Keep credentials private.** Pairing tokens, proxy credentials, request bodies, and authorization headers must never appear in logs, fixtures, issues, or commits.
3. **Keep responsibilities separated.** `src/index.ts` owns the OMP extension; `relayd/src/forward.rs` owns the standard forward proxy; legacy relay routes stay isolated from forward-proxy transport.
4. **Tests before behavior.** Extension changes need focused Bun tests under `tests/`; relay changes need Rust tests and the deterministic `self-test` path.

## Workflow

```bash
bun install          # locked development dependencies
bun run lint         # Biome lint
bun run typecheck    # TypeScript, no emit
bun run test         # extension tests
bun run test:rust    # relay unit tests
bun run test:mode    # deterministic relay self-test
bun run check        # all checks, leak scan, and package validation
```

- `bun run scan` runs the pre-publish leak gate; CI enforces it.
- Conventional commits (`feat:`, `fix:`, `chore:`, `docs:`) keep the changelog easy to generate.

## Reporting issues

Use the issue template. Include the command or request shape, relay version, platform, and sanitized logs. Never include credentials, authorization headers, provider payloads, or pairing tokens.
