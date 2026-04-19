# Contributing to The Open Compute Protocol

Thanks for contributing to OCP.

OCP is still early, so the most helpful contributions are the ones that make the protocol and reference implementation more coherent, more legible, and more durable, not just bigger.

## Ground Rules

- Keep OCP standalone.
- Do not collapse OCP back into OMP or Personal Mirror.
- Preserve the current framing:
  - `OCP v0.1` = protocol/spec
  - `Sovereign Mesh` = Python-first reference implementation
  - `sovereign-mesh/v1` = current wire version
- Prefer clear incremental improvements over speculative rewrites.
- Keep local-first, trust-aware behavior intact.

## Good Contribution Areas

- peer federation and sync behavior
- missions, recovery, and continuity
- helper enlistment and offload policy
- cooperative task orchestration
- artifact lineage, replication, and verification
- phone-friendly operator UX
- docs, diagrams, examples, and protocol clarity
- tests that lock in real behavior

## Before You Open a PR

1. Start from a clean branch.
2. Keep changes scoped to one coherent improvement.
3. Add or update tests when behavior changes.
4. Update docs when surfaces, semantics, or operator flows change.
5. Run:

```bash
python3 -m unittest tests.test_sovereign_mesh
python3 server.py --help
```

## Code Style

- Prefer straightforward Python over framework-heavy abstractions.
- Keep the runtime legible for protocol work.
- Avoid introducing dependencies unless the gain is substantial.
- Preserve operator-facing explainability.

## Design Direction

OCP is not trying to be a generic cloud clone.

The strongest current direction is:

- mission-oriented orchestration
- governed helper enlistment
- continuity-aware recovery
- trust-aware peer cooperation
- operator control from desktop and mobile surfaces

If your change strengthens those ideas, it is likely in the right direction.
