# OCP Threat Model

OCP v0.1.7 is an alpha reference implementation for a local-first compute protocol. This threat model is intentionally conservative: it documents what the current code helps with and what remains future work.

## Assets

- node identity keys and peer identity records
- operator token and phone-link bootstrap token
- local SQLite runtime state
- worker queue, attempts, missions, and scheduler decisions
- artifact metadata and private artifact content
- secrets and execution environment bindings
- route health, peer manifests, and discovery records
- audit-like events and operator approvals

## Actors

- local operator on the same machine
- phone or browser acting as an operator console
- trusted peer node on the LAN
- semi-trusted discovered peer
- untrusted LAN client
- malicious local process
- compromised trusted peer
- accidental operator error

## Trust Boundaries

- loopback HTTP boundary
- LAN HTTP boundary when binding to `0.0.0.0`
- signed peer envelope boundary
- operator-token boundary
- SQLite persistence boundary
- artifact content boundary
- executor boundary between the OCP server and shell/Python/Docker/WASM jobs
- browser boundary for phone-token storage

## Entry Points

- `GET /`, `/app`, `/easy`, and `/control`
- read-oriented `/mesh/*` GET routes
- raw mutation `/mesh/*` POST routes
- signed peer POST routes such as `/mesh/handshake`
- artifact content reads under `/mesh/artifacts/{artifact_id}`
- worker polling, claiming, and attempt completion
- secret put/list flows
- local scripts such as `scripts/start_ocp_easy.py`

## Abuse Cases

- a LAN client attempts to connect peers or mutate device profile without authorization
- a leaked operator token is used to submit jobs or replicate private artifacts
- a trusted peer submits shell or Python work that reads local files
- a Docker job receives a broad host workspace mount
- a private artifact is fetched from LAN without authorization
- a token is placed in a query string and captured by logs
- a stale trusted peer identity is reused after compromise
- a malicious peer submits a signed envelope for a currently allowed peer route
- secrets in payloads or environment leak into logs or artifacts

## Mitigations Already Present

- default loopback binding for the standalone server
- raw mesh mutation routes require loopback unless an operator token is configured
- operator tokens accepted through dedicated headers or bearer auth
- tokened phone links use URL fragments instead of query strings
- selected signed peer POST handlers are separated from raw operator mutations
- private artifact content requires operator auth unless artifact policy is public
- artifact replication remote auth is redacted from stored response and sync metadata
- protocol contract and schema snapshots exist for conformance work
- SQLite remains local and requires no external broker or control plane

## Mitigations Not Yet Present

- signed scoped capability grant enforcement for private artifact replication
- complete peer permission matrix
- key rotation and revocation flows
- durable audit export suitable for external review
- executor sandbox policy that is strict by default across shell, Python, Docker, and WASM
- network isolation policy for Docker/WASM jobs
- formal separation between demo-trust and real workload-trust tiers
- comprehensive security-focused conformance tests

## Priority Security Backlog

1. Canonicalize and verify signed scoped capability grants.
2. Enforce grant audience, subject, expiry, scope, and artifact constraints in the HTTP content gate.
3. Add key rotation and revocation mechanics.
4. Clarify peer permissions per route group.
5. Add durable audit export with token and grant-proof redaction checks.
6. Define executor sandbox policy and default-deny host mounts.
7. Add stricter network isolation for Docker/WASM.
8. Add more security-focused conformance tests.
9. Keep `remote_auth` operator-token replication only as an explicit fallback.

## Alpha Position

The current implementation is good enough for local development, demos, and protocol-boundary work by careful operators. It should not be treated as production-secure, internet-facing infrastructure.
