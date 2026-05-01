# Open Compute Protocol v0.1 Draft

## 1. Status

This is a draft boundary document for OCP v0.1. It describes the current Python-first Sovereign Mesh reference implementation and the direction of the protocol. It is not a final standard and does not claim independent implementation compatibility yet.

The current schema registry is descriptive and only partially enforcing; future OCP releases should tighten normative conformance.

## 2. Scope

OCP v0.1 covers local-first mesh identity, peer manifests, signed envelopes, operator authorization, jobs, workers, queue attempts, artifacts, missions, scheduler decisions, device profiles, trust tiers, and conformance inspection.

## 3. Non-goals

- production-grade security certification
- internet-scale federation
- cloud control-plane dependency
- replacement of the current HTTP server
- required databases beyond SQLite for the reference implementation
- stable endpoint compatibility guarantees

## 4. Terminology

- Node: a local OCP runtime participant.
- Peer: another known node.
- Operator: the human or app controlling raw mesh actions.
- Worker: an execution participant that can claim and run jobs.
- Mission: higher-level intent above jobs and cooperative tasks.
- Artifact: content-addressed output, checkpoint, log, attestation, or bundle.
- Signed envelope: a protocol message signed by a node identity.

## 5. Node Identity

The reference implementation maintains node identity material under the configured identity directory. Nodes SHOULD use stable identities for trusted mesh demos so peers can recognize them across restarts.

## 6. Peer Manifest

Peers expose manifests through `/mesh/manifest`. A manifest describes node identity, base URL, protocol posture, device profile, and current capability hints. The current HTTP contract treats this as a public/local read surface.

## 7. Signed Envelopes

Signed peer traffic uses `mesh_protocol` envelope helpers. The implementation currently treats these POST handlers as signed peer routes:

- `/mesh/handshake`
- `/mesh/jobs/submit`
- `/mesh/artifacts/publish`
- `/mesh/agents/handoff`

Signed envelopes SHOULD include enough identity, request, and payload data for validation and replay-aware handling. Current enforcement is alpha and route-specific.

## 8. Operator Authorization

Raw mesh mutation routes require operator authorization. If no token is configured, loopback clients MAY use raw mutation routes. If `OCP_OPERATOR_TOKEN` or `OCP_CONTROL_TOKEN` is configured, clients MUST present the matching token in `X-OCP-Operator-Token`, `X-OCP-Control-Token`, or `Authorization: Bearer`.

Phone links SHOULD carry the token in the URL fragment as `#ocp_operator_token=...`.

## 9. Jobs

Jobs describe bounded execution intent. The reference implementation supports submission, scheduling, cancellation, resume, restart, attempts, and checkpoints. Job schemas live in `mesh_protocol` and route metadata lives in `server_contract.py`.

## 10. Workers

Workers register capabilities and resources, poll for work, claim jobs, heartbeat, and report completion or failure. Full laptop/workstation nodes started through the easy launcher may auto-register a default worker for demos.

## 11. Queue and Attempts

The queue records messages and events for durable work progress. Attempts track execution leases, heartbeats, completion, failure, retry posture, and related artifacts.

## 12. Artifacts

Artifacts carry result content, logs, checkpoints, attestations, bundles, lineage, policy, and replication metadata. Private artifact content MUST NOT be served to LAN clients unless operator authorization succeeds or artifact policy allows public access.

Private artifact replication currently MAY use explicit `remote_auth` with an operator token. This is an alpha fallback. OCP v0.1.7 also defines the first `CapabilityGrant` schema and declared `remote_auth: {type: "capability_grant", grant: ...}` path for expiry-checked, redacted grant metadata. Future versions SHOULD enforce signed scoped capability grants for private content authorization.

## 13. Missions

Missions represent durable operator intent above individual jobs and cooperative tasks. The current implementation supports launch, list, inspect, cancel, resume, restart, checkpoint resume, continuity export, and restore planning.

## 14. Scheduler Decisions

Scheduler decisions record why a device or helper was selected. Inputs include device profiles, worker state, route health, trust posture, artifact locality, checkpoint locality, and placement constraints.

## 15. Device Profiles

Device profiles describe node class, form factor, execution tier, power profile, network profile, mobility, and related scheduling hints. Implementations SHOULD treat profiles as hints unless a route or scheduler rule explicitly enforces them.

## 16. Trust Tiers

Trust tiers currently guide scheduling, helper enlistment, discovery, and operator summaries. They are not yet a complete permissions system. Implementations SHOULD avoid treating trust tier labels alone as sufficient authority for dangerous execution.

## 17. Error Handling

The HTTP reference implementation returns JSON error objects for unknown endpoints, operator authorization failures, validation failures, and handler exceptions. Protocol error details are still evolving and should become more consistent in future releases.

## 18. Conformance

`server_contract.py` builds the current HTTP contract snapshot. `mesh_protocol` owns schema definitions and fixtures. `scripts/check_protocol_conformance.py` checks that the snapshot and schema fixtures are internally coherent.

Implementations SHOULD use the contract and schemas as conformance guidance, with the alpha caveat that enforcement is partial.

## 19. Compatibility

The current wire version is `sovereign-mesh/v1`. OCP v0.1 is the first boundary toward independent protocol compatibility, but the reference implementation remains the source of truth for this alpha.

## 20. Known Alpha Gaps

- incomplete response schema coverage
- descriptive schemas that are only partially enforcing
- no fully enforced signed scoped capability grants
- no complete peer permission matrix
- no mature key rotation or revocation
- incomplete executor sandbox policy
- no formal endpoint stability labels
- security conformance tests need expansion

References:

- `server_contract.py`
- `mesh_protocol` schemas
- `scripts/check_protocol_conformance.py`
- `docs/OCP_STATUS.md`
- `docs/OCP_MASTER_PLAN.md`
- `docs/spec/OCP_CAPABILITY_GRANTS.md`
