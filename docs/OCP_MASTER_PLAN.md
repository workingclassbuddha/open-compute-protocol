# OCP Master Plan

Status: working plan  
Date: 2026-04-17  
Scope: OCP only  
Protocol/spec: OCP v0.1  
Reference implementation: Sovereign Mesh  
Current wire version: `sovereign-mesh/v1`

Companion docs:

- `docs/OCP_STATUS.md`
- `docs/OCP_ALL_DEVICES_PLAN.md`

## 1. Executive Summary

OCP is the federated compute layer for a local-first stack. It moves bounded work, artifacts, and control packets between trusted or policy-allowed peers without making any single app the system of record.

OCP v0.1 already exists in working form inside Sovereign Mesh:

- signed peer identity and handshake
- peer registry
- exported agent presence
- bounded remote jobs
- advisory leases
- artifact publish/fetch
- explicit handoff packets
- outbound peer calls
- snapshot-based peer sync with cursors and heartbeat state

What must come next is not more control-plane surface. The next stage is a real execution plane:

- generic worker runtimes
- durable queued jobs with retry and dedupe
- worker heartbeats and claims
- scheduler decisions based on trust, capability, and load
- hardened artifact provenance and attestations
- OpenTelemetry-first observability

## 2. Architectural North Star

By v1.0, OCP should become a portable federated compute fabric for local-first systems:

- origin nodes keep authority over intent, policy, and lineage
- jobs can run locally, on trusted peers, or on optional public/provider lanes
- every execution has durable state transitions
- every output is an explicit signed or attestable artifact
- every placement decision is observable and policy-auditable

OCP solves:

- durable federated execution
- trust-aware placement
- app-neutral job packaging
- resumable or replay-safe delivery
- explicit artifact exchange and provenance
- peer-to-peer coordination without central cloud ownership

OCP does not try to solve yet:

- trustless consensus
- blockchain settlement
- universal NAT traversal
- generalized service-mesh concerns
- automatic secret replication
- cloud-scale multi-tenant orchestration

This master plan should now be read together with `docs/OCP_ALL_DEVICES_PLAN.md`, which expands the roadmap to phones, watches, relay nodes, and intermittent edge peers.

## 3. Design Principles

- Local-first: origin owns intent, policy, and lineage.
- Trust-aware: trust tier affects every placement and access decision.
- Capability-driven: nodes advertise runtimes, resources, and execution features.
- Policy-first: jobs and artifacts carry explicit execution and access policy.
- Durable: jobs survive crashes, restarts, and temporary disconnects.
- Observable: traces, logs, and metrics are first-class protocol outputs.
- Portable: shell, Python, containers, and future Wasm share one job contract.
- App-neutral: Personal Mirror can use OCP, but OCP must not encode app assumptions.
- Provenance-explicit: inputs, outputs, logs, checkpoints, and attestations are artifacts.

## 4. Recommended Architecture

### Identity Plane

Purpose:

- stable peer identity
- request signing
- replay protection
- key continuity and later rotation

Core abstractions:

- `PeerIdentity`
- `OrganismCard`
- `SignedEnvelope`
- `PeerKeyRecord`

Near-term implementation:

- move from dependency-free Schnorr-style signing to Ed25519
- keep nonce replay tables and signed envelopes
- support pinned peer keys and future multi-key rotation records

### Federation / Control Plane

Purpose:

- handshake
- manifest exchange
- presence export
- sync and cursors
- handoff delivery

Core abstractions:

- `PeerRegistry`
- `PeerSession`
- `ControlEvent`
- `HandoffPacket`

Near-term implementation:

- keep HTTP-first transport
- add worker and queue introspection endpoints
- add duplex session bootstrap later, but keep poll-based fallback forever

### Execution Plane

Purpose:

- universal compute jobs
- executor plugins
- attempts and checkpoints

Core abstractions:

- `ComputeJob`
- `JobAttempt`
- `Executor`
- `CheckpointRef`

Near-term implementation:

- build shell and Python executors first
- run through a worker daemon rather than inside request handlers
- keep Docker and Wasm as later adapters

### Durability Plane

Purpose:

- queued delivery
- retries
- ack/nack semantics
- dedupe
- recovery

Core abstractions:

- `JobQueue`
- `JobEvent`
- `AttemptLease`
- `RetryPolicy`

Near-term implementation:

- SQLite-backed append-oriented queue metadata
- explicit attempt records
- visibility timeout through advisory leases

### Artifact Plane

Purpose:

- content-addressed blobs
- manifests
- result bundles
- checkpoint and log storage

Core abstractions:

- `ArtifactBlob`
- `ArtifactManifest`
- `ResultBundle`
- `Attestation`

Near-term implementation:

- local CAS blobs on disk
- OCI-compatible manifest layout
- signed manifests later in v0.4

### Policy / Trust Plane

Purpose:

- trust tiers
- workload labels
- approval boundaries
- secret scope enforcement

Core abstractions:

- `TrustTier`
- `ExecutionPolicy`
- `ArtifactPolicy`
- `ApprovalGate`

Near-term implementation:

- keep current tiers: `self`, `trusted`, `partner`, `market`, `public`, `blocked`
- add stricter workload and public-lane isolation rules

### Observability Plane

Purpose:

- traces
- logs
- metrics
- correlation IDs
- scheduler decision visibility

Core abstractions:

- `TraceContext`
- `JobTelemetry`
- `DecisionRecord`

Near-term implementation:

- instrument all control-plane and job transitions with OpenTelemetry

### Scheduler Plane

Purpose:

- placement by trust, capability, load, and policy

Core abstractions:

- `PlacementRequest`
- `WorkerCard`
- `PlacementDecision`

Near-term implementation:

- origin-side authoritative scheduler
- trust-first scoring
- no hard preemption initially

## 5. Versioned Roadmap

### OCP v0.2

Goals:

- durable execution plane alpha
- generic worker runtime
- queued jobs and retries

Major features:

- worker registration and heartbeats
- queued shell and Python jobs
- job attempts
- lease-backed claim model
- result bundles as artifacts
- initial OTel instrumentation

Deferred:

- duplex federation sessions
- Docker and Wasm GA
- hardened signing plane

Risks:

- duplicate execution
- weak retry fencing
- inline and queued execution paths drifting apart

Success criteria:

- queued jobs survive worker crashes
- retries work deterministically
- results become artifacts with lineage

### OCP v0.3

Goals:

- scheduler maturity
- worker capacity and load routing
- federation session improvements

Major features:

- placement scoring
- latency-sensitive vs batch routing
- capability and resource fit
- worker load and backlog awareness
- duplex session bootstrap with HTTP fallback

Deferred:

- full attestation workflow
- public compute economics

Risks:

- split-brain between streaming and polling paths

Success criteria:

- trust-aware placement works across multiple peers with unstable links

### OCP v0.4

Goals:

- hardened artifact and provenance plane
- stronger policy boundaries

Major features:

- OCI-style manifests
- signed result bundles
- attestations
- retention and GC
- quarantined public-lane outputs
- checkpointed resumability

Deferred:

- trustless marketplace concerns

Risks:

- provenance complexity
- storage growth

Success criteria:

- every output can be traced to inputs, executor, worker, and origin identity

### OCP v1.0

Goals:

- stable protocol and conformance

Major features:

- frozen schemas
- conformance test suite
- stable queue semantics
- stable worker model
- stable scheduler and artifact plane

Deferred:

- consensus and settlement

Risks:

- freezing implementation accidents too early

Success criteria:

- independent implementations interoperate

## 6. Immediate Next Milestone

The single best next milestone is:

`Durable Execution Plane Alpha`

Why:

- OCP already has a strong control-plane kernel
- the main missing layer is durable, generic, policy-aware execution
- without this, scheduler, retries, resumability, and provenance stay shallow

Components:

- universal job metadata for queued execution
- worker registration and heartbeats
- job attempts
- shell executor
- Python executor
- artifactized results

APIs:

- `POST /mesh/jobs/submit`
- `GET /mesh/workers`
- `POST /mesh/workers/register`
- `POST /mesh/jobs/poll`
- `POST /mesh/jobs/{job_id}/claim`
- `POST /mesh/jobs/{job_id}/heartbeat`
- `POST /mesh/jobs/{job_id}/complete`
- `POST /mesh/jobs/{job_id}/fail`
- `POST /mesh/jobs/{job_id}/cancel`

Data model additions:

- `mesh_workers`
- `mesh_job_attempts`
- richer job metadata for retries and dispatch mode

Tests:

- queue claim and completion
- retry after failure
- lease fencing
- result artifact creation
- policy denial for secret/public mismatches

Rollout order:

1. queue jobs without replacing existing inline path
2. add local worker runtime
3. move generic runtimes to worker execution
4. add remote worker/session surface

## 7. OCP Job Model Evolution

Jobs should evolve from bounded `MeshJob` objects into universal compute jobs with:

- `kind`: shell, python, docker, wasm
- `requirements`: capabilities, resources, labels
- `policy`: trust and execution constraints
- `artifact_inputs`
- `result_bundle`
- `retry_policy`
- `timeouts`
- `provenance`
- `status`
- per-attempt state

Execution statuses:

- `accepted`
- `queued`
- `claimed`
- `running`
- `checkpointed`
- `retry_wait`
- `completed`
- `failed`
- `cancelled`
- `expired`
- `lost`

## 8. Worker Model

Each worker runs:

- poll/claim loop
- executor plugins
- artifact cache
- secret resolver
- heartbeat loop
- telemetry exporter

Workers register with:

- runtimes
- resources
- labels
- concurrency
- local trust clearance

Prefer pull over push:

- scheduler queues work
- workers poll and claim
- streaming sessions may only accelerate discovery

## 9. Scheduler Model

Placement order:

1. local
2. trusted peer
3. partner peer
4. market/public lane if policy allows

Scoring inputs:

- trust tier
- capability fit
- resource fit
- queue backlog
- recent reliability
- latency sensitivity

Start with no hard preemption. Use cooperative cancellation only.

## 10. Durability Model

Use at-least-once delivery with:

- `request_id` idempotency
- attempt-level leases
- retry caps
- durable cancellation
- resumable checkpoints later

Non-resumable jobs restart from scratch. Resumable jobs restart from checkpoint artifacts.

## 11. Artifact Model

Artifacts must be:

- immutable
- content-addressed
- policy-scoped
- digest-verified
- linked to jobs and attempts

Logs, checkpoints, and final outputs are all artifacts.

## 12. Security And Trust

Keep trust tiers explicit. Do not let public/provider lanes run:

- private jobs
- trusted-secret jobs
- unrestricted networked workloads

Secrets resolve locally by named scope only.

## 13. Observability

OpenTelemetry-first:

- spans for peer control, scheduler decisions, job attempts, artifact transfer
- metrics for queue depth, retries, latency, failures, lease expiry
- structured logs keyed by `request_id`, `job_id`, `attempt_id`, `worker_id`, `peer_id`

## 14. Implementation-Oriented Package Layout

Recommended OCP-only structure:

- `ocp/identity/`
- `ocp/protocol/`
- `ocp/federation/`
- `ocp/execution/`
- `ocp/workers/`
- `ocp/scheduler/`
- `ocp/durability/`
- `ocp/artifacts/`
- `ocp/policy/`
- `ocp/telemetry/`
- `ocp/adapters/`

## 15. Testing Strategy

- unit tests for policy, retry, queue, digests, and scheduler scoring
- protocol tests for signing, replay, and versioning
- integration tests for multi-peer execution
- simulation tests for disconnects and crash recovery
- fault injection for stale leases, duplicate deliveries, and corrupted artifacts

## 16. Recommended External Technologies

Adopt now:

- SQLite durability
- Ed25519 crypto
- OpenTelemetry
- CAS blobs with OCI-style manifests

Consider later:

- ORAS / OCI distribution APIs
- Sigstore / Cosign
- Wasmtime / WASI
- NATS JetStream bridge

Avoid for now:

- Temporal as a required dependency
- Ray as the control plane
- Kafka or Redis as mandatory infrastructure
- Kubernetes or Nomad as a protocol substrate

## 17. Final Recommendation

Build now:

- durable execution plane alpha

Then:

1. scheduler and worker capacity v1
2. duplex federation sessions with HTTP fallback
3. artifact provenance and policy hardening

The biggest trap to avoid is leaving execution inside the synchronous HTTP request path. OCP will only become a real federated compute protocol once execution, durability, and scheduling are allowed to live as first-class planes.
