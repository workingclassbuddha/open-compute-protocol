# OCP Status

This repo now carries the standalone OCP reference implementation under the current Sovereign Mesh runtime.

Related planning docs:

- `docs/OCP_MASTER_PLAN.md`
- `docs/OCP_ALL_DEVICES_PLAN.md`

## Current framing

- `OCP v0.1` = protocol/spec draft
- `Sovereign Mesh` = current Python-first reference implementation
- `sovereign-mesh/v1` = current wire version

## Standalone project maturity

- Standalone sqlite-backed local runtime for agents, sessions, events, beacons, and locks
- Standalone `/mesh/*` HTTP server in `server.py`
- Standalone OCP regression suite no longer importing `personal_mirror`
- Legacy Personal Mirror integration retained only as an optional reference host

## Already implemented in OCP v0.1

- Signed peer identity and handshake
- Peer registry and remote peer manifests
- Exported agent presence
- Bounded remote jobs
- Advisory leases
- Artifact publish and fetch
- Explicit handoff packets
- Outbound peer calls
- Snapshot-based peer sync with cursors and heartbeat state

## Added in this implementation pass

- Generic worker runtime registration and capability advertising
- Queued execution with durable job attempts
- Durable queue records with visibility timeout, redelivery, and dedupe-aware submission
- Queue event stream with replay cursors over the mesh API
- Queue metrics, backpressure reporting, and dead-letter replay controls
- Queue policy controls for ack deadline, replay window, retention, and dead-letter routing
- Universal normalized job envelope for shell, Python, container, WASM, and custom executor lanes
- Rich runtime environment contract with env policy, filesystem profile, network mode, and writable path hints
- Scoped secret binding policy for runtime env injection with spec-surfaced redacted bindings
- Local secret providers for inline, host env, store-backed, and workspace-file delivery
- Artifact result packages with bundle manifests, execution attestations, first-class log artifacts, and checkpoint artifacts
- OCI-compatible result manifests with config descriptors, layered artifact descriptors, and subject linkage
- Stronger attestations with signed subject descriptors, material descriptors, job-spec provenance digests, and redacted secret-delivery records
- Descriptor-aware remote artifact publish verification for digest, size, and media type
- Artifact discovery surfaces with retention classes, metadata filters, and purge controls
- Peer artifact replication with pull-through content-addressable sync by artifact id or digest
- Digest-filtered artifact discovery and local CAS reuse for replicated artifacts
- Mirror verification records for replicated artifacts with remote descriptor checks and verification status
- Pinning-aware artifact policy so replicated artifacts can be held durably across purge windows
- Graph-aware replication for OCI result bundles and checkpoint-linked attempt artifact sets
- Pull-through sync of linked subject, config, attestation, log, and checkpoint artifacts from bundle roots
- First-class device profiles for `full`, `light`, `micro`, and `relay` nodes with durable local profile state
- Device-profile-aware peer manifests, stream snapshots, and peer registry surfaces for phones, watches, relays, and heavier compute nodes
- Device-aware scheduler inputs for preferred and required device classes, stable-network requirements, battery avoidance, and artifact-mirror-capable placement
- Intermittent-node sync policy surfaces for sleep-capable and mobile peers with preferred sync intervals and offline grace windows
- Resumability-aware placement so intermittent/mobile compute peers are only selected for checkpoint-capable work
- Recovery hints that explicitly steer checkpointed intermittent-node work back toward stable `full` or `relay` peers
- Durable notification inboxes for operator, phone, watch, and relay control flows with compact presentation hints for `light` and `micro` devices
- Durable approval inboxes with request, expiry, resolution, and linked notification records for app-neutral operator control
- Resumability contract in the job envelope with checkpoint policy, explicit recovery states, resume metadata, and retry-time checkpoint recovery
- Operator recovery controls for resume-latest, resume-from-checkpoint, and clean restart
- First-class checkpoint lifecycle on jobs with latest checkpoint refs, selected resume refs, resume counters, and operator recovery audit fields
- True `docker.container` execution through the universal job envelope using the local Docker runtime
- Container runtime policy propagation for env injection, workspace mounting, network mode, timeout handling, and queue-driven worker execution
- True `wasm.component` execution through the universal job envelope using a local Wasmtime runtime
- WASM component resolution from artifact refs or local paths with env injection, preopened workspace access, timeout handling, and queue-driven worker execution
- Pull-based worker claim model with attempt heartbeats
- Shell and Python execution runtimes
- Scheduler v1 with local-vs-remote placement
- Scheduler v1.1 placement hints:
  - `stay_local`
  - `avoid_public`
  - `latency_sensitive`
  - `batch`
  - `preferred_peer_ids`
  - `required_peer_ids`
  - `queue_class`
- Scheduler v1.2 advanced placement inputs:
  - `execution_class`
  - `trust_floor`
  - `preferred_trust_tiers`
  - `prefer_low_backlog`
  - `max_local_queue_depth`
  - `max_peer_queue_depth`
- Durable scheduler decision history
- Reliability-aware placement scoring from local outcomes and synced remote events
- Synced remote queue/load summaries surfaced in manifests and peer listings

## Strongest areas right now

- Identity and peer federation baseline
- Local-first remote sync model
- Initial execution plane for shell and Python jobs
- Universal job envelope with richer runtime environments and scoped secret bindings
- Real shell, Python, and Docker execution lanes behind the same normalized job contract
- Real WASM execution lane behind the same normalized job contract
- Durable job-attempt lifecycle
- Queue-aware resumability and operator recovery controls
- Provider-backed secret delivery with redacted job surfaces and attested execution metadata
- OCI-shaped artifact packaging and signed provenance attestations
- First artifact mobility foundation for multi-device and intermittent-peer workflows
- Stronger replicated-artifact lifecycle for bundle, checkpoint, and attestation-linked sync flows
- First bundle/checkpoint graph sync foundation for richer multi-device recovery and result mobility
- Device-class-aware peer identity, intermittent sync posture, and recovery foundations for all-device mesh expansion
- App-neutral mobile control plane foundation for notifications, approvals, and compact operator inbox flows
- Trust-aware and load-aware scheduler foundations
- Test coverage for multi-node execution and scheduling behavior

## Weakest areas right now

- No generic executor plugin runtime beyond built-in shell and Python handlers
- No true long-lived duplex federation session model
- Policy and trust rules are still simpler than the intended OCP v1 shape

## Key OCP surfaces in this repo

- Manifest: `GET /mesh/manifest`
- Device profile: `GET /mesh/device-profile`
- Peer stream snapshot: `GET /mesh/stream`
- Peer registry: `GET /mesh/peers`
- Job inspect: `GET /mesh/jobs/{job_id}`
- Job cancel: `POST /mesh/jobs/{job_id}/cancel`
- Job resume latest: `POST /mesh/jobs/{job_id}/resume`
- Job resume from checkpoint: `POST /mesh/jobs/{job_id}/resume-from-checkpoint`
- Job restart clean: `POST /mesh/jobs/{job_id}/restart`
- Scheduler decisions: `GET /mesh/scheduler/decisions`
- Worker register: `POST /mesh/workers/register`
- Queue inspect: `GET /mesh/queue`
- Queue events: `GET /mesh/queue/events`
- Queue metrics: `GET /mesh/queue/metrics`
- Queue replay: `POST /mesh/queue/replay`
- Queue ack deadline: `POST /mesh/queue/ack-deadline`
- Notification inbox: `GET /mesh/notifications`
- Notification publish: `POST /mesh/notifications/publish`
- Notification ack: `POST /mesh/notifications/{notification_id}/ack`
- Approval inbox: `GET /mesh/approvals`
- Approval request: `POST /mesh/approvals/request`
- Approval resolve: `POST /mesh/approvals/{approval_id}/resolve`
- Secret list: `GET /mesh/secrets`
- Secret put: `POST /mesh/secrets/put`
- Worker heartbeat: `POST /mesh/workers/{worker_id}/heartbeat`
- Worker poll: `POST /mesh/workers/{worker_id}/poll`
- Worker claim: `POST /mesh/workers/{worker_id}/claim`
- Attempt heartbeat: `POST /mesh/jobs/attempts/{attempt_id}/heartbeat`
- Attempt complete: `POST /mesh/jobs/attempts/{attempt_id}/complete`
- Attempt fail: `POST /mesh/jobs/attempts/{attempt_id}/fail`
- Artifact list: `GET /mesh/artifacts`
- Artifact fetch: `GET /mesh/artifacts/{artifact_id}`
- Artifact publish: `POST /mesh/artifacts/publish`
- Artifact replicate: `POST /mesh/artifacts/replicate`
- Artifact graph replicate: `POST /mesh/artifacts/replicate-graph`
- Artifact pin: `POST /mesh/artifacts/pin`
- Artifact mirror verify: `POST /mesh/artifacts/verify-mirror`
- Artifact purge: `POST /mesh/artifacts/purge`
- Device profile update: `POST /mesh/device-profile`

## Recommended next OCP builds

1. Add stronger relay artifact promotion and resumable-job handoff semantics for sleeping mobile peers.
2. Add push/event bridge adapters so phone and watch clients can receive durable OCP notifications without polling.

## Broader roadmap

- All-devices expansion plan: `docs/OCP_ALL_DEVICES_PLAN.md`

## Verification

Primary regression suite:

```bash
python3 -m unittest tests.test_sovereign_mesh
```

Current standalone baseline:
- `tests.test_sovereign_mesh`: 85 tests passing
