# OCP Status

This repo now carries the standalone OCP reference implementation under the current Sovereign Mesh runtime.

Related planning docs:

- `docs/OCP_MASTER_PLAN.md`
- `docs/OCP_ALL_DEVICES_PLAN.md`
- `docs/OCP_7026_VISION.md`
- `docs/QUICKSTART.md`

## Current framing

- `OCP v0.1` = protocol/spec draft
- `v0.1.4` = current Desktop Alpha RC implementation release
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
- Mobile-first web control page for phone browsers with live peer, inbox, and approval actions backed by the native mesh endpoints
- Stronger phone operator deck with queue/recovery cards, direct resume/restart/replay/cancel controls, and live auto-refresh
- SSE-backed control stream for the cockpit so mission, queue, helper, approval, and notification updates can push into the deck without waiting for timer refreshes
- Active peer seek/discovery with candidate tracking, optional auto-connect, and mesh-visible discovery records
- First operator-grade `Connect Devices` flow in the control deck with nearby scan, one-click connect, built-in reachability diagnostics, and one-click test missions
- New unified OCP app shell at `GET /` and `GET /app` so phone and desktop operators get setup, control, and protocol inspection in one surface
- App home now includes a `Today` panel with mesh strength, Autonomic Mesh activation, latest proof state, next actions, phone link/QR, and route-health summaries
- Compact app status API at `GET /mesh/app/status` so product surfaces can render operator state without scraping the advanced cockpit
- Mac-first beta desktop launcher with Local Only and Mesh Mode starts, Application Support state defaults, live status, app opening, and phone/LAN link copy
- Unsigned macOS beta bundle builder at `python3 scripts/build_macos_app.py` that excludes local state, identities, databases, git metadata, caches, and test artifacts
- Plain-language easy setup remains available at `GET /easy` so first-run pairing can stay friendly while `/control` remains the advanced cockpit module
- Easy setup share-link copy and plain troubleshooting guidance so nearby pairing can fall back to “copy this link to the other computer” instead of terminal instructions
- QR pairing on the easy page plus an auto-open launcher script so first-run setup can start with `python3 scripts/start_ocp_easy.py` and a scannable pairing link
- One-button `Connect Everything` mesh join flow so the runtime can scan, connect, and fold nearby trusted devices into one reachable mesh without per-peer clicking
- Whole-mesh proof launch so one button can fan a cooperative test mission across the current sovereign mesh and verify multi-device execution as one fabric
- Base-url normalization for wildcard-bound nodes so `--host 0.0.0.0` advertises a reachable endpoint instead of leaking wildcard addresses into peer state
- Cooperative task groups that fan one logical task into multiple child jobs across local and remote peers
- Aggregated cooperative-task state with child-job summaries so multiple machines can act on one larger job together
- First Mission Layer with durable mission objects above jobs and cooperative task groups
- Mission launch paths that wrap a single local job or a cooperative task launch without replacing existing execution primitives
- Mission continuity tracking with child-job lineage, checkpoint/result references, UI drill-down links, mission-level resume/restart/cancel controls, and status propagation from execution state
- Continuity export alpha with dry-run vessel planning and sealed vessel/witness artifact publication for mission continuity state
- Continuity verification alpha with vessel verification and dry-run restore planning over the mesh API
- Continuity metadata overlays now surface in mission state, manifests, and peer listings with habitat-role and continuity-capability hints
- Scheduler decisions now include continuity-aware soft preferences and explainable candidate alignment for continuity metadata and habitat roles
- First treaty groundwork for continuity custody with normalized treaty documents, treaty listing/proposal APIs, continuity export validation, and treaty-aware restore blocking/reporting
- Treaty posture now surfaces in manifests, peer summaries, and mission continuity summaries, with an audit endpoint for operator-readable treaty validation guidance
- Mesh Pulse control-deck panorama with live mission/queue/helper/approval/notification summaries and a cross-system activity stream for operator visibility
- In-place operator inspect overlay for mission, queue-job, and cooperative-task drill-down without leaving the cockpit
- Compute-profile-aware device modeling with CPU, memory, disk, accelerator, GPU class, and VRAM hints
- Helper enlistment planning and lifecycle controls for enlist, drain, retire, and pressure-triggered auto-seek
- GPU-aware scheduling and cooperative shard placement so GPU-heavy work can prefer the right helper peer
- Mesh pressure reporting that surfaces local saturation and whether helper compute should be enlisted
- Policy-driven autonomous offload with safe defaults for manual, approval-gated, and auto-enlist helper behavior
- Approval-backed autonomous offload application so a granted operator decision can immediately enlist the proposed helper peers
- Durable offload preference memory by peer and workload class so OCP can remember prefer, allow, approval, avoid, and deny choices
- Workload-class-aware autonomy rules so only selected workload classes are auto-offloaded while others can require approval or stay local
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
- First continuity-vessel artifact baseline with reserved `vessel` and `witness` artifact kinds
- First treaty-aware continuity governance baseline with explicit treaty references on mission continuity overlays and exported vessels
- Stronger replicated-artifact lifecycle for bundle, checkpoint, and attestation-linked sync flows
- First bundle/checkpoint graph sync foundation for richer multi-device recovery and result mobility
- Device-class-aware peer identity, intermittent sync posture, and recovery foundations for all-device mesh expansion
- App-neutral mobile control plane foundation for notifications, approvals, and compact operator inbox flows
- Phone-friendly web controller surface served directly by the standalone OCP node
- Mobile operator workflow for acting on queue-backed jobs and resumable recovery from the phone browser
- First active peer-discovery baseline for operator-driven mesh expansion
- First app-grade pair/connect baseline so two fresh nodes can move from discovery to trusted remote execution without raw Python snippets
- First cooperative multi-peer execution baseline for splitting a single task across multiple computers
- First mission-oriented orchestration baseline for keeping higher-level intent durable across retries, checkpoints, restarts, and cooperative handoffs
- First autonomous helper-enlistment baseline for bringing extra devices into the mesh when the local node is saturated
- First GPU-aware helper selection baseline for CPU/GPU split planning across multiple peers
- First trust-gated autonomy layer so offload can escalate to approvals instead of blindly auto-enlisting peers
- First persistent offload-memory layer so approved or rejected helper choices can influence future autonomy decisions
- Trust-aware and load-aware scheduler foundations
- Test coverage for multi-node execution and scheduling behavior

## Weakest areas right now

- No generic executor plugin runtime beyond built-in shell and Python handlers
- No true long-lived duplex federation session model
- Policy and trust rules are still simpler than the intended OCP v1 shape

## Key OCP surfaces in this repo

- Manifest: `GET /mesh/manifest`
- HTTP contract and schema snapshot: `GET /mesh/contract`
- Unified OCP app: `GET /`
- Installable app shell: `GET /app`
- App status: `GET /mesh/app/status`
- App manifest: `GET /app.webmanifest`
- Autonomic status: `GET /mesh/autonomy/status`
- Autonomic activation: `POST /mesh/autonomy/activate`
- Route health: `GET /mesh/routes/health`
- Route probe: `POST /mesh/routes/probe`
- Easy setup module: `GET /easy`
- Phone control module: `GET /control`
- Phone control stream: `GET /mesh/control/stream`
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
- Treaty list: `GET /mesh/treaties`
- Treaty inspect: `GET /mesh/treaties/{treaty_id}`
- Treaty propose: `POST /mesh/treaties/propose`
- Treaty audit: `POST /mesh/treaties/audit`
- Discovery candidates: `GET /mesh/discovery/candidates`
- Discovery seek: `POST /mesh/discovery/seek`
- Local discovery scan: `POST /mesh/discovery/scan-local`
- Connectivity diagnostics: `GET /mesh/connectivity/diagnostics`
- One-click peer connect: `POST /mesh/peers/connect`
- One-click peer connect-all: `POST /mesh/peers/connect-all`
- Mission list: `GET /mesh/missions`
- Mission inspect: `GET /mesh/missions/{mission_id}`
- Mission launch: `POST /mesh/missions/launch`
- Mission test launch: `POST /mesh/missions/test-launch`
- Whole-mesh test launch: `POST /mesh/missions/test-mesh-launch`
- Mission continuity export: `POST /mesh/missions/{mission_id}/continuity/export`
- Continuity vessel verify: `POST /mesh/continuity/vessels/verify`
- Continuity restore plan: `POST /mesh/continuity/vessels/restore-plan`
- Mission cancel: `POST /mesh/missions/{mission_id}/cancel`
- Mission resume latest: `POST /mesh/missions/{mission_id}/resume`
- Mission resume checkpoint: `POST /mesh/missions/{mission_id}/resume-from-checkpoint`
- Mission restart: `POST /mesh/missions/{mission_id}/restart`
- Cooperative task list: `GET /mesh/cooperative-tasks`
- Cooperative task inspect: `GET /mesh/cooperative-tasks/{task_id}`
- Cooperative task launch: `POST /mesh/cooperative-tasks/launch`
- Mesh pressure: `GET /mesh/pressure`
- Helper list: `GET /mesh/helpers`
- Helper enlistment plan: `POST /mesh/helpers/plan`
- Helper enlist: `POST /mesh/helpers/enlist`
- Helper drain: `POST /mesh/helpers/drain`
- Helper retire: `POST /mesh/helpers/retire`
- Helper auto-seek: `POST /mesh/helpers/auto-seek`
- Helper preference list: `GET /mesh/helpers/preferences`
- Helper preference set: `POST /mesh/helpers/preferences/set`
- Helper autonomy evaluate: `GET /mesh/helpers/autonomy`
- Helper autonomy run: `POST /mesh/helpers/autonomy/run`

`/mesh/contract` now exposes the grouped route contract, reusable protocol schema registry, and schema refs used by the first lightweight ingress validation path.
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

1. Add signed/notarized packaging, tray presence, startup defaults, and deeper firewall prompts after the unsigned Mac beta launcher proves the flow.
2. Add a mission launch helper in the control surface so operators can create single-job or cooperative missions without dropping to raw JSON.

## Broader roadmap

- All-devices expansion plan: `docs/OCP_ALL_DEVICES_PLAN.md`

## Verification

Primary regression suite:

```bash
python3 -m unittest tests.test_sovereign_mesh
```

Current standalone baseline:
- `tests.test_sovereign_mesh`: 185 tests passing
