# OCP All Devices Plan

Status: working plan  
Date: 2026-04-18  
Scope: OCP only  
Protocol/spec: OCP v0.1  
Reference implementation: Sovereign Mesh  
Current wire version: `sovereign-mesh/v1`

## 1. Purpose

This document expands OCP from a federated compute layer for desktops and servers into a universal device mesh that can include:

- desktops
- laptops
- servers
- GPU nodes
- phones
- tablets
- watches
- small edge devices
- intermittent or battery-powered nodes

The goal is not to make every device do everything.

The goal is to let every device participate safely and usefully in one protocol, with each device contributing according to its class, trust level, power budget, connectivity, and runtime capability.

## 2. North Star

OCP should evolve into a sovereign compute fabric where all trusted devices can behave like one policy-aware distributed machine.

In that system:

- heavy compute runs on the strongest nodes
- sensitive workloads stay on approved local devices
- phones hold approvals, keys, and lightweight context
- watches act as minimal control and trust surfaces
- artifacts, checkpoints, and attestations flow across the mesh
- failures recover across devices instead of dying with one machine

The user should be able to think:

`run this on the best safe device`

instead of:

`which machine should run this?`

## 3. Device Classes

### Full Nodes

Examples:

- desktop
- laptop
- server
- GPU workstation

Responsibilities:

- accept and execute queued jobs
- store and replicate artifacts
- host worker runtimes
- keep queue and scheduler state
- perform checkpoint and recovery flows

Typical runtimes:

- `shell.command`
- `python.inline`
- `docker.container`
- `wasm.component`

### Light Nodes

Examples:

- phone
- tablet

Responsibilities:

- join the mesh with signed identity
- receive notifications and operator prompts
- hold secrets or approval tokens
- run lightweight jobs when allowed
- contribute context such as battery, location, mobility, and connectivity
- inspect jobs, artifacts, and recovery state

Typical runtimes:

- lightweight shell or Python lanes
- later mobile-native lanes

### Micro Nodes

Examples:

- watch
- wearable
- tiny embedded or sensor devices

Responsibilities:

- act as presence beacons
- expose approval or acknowledgement actions
- contribute minimal sensor signals
- provide emergency stop, pause, or confirm actions
- show compact mesh state

Typical runtimes:

- no heavy execution by default
- signal, approval, and event roles first

### Relay / Cache Nodes

Examples:

- always-on mini PC
- home server
- VPS relay

Responsibilities:

- keep sync alive for intermittent devices
- mirror or pin artifacts
- relay queue snapshots, notifications, and control packets
- reduce wake-up cost for sleeping peers

## 4. Required New Device Identity Fields

OCP peer cards and manifests should grow a device profile section with fields like:

- `device_class`
- `execution_tier`
- `power_profile`
- `network_profile`
- `mobility`
- `storage_class`
- `approval_capable`
- `secure_secret_capable`
- `sensor_capabilities`
- `interactive_capable`
- `background_execution_capable`

Suggested normalized values:

- `device_class`: `full`, `light`, `micro`, `relay`
- `execution_tier`: `heavy`, `standard`, `light`, `control_only`
- `power_profile`: `plugged`, `battery`, `low_power`, `mixed`
- `network_profile`: `wired`, `wifi`, `metered`, `intermittent`
- `mobility`: `fixed`, `portable`, `wearable`, `embedded`

This keeps scheduling policy concrete instead of inferring everything from raw labels.

## 5. What Phones Should Do

Phones should be first-class OCP peers, but not treated like mini servers.

Phones should be able to:

- maintain signed mesh identity
- receive job completion and failure notifications
- approve sensitive actions
- submit jobs remotely into the mesh
- inspect queue, artifacts, and checkpoint state
- hold local encrypted secrets
- contribute mobility, battery, and network state to scheduling
- run lightweight jobs when charging and policy allows

Phones should not be assumed to:

- stay online continuously
- accept large artifact replication by default
- run heavy container workloads
- host large queues or artifact stores

## 6. What Watches Should Do

Watches should be treated as micro control peers.

Watches should be able to:

- maintain or derive a mesh-linked identity
- receive urgent alerts
- confirm or deny operator actions
- show compact peer and job health
- emit presence and sensor events
- act as emergency control surfaces for pause, cancel, or acknowledge flows

Watches should not be assumed to:

- run general compute jobs
- store large artifacts
- remain online long enough for durable execution ownership

## 7. Core Protocol Work Needed

### 7.1 Device Profile Surface

Add device profile fields to:

- manifest payloads
- peer registry rows
- worker cards where relevant
- scheduler decision context

### 7.2 Intermittent Session Model

Phones and watches sleep, disconnect, and roam.

OCP needs:

- resumable peer sessions
- sync watermarks and incremental resync
- delayed delivery of control events
- peer freshness windows
- heartbeat semantics that tolerate background suspension

### 7.3 Notification and Approval Plane

All-device OCP requires more than polling.

OCP should add:

- operator approval requests
- job completion notifications
- recovery prompts
- trust or policy warnings
- device wake hints

This can start HTTP-first and poll-friendly, but the data model should already support:

- pending approvals
- acknowledgement state
- expiry
- escalation target

### 7.4 Artifact Movement for Intermittent Peers

Artifact sync must account for:

- partial download and resume
- delayed pull-through caching
- pinned artifacts for offline recovery
- bandwidth-aware replication
- optional summaries for constrained devices

### 7.5 Capability-Tiered Execution

Jobs need execution classes that align with device class:

- heavy compute
- standard compute
- light compute
- control-only

The scheduler should refuse placements that are physically or operationally wrong, even if capability names match.

## 8. Scheduling Model for All Devices

The scheduler should evolve from trust and backlog awareness into device-aware placement.

New placement inputs should include:

- `device_class_allow`
- `device_class_deny`
- `power_requirement`
- `network_requirement`
- `interactive_ok`
- `background_only`
- `metered_network_ok`
- `battery_ok`
- `portable_ok`
- `approval_required`
- `local_secret_required`

New peer and worker scoring signals should include:

- battery state
- charging state
- storage pressure
- connectivity quality
- background execution reliability
- artifact proximity
- checkpoint proximity

Examples:

- a heavy model job should prefer `full` nodes with `heavy` execution tier
- a private approval step can target a `light` phone node
- a watch can receive a high-priority confirm/deny request but never be considered for compute

## 9. Secret and Trust Model

Phones and watches become much more powerful if they can participate in trust and approval without being compute owners.

OCP should support:

- operator-held approval tokens
- device-held scoped secrets
- secret release rules tied to peer trust and device class
- approval-gated secret delivery
- attested record of which device authorized release

Important principle:

secret custody and compute placement should be separable.

That allows patterns like:

- phone approves
- server executes
- artifact bundle returns
- watch confirms final publication

## 10. Artifact Strategy for All Devices

Artifact movement is the foundation of all-device OCP.

The artifact plane should support:

- content-addressable replication
- pinning and mirror policy
- digest-first pull-through
- checkpoint locality hints
- compact summary artifacts for constrained clients
- resumable fetch
- provenance-preserving relay

Artifacts should be classified by access pattern:

- `hot`
- `durable`
- `session`
- `summary`
- `checkpoint`

Constrained devices should be able to request summaries and metadata without needing full blobs.

## 11. Proposed Phases

### Phase 1: Multi-Device Foundation

Goals:

- artifact mobility
- device identity
- intermittent sync

Milestones:

1. peer-to-peer artifact replication and pull-through CAS sync
2. device profile fields in manifest and peer registry
3. relay/cache node behavior for intermittent devices
4. sync cursors and resumable reconnection semantics

### Phase 2: Control Surfaces

Goals:

- phones and watches become useful operators

Milestones:

1. pending approval model
2. notification event model
3. phone-safe mesh control APIs
4. watch-safe compact status and approval APIs

### Phase 3: Device-Aware Scheduling

Goals:

- correct work lands on correct devices

Milestones:

1. device-class-aware placement fields
2. power and network-aware scheduler scoring
3. artifact-locality and checkpoint-locality weighting
4. policy enforcement for non-executable device classes

### Phase 4: Lightweight Execution

Goals:

- light nodes can do useful work safely

Milestones:

1. lightweight runtime class for phones/tablets
2. explicit `light` and `control_only` execution lanes
3. operator and policy controls for charging-only or Wi-Fi-only execution
4. recovery semantics for sleeping or background-suspended devices

### Phase 5: Sovereign Device Mesh

Goals:

- all devices behave as one compute fabric

Milestones:

1. cross-device checkpoint resume by default
2. durable secret and approval delegation
3. mesh UI and observability for every device class
4. mission-level orchestration over mixed device classes

## 12. Immediate Implementation Order

The best next concrete build order is:

1. artifact replication and remote content-addressable sync
2. device profile fields in manifests and peer records
3. relay/cache behavior for intermittent peers
4. device-aware scheduler inputs
5. approval and notification plane

That order matters because phones and watches become truly useful only after the mesh can move artifacts, preserve context, and survive disconnects.

## 13. Near-Term Spec Additions

Near-term OCP spec additions should likely include:

- `peer.device_profile`
- `worker.execution_tier`
- `job.placement.device_policy`
- `job.policy.approval_requirements`
- `artifact.summary_ref`
- `notification` packet type
- `approval_request` packet type
- `approval_response` packet type

These should remain app-neutral and transport-neutral.

## 14. Success Criteria

This plan is succeeding when all of the following become true:

- a server can execute work originated from a phone
- a phone can approve or deny a sensitive recovery action
- a watch can receive and acknowledge urgent mesh prompts
- artifacts and checkpoints can move across peers without manual copying
- the scheduler can distinguish between heavy, light, and control-only nodes
- a sleeping or disconnected device no longer breaks the user’s mental model of one mesh

## 15. Summary

OCP should aim to be the protocol that lets every device participate in one trusted compute fabric.

Desktops and servers should carry the heavy execution burden. Phones should become first-class control, trust, and light-execution peers. Watches should become compact approval, presence, and emergency control peers. Relay nodes should keep the mesh coherent when portable devices disappear and return.

That is the path from a strong decentralized job runner to a real all-devices sovereign compute system.
