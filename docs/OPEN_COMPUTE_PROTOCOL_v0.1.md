# Open Compute Protocol (OCP) v0.1

Status: draft  
Date: 2026-04-17  
Reference implementation: `Sovereign Mesh` in [`../mesh/sovereign.py`](../mesh/sovereign.py)  
Current wire version: `sovereign-mesh/v1`

## Purpose

Open Compute Protocol (`OCP`) is a local-first federation protocol for moving bounded work between trusted organisms, worker nodes, and external compute lanes.

It standardizes:

- organism identity
- signed request envelopes
- capability discovery
- bounded job delegation
- advisory leases
- artifact exchange
- agent handoff packets
- event stream synchronization

`Sovereign Mesh` is the first working Python-first reference implementation of this protocol.

## Design Goals

- local-first control with explicit trust boundaries
- portable jobs across heterogeneous runtimes
- no automatic replication of private cognition
- simple HTTP-first transport with optional stream transport
- deterministic replay protection and idempotency
- clear separation between protocol and implementation branding

## Non-Goals For v0.1

- trustless consensus
- blockchain settlement
- NAT traversal
- autonomous secret replication
- fully persistent duplex federation sessions

## Versioning Model

OCP separates the protocol spec version from the implementation wire version.

| Layer | Value |
|---|---|
| Protocol family | `OCP` |
| Protocol release | `0.1` |
| Current implementation | `Sovereign Mesh` |
| Current wire version | `sovereign-mesh/v1` |

This allows multiple implementations to speak the same protocol family while evolving their runtime details independently.

## Trust Model

Every peer has an explicit trust tier:

- `self`
- `trusted`
- `partner`
- `market`
- `public`
- `blocked`

Every remotely executable payload carries a policy:

- `private`
- `trusted`
- `public`

Current policy rules:

- `private` jobs require an explicitly trusted peer
- `trusted` jobs require a trusted or partner peer
- `public` jobs may run on market-facing peers
- secrets do not transit unless explicitly scoped

## Core Objects

| Type | Purpose |
|---|---|
| `OrganismCard` | Identity, endpoints, transport metadata, capability cards, trust summary |
| `CapabilityCard` | Declares available runtimes or providers |
| `AgentPresence` | Exported agent and active-session description |
| `MeshJob` | Portable bounded remote execution unit |
| `LeaseRecord` | Advisory claim on a resource or in-flight job |
| `ArtifactRef` | Signed reference to content, result, or checkpoint material |
| `HandoffPacket` | Explicit agent-to-agent delegation bundle |

## Signed Envelope

All mutable OCP requests are carried in a signed envelope:

```json
{
  "request": {
    "node_id": "alpha-node",
    "timestamp": "2026-04-17T05:00:00Z",
    "nonce": "2f1c...",
    "request_id": "a4d0...",
    "protocol_family": "OCP",
    "protocol_release": "0.1",
    "implementation": "Sovereign Mesh",
    "protocol_version": "sovereign-mesh/v1",
    "signature_scheme": "schnorr-sha256-modp1024-v1",
    "signature": "..."
  },
  "body": {
    "job": {}
  }
}
```

Required integrity properties:

- timestamp bounded by maximum clock skew
- nonce remembered per peer to prevent replay
- request id used for idempotency and deduplication
- body signed together with route and protocol version

## Transport

### Required in v0.1

- HTTP request/response

### Optional in v0.1

- stream bootstrap over `GET /mesh/stream`
- websocket or long-lived duplex sessions by implementation

## Endpoint Surface

The current reference implementation exposes the protocol under `/mesh/*`.

| Endpoint | Method | Purpose |
|---|---|---|
| `/mesh/manifest` | GET | Capability, identity, and exported-presence discovery |
| `/mesh/handshake` | POST | Signed peer introduction and trust bootstrap |
| `/mesh/peers` | GET | Inspect known peers |
| `/mesh/peers/sync` | POST | Import remote events using cursors |
| `/mesh/stream` | GET | Event snapshot and stream bootstrap |
| `/mesh/lease/acquire` | POST | Acquire advisory lease |
| `/mesh/lease/heartbeat` | POST | Renew advisory lease |
| `/mesh/lease/release` | POST | Release advisory lease |
| `/mesh/jobs/submit` | POST | Submit bounded remote job |
| `/mesh/jobs/{job_id}` | GET | Inspect job state |
| `/mesh/jobs/{job_id}/cancel` | POST | Cancel job |
| `/mesh/artifacts/publish` | POST | Publish artifact |
| `/mesh/artifacts/{artifact_id}` | GET | Fetch artifact subject to policy |
| `/mesh/agents/handoff` | POST | Send explicit delegation packet |

## Execution Lifecycle

1. Peer fetches `/mesh/manifest`.
2. Peer sends a signed `/mesh/handshake`.
3. Organisms exchange presence, beacons, and capability cards.
4. A job is submitted through `/mesh/jobs/submit`.
5. The receiving organism may acquire an advisory lease.
6. Job state progresses through queued, running, completed, cancelled, expired, or lost.
7. Outputs may be published as artifacts.
8. Peers synchronize resulting events with `/mesh/peers/sync`.

## Artifacts

Artifacts are immutable references to payloads or outputs with:

- digest
- media type
- size
- owner peer id
- access policy
- metadata
- download URL

The v0.1 reference implementation verifies digests and refuses downloads that violate the artifact policy.

## Agent Federation

Agent federation is the first proof target of OCP v0.1.

It includes:

- exported `AgentPresence`
- explicit `HandoffPacket` delivery
- bounded remote background jobs
- artifact-backed context passing

It does not include:

- unrestricted lattice replication
- remote secret mirroring
- full shared-memory semantics

## Current Implementation Notes

The working implementation in this repo intentionally stays pragmatic:

- signature scheme is currently dependency-free Schnorr-style signing, not Ed25519
- stream sync is snapshot-and-cursor based, not yet a permanent duplex session manager
- Golem is treated as a provider lane, not a trust authority
- the standalone OCP store remains the local source of truth for mesh runtime state

## Planned OCP v0.2 Themes

- standardized Ed25519 signatures
- true duplex federation sessions
- background sync daemon
- richer executor and scheduling metadata
- cost and settlement extensions

## Relationship To OMP

`OMP` and `OCP` solve different layers of the stack:

- `OMP` is memory and recall
- `OCP` is federated compute and coordination

Applications can compose both:

- their own memory/knowledge layer for local cognition
- OCP for cross-organism delegation, artifacts, and jobs
