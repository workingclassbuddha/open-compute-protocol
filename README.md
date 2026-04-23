<p align="center">
  <img src="./assets/ocp-hero.png" alt="Open Compute Protocol" width="100%" />
</p>

<br/>

<div align="center">

# Open Compute Protocol

**A sovereign, local-first compute fabric for trusted devices.**

[![Tests](https://img.shields.io/badge/tests-187%20passing-00FF88?style=flat-square&labelColor=06090F)](./tests/test_sovereign_mesh.py)
[![Release](https://img.shields.io/badge/release-v0.1.4-F6C177?style=flat-square&labelColor=06090F)](./README.md#current-status)
[![Version](https://img.shields.io/badge/wire%20version-sovereign--mesh%2Fv1-00D4FF?style=flat-square&labelColor=06090F)](./docs/OCP_STATUS.md)
[![Status](https://img.shields.io/badge/status-active%20development-C8A96E?style=flat-square&labelColor=06090F)](./docs/OCP_MASTER_PLAN.md)
[![Protocol](https://img.shields.io/badge/protocol-OCP%20v0.1-7BC6FF?style=flat-square&labelColor=06090F)](./docs/OCP_STATUS.md)
[![License](https://img.shields.io/badge/license-AGPL--3.0-F4F1E8?style=flat-square&labelColor=06090F)](./LICENSE)

</div>

<br/>

OCP is the layer that lets laptops, desktops, servers, GPU boxes, relays, and phones begin acting like **one practical distributed machine** without pretending to be one literal operating system.

<br/>

<p align="center">
  <img src="./assets/ocp-architecture.svg" alt="OCP architecture layers" width="100%" />
</p>

<br/>

---

## The Problem

Most systems make you choose between:

- one machine, local control, limited power
- or someone else's cloud, unlimited power, zero control

**OCP is building the third option.**

A governed mesh of your own devices and trusted peers, where computation can move, artifacts can follow it, recovery can survive device failure, and your phone can still govern what the system is allowed to do.

---

## What It Does

When your workstation strains, the mesh notices. A helper laptop or GPU node is enlisted. The right workload shards move. Artifacts and checkpoints stay coherent. You remain in control from any device.

That is the difference between *scripts on a few boxes* and a real compute protocol.

<br/>

<details>
<summary><strong>Full capability list</strong></summary>

<br/>

**Identity & Peers**
- Signed peer identity and handshake
- Peer discovery, manifests, registry, and sync

**Execution**
- Worker registration, polling, claiming, and heartbeats
- Durable queued execution
- Shell, Python, Docker, and WASM execution lanes
- Resumable recovery with checkpoints, resume, restart, and audit trails

**Artifacts**
- Publishing, bundles, attestations, replication
- Graph replication, verification, and pinning

**Orchestration**
- Device profiles: `full` · `light` · `micro` · `relay`
- Compute profiles with CPU, memory, disk, GPU class, and VRAM hints
- Mesh pressure reporting
- GPU-aware cooperative task placement
- Trust-gated autonomous offload
- Durable offload preference memory

**Helper Lifecycle**
- Plan · Enlist · Drain · Retire · Auto-seek

**Operator Layer**
- Durable notifications and approvals
- Mission layer above jobs and cooperative tasks
- Mobile-friendly sovereign control deck
- Treaty-aware peer posture, custody hints, and operator summaries

</details>

---

## What Makes It Different

OCP does not treat machines as anonymous disposable capacity.

It treats them as **situated participants** in a trust-aware system.

| Other systems | OCP |
|---|---|
| Blunt autoscaling | Helper enlistment |
| Blind placement | Pressure-aware offload |
| Job retries | Mission continuity |
| Flat worker pool | Device classes |
| Desktop-only control | Phone / watch operator |

Some devices are powerful. Some are private. Some are fragile. Some are approval-only. Some should only be touched with permission. OCP knows the difference.

---

## Architecture

| Surface | Role |
|---|---|
| `mesh/sovereign.py` | Compatibility-preserving façade and orchestrator |
| `mesh_protocol/` | Signed envelopes, request IDs, handshake contracts, protocol errors |
| `mesh_state/` | SQLite-backed state helpers, projections, event and secret access |
| `mesh_scheduler/` | Placement, scoring, trust, backlog, helper, and GPU-aware selection |
| `mesh_execution/` | Runtime adapters, env and secret binding, submission, result packaging |
| `mesh_artifacts/` | Content-addressed artifacts, bundles, attestations, replication, pinning |
| `mesh_missions/` | Mission lifecycle, continuity, checkpoints, resume, and restart |
| `mesh_helpers/` | Helper lifecycle, offload preferences, autonomous helper evaluation |
| `mesh_governance/` | Notifications, approvals, and governance/policy helpers |
| `runtime.py` | Standalone SQLite-backed substrate |
| `server.py` | `/mesh/*` HTTP API, unified app shell, and operator UI routes |
| `server_app.py` | Installable app shell that unifies setup, control, and protocol inspection |
| `server_control_page.py` | Extracted control-deck renderer for the advanced operator surface |
| `server_http_handlers.py` | Grouped HTTP route handlers so `server.py` stays a thin transport host |
| `docs/` | Protocol notes, status, and roadmap |
| `tests/test_sovereign_mesh.py` | Regression suite — 187 tests |

**Key runtime concepts:**

- **Peers** — known remote nodes with trust and device profile state
- **Jobs** — normalized bounded execution units
- **Missions** — durable higher-level intent above jobs and cooperative tasks
- **Cooperative Tasks** — one logical task split across multiple peers
- **Artifacts** — bundles, checkpoints, logs, attestations, and replicated results
- **Helpers** — extra devices enlisted when the local node is under pressure
- **Treaty Advisories** — peer-level continuity, custody, and governance compatibility hints

---

## Prerequisites

- Python 3.11+
- Bash-compatible shell for `./scripts/start_ocp.sh`
- No external services required for the standalone local node flow

---

## Quick Start

```bash
git clone https://github.com/workingclassbuddha/open-compute-protocol.git
cd open-compute-protocol
python3 scripts/start_ocp_easy.py
```

Then open the OCP app:

```text
http://127.0.0.1:8421/
```

If the deck is empty on a fresh node, seed demo activity in a second terminal:

```bash
python3 scripts/seed_control_demo.py --base-url http://127.0.0.1:8421
```

If you want the direct server form instead of the helper script:

```bash
python3 server.py --host 127.0.0.1 --port 8421
```

If you want the shell-based starter instead of the auto-open launcher:

```bash
./scripts/start_ocp.sh
```

**Useful options:**

```text
--db-path         ./ocp.db
--identity-dir    ./.mesh
--workspace-root  .
--node-id         alpha-node
--display-name    "Alpha"
--device-class    full
--form-factor     workstation
```

For a fuller walkthrough, see [docs/QUICKSTART.md](./docs/QUICKSTART.md).

---

## OCP App

OCP ships a built-in one-app surface at `GET /` and `GET /app`. It is designed for phone browsers and can be added to the home screen as a local-first operator app.

Mac beta desktop launcher:

```bash
python3 -m ocp_desktop.launcher
```

The launcher keeps state under `~/Library/Application Support/OCP/`, can start a local-only node or LAN-reachable Mesh Mode node, opens the OCP app, and shows the phone link for testing on the same Wi-Fi.

Unsigned macOS beta bundle:

```bash
python3 scripts/build_macos_app.py
open dist/OCP.app
```

The beta `.app` requires `python3` to be installed on the Mac. It excludes local state, identities, databases, `.git`, caches, and test artifacts from the bundle.

When Mesh Mode is started from the desktop launcher, copied phone links include an operator token in the URL fragment and the browser stores it locally for OCP POST actions. If you start the server manually with `OCP_HOST=0.0.0.0`, set `OCP_OPERATOR_TOKEN` and open `http://HOST_IP:8421/app#ocp_operator_token=YOUR_TOKEN` from the phone.

Inside the app:

- `Today` shows mesh strength, Autonomic Mesh status, latest proof, next actions, and a phone link/QR
- `Setup` embeds the easy setup flow from `GET /easy`
- `Control` embeds the advanced control deck from `GET /control`
- `Protocol` links the live manifest, device profile, and HTTP contract from `/mesh/*`

The setup module is meant for the common human flow: open OCP on two or more machines, press `Connect Everything`, then press `Test Whole Mesh`.
It now also supports:

- `Copy My Easy Link` for manual fallback
- QR pairing so the second device can open the pairing link by scanning instead of typing
- automatic LAN/share URL detection in the easy page so the phone and spare laptop can see the best local address without manual IP hunting
- one-button nearby mesh join with `Connect Everything`
- one-button cooperative verification across the whole current mesh with `Test Whole Mesh`
- an auto-open starter script at `python3 scripts/start_ocp_easy.py`, which now also prints detected LAN URLs when the node is network-reachable

The control module is phone-friendly, so your phone can act as a real operator console for the mesh. From there you can inspect and act on:

- Peer and helper state
- Queue and recovery status
- Approvals and notifications
- Cooperative tasks and missions
- Autonomy posture
- Offload memory
- Treaty/custody compatibility, restore-target hints, and live peer advisory events

For remote UI testing on a fresh standalone node, use:

```bash
python3 scripts/seed_control_demo.py --base-url http://HOST_IP:8421
```

---

## Visual Identity

This repo includes branded OCP graphics directly in source:

- `assets/ocp-hero.svg`
- `assets/ocp-architecture.svg`

These are meant to give the project a clearer identity as:

- a protocol
- a mesh
- a mission-oriented control layer
- a sovereign alternative to anonymous cloud orchestration

---

## Tests

```bash
python3 -m unittest tests.test_sovereign_mesh
python3 server.py --help
```

Current baseline: **187 tests passing.**

---

## Current Status

**Released in v0.1.4**

- Autonomic Mesh alpha adds route health, one-button activation, proof repair, helper-safe enlistment, and app-visible summaries.
- Desktop Alpha RC adds a Mac beta launcher, unsigned `.app` bundle builder, and a polished `/app` Today surface backed by `GET /mesh/app/status`.
- LAN operator hardening now requires signed peer traffic or operator-token authenticated raw mesh mutations from non-loopback clients.
- Private artifact content fetches now require operator auth unless the artifact policy is public.
- Runtime execution now defaults to explicit environment inheritance, with `inherit_env_allowlist` for deliberate host env pass-through.
- The signed envelope implementation now uses dependency-free Ed25519 helpers under `ed25519-sha512-v1`.
- The protocol-kernel refactor and mission-continuity/treaty foundation from v0.1.3 remain intact, with the full regression suite green at 187 tests.

**Implemented in the current runtime**

- standalone local node startup
- peer identity, manifests, sync, and discovery
- queued jobs, missions, cooperative tasks, and recovery controls
- helper enlistment, mesh pressure, and operator approvals
- built-in `/` app shell with `/easy` setup and `/control` operator modules
- treaty-aware continuity advisories across peer cards, mission summaries, connect/sync responses, and live streams
- code-owned `/mesh/contract` route map, schema registry, and validation helpers for protocol and conformance work

**Still evolving**

- richer treaty and custody enforcement beyond the current advisory and continuity-specific layer
- continuity-vessel and richer artifact lineage work
- broader multi-device orchestration UX

---

## Current Framing

- `OCP v0.1` — protocol and spec draft
- `v0.1.4` — current implementation release
- `Sovereign Mesh` — Python-first reference implementation
- `sovereign-mesh/v1` — current wire version

---

## Direction

The strongest near-term directions:

1. Richer mission-centric operator UX
2. Stronger policy and treaty semantics for peer cooperation
3. Continuity-vessel evolution of checkpoints and recovery
4. More expressive helper and GPU orchestration
5. A more cinematic, legible constellation-style cockpit

OCP is already past "protocol sketch" stage. If it keeps going in this direction, it becomes more than a scheduler. It becomes a practical sovereign compute layer for all your devices.

---

## Related

- [Status](./docs/OCP_STATUS.md)
- [v0.1.4 Release Notes](./docs/RELEASE_v0.1.4.md)
- [7026 Vision](./docs/OCP_7026_VISION.md)
- [Quickstart](./docs/QUICKSTART.md)
- [Master Plan](./docs/OCP_MASTER_PLAN.md)
- [All Devices Plan](./docs/OCP_ALL_DEVICES_PLAN.md)

---

## Boundary

OCP is standalone. It can integrate with other systems but is not a submodule of any of them and should not be described as one.

---

<div align="center">
<br/>
<sub>sovereign · local-first · trust-aware · all your devices</sub>
</div>
