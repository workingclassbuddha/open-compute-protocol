# Open Compute Protocol

OCP is a **sovereign, local-first compute fabric** for trusted devices.

It is the layer that lets laptops, desktops, servers, GPU boxes, relays, phones, and other peers begin acting like one practical distributed machine without pretending they are one literal operating system.

## Current Framing

- `OCP v0.1` = protocol/spec draft
- `Sovereign Mesh` = current Python-first reference implementation
- `sovereign-mesh/v1` = current wire version

This repository is the standalone home of that implementation.

---

## Why OCP Exists

Most systems make you choose between:

- one machine with local control
- or someone else’s cloud with centralized control

OCP is trying to build a third thing:

**a governed mesh of your own devices and trusted peers**

That means:

- computation can move
- artifacts can follow it
- recovery can survive device failure
- helpers can be enlisted when one machine is under pressure
- operator approval can still matter
- the system stays local-first, policy-aware, and legible

OCP is not just “remote jobs.”
It is the beginning of a sovereign compute layer.

---

## What It Can Do Now

The current standalone OCP already supports:

- signed peer identity and handshake
- peer discovery, manifests, registry, and sync
- worker registration, polling, claiming, and heartbeats
- durable queued execution
- shell, Python, Docker, and WASM execution lanes
- resumable recovery with checkpoints, resume, restart, and audit trails
- artifact publishing, bundles, attestations, replication, graph replication, verification, and pinning
- device profiles for `full`, `light`, `micro`, and `relay`
- compute profiles with CPU, memory, disk, accelerator, GPU class, and VRAM hints
- helper lifecycle controls:
  - plan
  - enlist
  - drain
  - retire
  - auto-seek
- mesh pressure reporting
- GPU-aware cooperative task placement
- trust-gated autonomous offload
- durable offload preference memory
- durable notifications and approvals
- a mobile-friendly web control deck
- cooperative tasks that spread one logical task across multiple peers
- a first **Mission Layer** above jobs and cooperative tasks

---

## What Makes It Different

OCP does not treat machines as anonymous disposable capacity.

It treats them as **situated participants** in a trust-aware system:

- some devices are powerful
- some are private
- some are fragile
- some are approval-only
- some are ideal helpers
- some should only be touched with permission

That worldview shows up in the runtime:

- helper enlistment instead of blunt autoscaling
- pressure-aware offload instead of blind placement
- mission continuity instead of just job retries
- device classes instead of one flat worker pool
- phone/watch operator control instead of “desktop only”

---

## Architecture At A Glance

| Surface | Role |
|---|---|
| `mesh/sovereign.py` | Core OCP runtime: peers, jobs, missions, helpers, artifacts, recovery |
| `runtime.py` | Standalone sqlite-backed substrate |
| `server.py` | `/mesh/*` HTTP API and `/control` operator UI |
| `docs/` | Protocol notes, status, and roadmap |
| `tests/test_sovereign_mesh.py` | Regression suite |

Key runtime concepts:

- **Peers**: known remote nodes with trust and device profile state
- **Jobs**: normalized bounded execution units
- **Missions**: higher-level durable intent above jobs and cooperative tasks
- **Cooperative Tasks**: one logical task split across multiple peers
- **Artifacts**: bundles, checkpoints, logs, attestations, and replicated results
- **Helpers**: extra devices enlisted when the local node is under pressure

---

## Operator Experience

OCP ships a built-in control surface:

- `GET /control`

From there, an operator can already inspect:

- peer and helper state
- queue and recovery status
- approvals and notifications
- cooperative tasks
- mission cards
- autonomy posture
- offload memory

The control deck is phone-friendly, so your phone can already act as a real operator console for the mesh.

---

## Quick Start

Run one local node:

```bash
cd /Users/mespoy/Desktop/ocp
python3 server.py --host 127.0.0.1 --port 8421
```

Useful options:

- `--db-path ./ocp.db`
- `--identity-dir ./.mesh`
- `--workspace-root .`
- `--node-id alpha-node`
- `--display-name "Alpha"`
- `--device-class full`
- `--form-factor workstation`

Then open:

- [http://127.0.0.1:8421/control](http://127.0.0.1:8421/control)

---

## Test

```bash
cd /Users/mespoy/Desktop/ocp
python3 -m unittest tests.test_sovereign_mesh
python3 server.py --help
```

Current baseline:

- `tests.test_sovereign_mesh`: 117 tests passing

---

## Project Direction

The repo is already past “protocol sketch” stage.

The strongest near-term direction is:

1. richer mission-centric operator UX
2. stronger policy and treaty semantics for peer cooperation
3. continuity-vessel evolution of checkpoints and recovery
4. more expressive helper/GPU orchestration
5. a more cinematic, more legible constellation-style cockpit

If OCP keeps going in that direction, it becomes more than a scheduler.
It becomes a practical sovereign compute layer for all your devices.

---

## Related Notes

- [Status](./docs/OCP_STATUS.md)
- [Master Plan](./docs/OCP_MASTER_PLAN.md)
- [All Devices Plan](./docs/OCP_ALL_DEVICES_PLAN.md)

---

## Boundary

OCP is standalone.

It can integrate with Personal Mirror, Golem, or other systems, but it is not a submodule of them and should not be described as one.
