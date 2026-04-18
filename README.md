# Open Compute Protocol

This project is the standalone home of the Open Compute Protocol reference implementation.

Current framing:
- `OCP v0.1` = protocol/spec draft
- `Sovereign Mesh` = current Python-first reference implementation
- `sovereign-mesh/v1` = current wire version

Project layout:
- `mesh/`
  Core Sovereign Mesh protocol and execution runtime
- `runtime.py`
  Standalone local sqlite substrate for agents, sessions, events, beacons, and locks
- `server.py`
  Standalone `/mesh/*` HTTP server entrypoint
- `docs/`
  OCP spec, status, and roadmap material
- `integrations/personal_mirror_server.py`
  Legacy host-integration reference from the earlier embedding
- `tests/test_sovereign_mesh.py`
  Standalone OCP regression suite

## Run

```bash
cd /Users/mespoy/Desktop/ocp
python3 server.py --host 127.0.0.1 --port 8421
```

Useful options:
- `--db-path ./ocp.db`
- `--identity-dir ./.mesh`
- `--workspace-root .`
- `--node-id alpha-node`
- `--display-name "Alpha Organism"`

## Test

```bash
cd /Users/mespoy/Desktop/ocp
python3 -m unittest tests.test_sovereign_mesh
```

Notes:
- This repo is now intended to stand on its own, not as a Personal Mirror submodule.
- Personal Mirror is one possible host/integration, not a runtime dependency.
