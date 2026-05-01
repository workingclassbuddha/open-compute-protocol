# OCP Test Structure

`tests.test_sovereign_mesh` is the current broad regression suite and remains the baseline for the v0.1.7 Trustworthy Alpha pass.

New tests should be added by subsystem as the suite is split up:

- `tests/protocol/` for contract, schema, signed-envelope, and conformance behavior
- `tests/runtime/` for local SQLite runtime and state behavior
- `tests/http/` for server route and auth behavior
- `tests/execution/` for worker execution lanes and result packaging
- `tests/scheduler/` for placement and decision behavior
- `tests/artifacts/` for artifact publishing, replication, policy, and retention
- `tests/missions/` for mission and continuity behavior
- `tests/app/` for app/status/control API behavior
- `tests/desktop/` for launcher and desktop integration behavior
- `tests/integration/` for slower multi-node flows

Integration tests may remain broader and slower. Protocol and conformance tests should prefer `server_contract.py` and `mesh_protocol` schemas over duplicated route metadata.
