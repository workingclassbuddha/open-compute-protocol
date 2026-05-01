# OCP Security Model

OCP v0.1.7 is a trustworthy alpha, not a production security boundary. This document explains the current protection model so operators can make safer choices while the protocol and reference implementation mature.

## What OCP Protects

OCP currently focuses on local-first trust for a small mesh of devices you operate or deliberately trust:

- local node identity material under the identity directory
- peer manifests and signed peer handshakes
- raw mesh mutation routes from arbitrary LAN clients
- private artifact content unless policy permits public access
- redaction of operator-mediated remote artifact tokens from stored metadata and responses
- basic secret storage and delivery through the local SQLite runtime
- durable jobs, attempts, artifacts, missions, scheduler decisions, and governance records

## What OCP Does Not Yet Protect

OCP does not yet provide production-grade isolation or enterprise-grade key management:

- no stable security certification or external audit
- signed scoped capability grants are schema-defined only and not yet enforced as private-content authorization
- no mature revocation, key rotation, or peer permission matrix
- no hardened executor sandbox policy across all lanes
- no network isolation guarantee for Docker or WASM lanes
- no durable audit export format intended for compliance
- no guarantee that a malicious trusted peer cannot submit dangerous work if the operator allows it

## Local-First Assumptions

The default server binds to loopback. In that mode, OCP assumes the browser and CLI clients are local to the same machine and controlled by the operator. SQLite is the only required database. No external cloud service, queue, control plane, or broker is required for the standalone alpha flow.

## Loopback Default Safety Model

When no `OCP_OPERATOR_TOKEN` or `OCP_CONTROL_TOKEN` is configured, raw mesh mutation routes are allowed only from loopback clients. This makes the default `127.0.0.1:8421` startup safer for local experimentation.

Loopback is not a substitute for host security. A local process running as the same user can still call OCP routes.

## LAN Exposure Risk

WARNING: Binding OCP to `0.0.0.0` exposes the HTTP server to the local network. Anyone on the LAN who can reach the port may be able to read public/local status routes and attempt operator actions.

When using LAN mode:

- set `OCP_OPERATOR_TOKEN` to a high-entropy value
- send operator actions through `X-OCP-Operator-Token`, `X-OCP-Control-Token`, or `Authorization: Bearer`
- avoid untrusted Wi-Fi networks
- stop the server when the demo is over
- do not expose the alpha server directly to the internet

## Operator Token Behavior

If `OCP_OPERATOR_TOKEN` or `OCP_CONTROL_TOKEN` is configured, non-signed raw mutation routes require a matching presented token. The server accepts:

- `X-OCP-Operator-Token: <token>`
- `X-OCP-Control-Token: <token>`
- `Authorization: Bearer <token>`

WARNING: Operator token leakage gives the holder operator authority over raw mutation routes. Treat the token like a password. Do not paste it into logs, screenshots, shell history you share, or query strings.

## Signed Peer Request Behavior

Some peer POST handlers are allowed as signed peer traffic instead of operator-token traffic:

- `POST /mesh/handshake`
- `POST /mesh/jobs/submit`
- `POST /mesh/artifacts/publish`
- `POST /mesh/agents/handoff`

Those requests are still protocol-alpha surfaces. They rely on signed envelopes and handler-level validation, but peer permissions are not yet a complete capability system.

## Artifact Access Behavior

Artifact metadata is listable through the artifact routes. Artifact content is more sensitive:

- `GET /mesh/artifacts/{artifact_id}?include_content=0` returns metadata without content.
- `GET /mesh/artifacts/{artifact_id}` includes content by default.
- Private content from LAN clients requires operator authorization unless artifact policy allows public access.

WARNING: Private artifact replication currently supports explicit operator-token `remote_auth` in the request body. That is an operator-mediated alpha bridge, not the final security model. Capability grants now have an alpha schema and expiry/redaction helper, but grant signatures are not yet enforced by the HTTP artifact content gate.

## Secret Delivery Behavior

OCP has a local secret surface for execution and runtime flows. Secrets should be scoped narrowly and kept out of payloads whenever possible.

WARNING: Secrets passed in JSON payloads, shell commands, environment variables, or job metadata can be exposed through process inspection, logs, history, crash reports, or accidental artifact publication. Prefer explicit secret references and keep demos free of real credentials.

## Execution Risk

OCP can run shell, Python, Docker, and WASM-style workloads through the execution service.

WARNING: Running shell or Python jobs from peers is equivalent to running code chosen by another party on your machine. Only enable worker execution for peers and workloads you trust.

WARNING: Docker jobs can still touch host resources if mounted workspaces, sockets, credentials, or broad paths are exposed. Treat Docker host workspace mounts as sensitive until stricter sandbox policy exists.

WASM is the intended safer direction for portable execution, but the alpha does not yet claim a complete WASM isolation boundary.

## Trust Tiers

Trust tiers are currently policy hints used by peer, helper, scheduler, and operator flows. They help the reference implementation explain posture and choose safer defaults, but they are not yet a complete permissions system.

Current meaning is modest:

- trusted peers may be considered for more cooperation
- lower-trust peers should be treated as discovery or limited-control participants
- approval and treaty surfaces can advise operators, but enforcement is still partial

## Known Alpha Limitations

- protocol schemas are descriptive and only partially enforcing
- response schema coverage is incomplete
- peer permissions are coarse
- capability grants are not fully enforced
- token rotation and revocation are manual
- Docker/WASM isolation policy is still evolving
- audit trails are useful for debugging, not compliance
- LAN demos depend on local firewall and router behavior

## Recommended Safe Defaults

- run local-only with `127.0.0.1` unless you are actively testing LAN mode
- set `OCP_OPERATOR_TOKEN` before binding to `0.0.0.0`
- use phone links with URL fragments for token bootstrap
- avoid real secrets and private data in alpha demos
- run workers only on machines you control
- keep artifact content private unless you deliberately publish it
- prefer metadata-only artifact reads when inspecting from LAN
- stop the server after demos on shared networks
