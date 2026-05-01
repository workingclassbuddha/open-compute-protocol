# OCP HTTP API

This is a human-readable overview of the current `/mesh/*` HTTP surface.

`server_contract.py` is the code-owned source of truth for route metadata. It builds the same snapshot served by:

```text
GET /mesh/contract
```

Export the contract without starting the server:

```bash
python3 scripts/export_contract.py --pretty
python3 scripts/export_contract.py --pretty --output docs/generated/OCP_CONTRACT_v0.1.json
```

## Route Groups

The current contract contains 94 `/mesh/*` endpoints across these route groups:

| Group | Purpose |
|---|---|
| runtime | manifests, contract, app status/history, device profile, discovery, peers, stream, leases, handoff |
| control | server-sent control stream |
| missions | jobs, missions, cooperative tasks, continuity, test missions |
| ops | helpers, workers, approvals, notifications, treaties, secrets, queue, pressure, scheduler decisions |
| artifacts | artifact list/get, publish, replicate, graph replicate, pin, purge, mirror verification |

The app/status/control pages are outside the `/mesh/*` contract but are part of the same standalone HTTP server:

- `/` and `/app`
- `/easy`
- `/control`
- `/app.webmanifest`

## Auth Modes

OCP currently uses four practical auth modes:

- public/local read: GET routes such as `/mesh/manifest` are readable without operator auth.
- operator mutation: raw POST mutation routes require loopback by default or a configured operator token.
- signed peer mutation: selected peer POST handlers accept signed protocol envelopes.
- private artifact content: content reads require operator authorization unless artifact policy allows public access.

See [Operator Authorization](./OPERATOR_AUTH.md) and [Security Model](./SECURITY_MODEL.md) for details.

## Examples

Read the local manifest:

```bash
curl http://127.0.0.1:8421/mesh/manifest
```

Read the live contract:

```bash
curl http://127.0.0.1:8421/mesh/contract
```

Connect a peer with an operator token:

```bash
curl -X POST http://HOST_IP:8421/mesh/peers/connect \
  -H 'Content-Type: application/json' \
  -H 'X-OCP-Operator-Token: change-me' \
  -d '{"base_url":"http://PEER_IP:8422","trust_tier":"trusted"}'
```

Submit a high-level job example. Exact job shape is still governed by the protocol schema and current runtime support:

```bash
curl -X POST http://127.0.0.1:8421/mesh/jobs/submit \
  -H 'Content-Type: application/json' \
  -d '{"job":{"kind":"demo","command":"echo hello from ocp"}}'
```

Inspect artifact metadata without content:

```bash
curl 'http://127.0.0.1:8421/mesh/artifacts/ARTIFACT_ID?include_content=0'
```

## Stability

OCP v0.1 APIs are alpha. The schema registry is descriptive and only partially enforcing. Response schema coverage may be partial, and future releases should add explicit stability labels per endpoint.

Use the generated contract as an inspection and conformance aid, not as a final compatibility promise for independent implementations yet.
