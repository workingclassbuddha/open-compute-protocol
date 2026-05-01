# Operator Authorization

OCP uses operator authorization to protect raw mesh mutation routes when a node is reachable beyond loopback. This is an alpha control-plane protection, not a production identity system.

## Environment Variables

`OCP_OPERATOR_TOKEN` is the preferred operator token variable.

`OCP_CONTROL_TOKEN` is also accepted for compatibility. If both are set, `OCP_OPERATOR_TOKEN` wins.

If neither token is configured, the server falls back to loopback-only authorization for raw mutation routes.

## Accepted Request Credentials

The server accepts an operator token from the first non-empty value among:

- `X-OCP-Operator-Token: <token>`
- `X-OCP-Control-Token: <token>`
- `Authorization: Bearer <token>`

Bearer values are normalized by stripping the `Bearer ` prefix before comparison.

## Loopback Fallback

When no token is configured, raw mesh mutation routes are allowed from loopback clients such as `127.0.0.1`, `::1`, and `localhost`. Non-loopback clients are rejected for those routes.

When a token is configured, loopback clients must also present the token for raw mesh mutation routes. This keeps LAN and local behavior consistent once the operator opts into token mode.

Signed peer POST routes are a separate path and do not use operator-token auth as their primary gate.

## Phone Links

Phone links should carry the operator token in the URL fragment:

```text
http://HOST_IP:8421/app#ocp_operator_token=YOUR_TOKEN
```

Fragments are preferable to query strings because browsers do not send fragments to the HTTP server. That keeps the token out of normal server request paths, router logs, reverse-proxy logs, and analytics-style URL capture. The browser app can read the fragment locally and store the token for later OCP POST actions.

Do not put operator tokens in query strings.

## Behavior Table

| Situation | Allowed? | Required credential |
|---|---:|---|
| `GET /mesh/manifest` from LAN | yes | none |
| `POST /mesh/peers/connect` from loopback | yes | none if no token configured; operator token if token configured |
| `POST /mesh/peers/connect` from LAN | no unless token configured | operator token |
| signed peer `POST /mesh/handshake` | yes | signed envelope |
| `GET` private artifact content from LAN | no unless authorized | operator token or public artifact policy |

This table is based on `server_http_handlers.py`: GET routes are generally dispatched without operator auth, raw POST routes require operator auth unless their handler is in the signed peer allowlist, and private artifact content reads require operator auth unless policy allows public access.

## Curl Examples

Local manifest:

```bash
curl http://127.0.0.1:8421/mesh/manifest
```

LAN manifest:

```bash
curl http://HOST_IP:8421/mesh/manifest
```

Connect a peer with an operator token:

```bash
curl -X POST http://HOST_IP:8421/mesh/peers/connect \
  -H 'Content-Type: application/json' \
  -H 'X-OCP-Operator-Token: change-me' \
  -d '{"base_url":"http://PEER_IP:8422","trust_tier":"trusted"}'
```

Use `Authorization: Bearer` instead:

```bash
curl -X POST http://HOST_IP:8421/mesh/peers/connect \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer change-me' \
  -d '{"base_url":"http://PEER_IP:8422","trust_tier":"trusted"}'
```

Fetch artifact metadata without content:

```bash
curl 'http://HOST_IP:8421/mesh/artifacts/ARTIFACT_ID?include_content=0'
```

Fetch private artifact content with an operator token:

```bash
curl 'http://HOST_IP:8421/mesh/artifacts/ARTIFACT_ID' \
  -H 'X-OCP-Operator-Token: change-me'
```

## Warnings

- Rotate the token if it appears in shell history, screenshots, logs, or shared chat.
- Do not use a real credential as an OCP operator token.
- Do not expose the alpha server to the internet.
- Use high-entropy tokens for LAN tests.
- Prefer the generated phone link from the app or launcher when possible.
