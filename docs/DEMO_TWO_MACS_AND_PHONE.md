# Two Macs and a Phone: First Sovereign Mesh Proof

This demo is designed for a skeptical user to understand OCP in about 10 minutes. It uses two Macs on the same Wi-Fi network and one phone browser as the operator console.

## Safety First

WARNING: This demo binds OCP to `0.0.0.0` so other devices on the LAN can reach it. For LAN actions, set `OCP_OPERATOR_TOKEN` and use the phone link with a URL fragment token:

```text
http://HOST_IP:8421/app#ocp_operator_token=YOUR_TOKEN
```

Do not expose this alpha server to the internet. Do not use real secrets in demo jobs.

## 1. Start Alpha on Machine A

```bash
OCP_HOST=0.0.0.0 OCP_NODE_ID=alpha-node OCP_DISPLAY_NAME=Alpha python3 scripts/start_ocp_easy.py
```

Keep the terminal open. Note the printed LAN share URL.

For LAN operator actions, prefer the safer tokened form:

```bash
OCP_OPERATOR_TOKEN=change-me OCP_HOST=0.0.0.0 OCP_NODE_ID=alpha-node OCP_DISPLAY_NAME=Alpha python3 scripts/start_ocp_easy.py
```

## 2. Start Beta on Machine B

```bash
OCP_HOST=0.0.0.0 OCP_PORT=8422 OCP_NODE_ID=beta-node OCP_DISPLAY_NAME=Beta python3 scripts/start_ocp_easy.py
```

Keep this terminal open too.

For LAN operator actions on Beta too:

```bash
OCP_OPERATOR_TOKEN=change-me OCP_HOST=0.0.0.0 OCP_PORT=8422 OCP_NODE_ID=beta-node OCP_DISPLAY_NAME=Beta python3 scripts/start_ocp_easy.py
```

## 3. Open the App on Machine A

Open:

```text
http://127.0.0.1:8421/app
```

or use the URL printed by the starter.

## 4. Open the Phone Link

From Machine A, copy the phone link or LAN URL. On the phone, open:

```text
http://ALPHA_LAN_IP:8421/app#ocp_operator_token=change-me
```

The fragment after `#` stays in the phone browser and is not sent as part of the HTTP request path.

## 5. Connect Everything

In the app or easy setup view, use `Connect Everything`. OCP should discover or connect the two nodes. If automatic discovery misses, use `Copy My Easy Link` on one machine and paste it into the manual connect box on the other.

## 6. Test Whole Mesh

Use `Test Whole Mesh`. This launches the existing mesh proof flow and should produce operator-readable status about route health and peer cooperation.

## 7. Run on Best Device

If available, use `Run on Best Device`. Full laptop/workstation nodes started with `scripts/start_ocp_easy.py` advertise a default worker, so the scheduler can choose a placement for the demo workload.

## 8. Replicate Proof Artifact

If available, use `Replicate Proof Artifact`. Private proof artifacts require operator-mediated auth today. The app should avoid storing remote operator tokens and should use redacted sync metadata.

## 9. Inspect the Mesh

Open these URLs from a browser or use `curl`:

```text
http://ALPHA_LAN_IP:8421/mesh/manifest
http://ALPHA_LAN_IP:8421/mesh/app/status
http://ALPHA_LAN_IP:8421/mesh/routes/health
http://ALPHA_LAN_IP:8421/mesh/artifacts
http://ALPHA_LAN_IP:8421/mesh/scheduler/decisions
```

For private artifact content, use metadata-only inspection unless you intentionally authorize content access:

```text
http://ALPHA_LAN_IP:8421/mesh/artifacts/ARTIFACT_ID?include_content=0
```

## 10. What Happened

You started two local-first OCP nodes, exposed them to the LAN for discovery, used the phone as an operator console, connected peers, tested route health, optionally scheduled work on the best available device, and optionally replicated a proof artifact with explicit operator mediation.

The important boundary: OCP is not pretending the machines are one OS. It is creating a protocol-visible fabric where identity, manifests, routes, work, artifacts, and operator decisions can be inspected.

## Troubleshooting

Cannot find peer:

- Make sure both machines are on the same Wi-Fi.
- Try `Copy My Easy Link` and paste the URL manually.
- Check `/mesh/discovery/candidates`.

Firewall issue:

- Allow incoming connections for Python or the OCP app on both Macs.
- Confirm the port is reachable from the other machine.

Wrong `HOST_IP`:

- Use the LAN URL printed by `scripts/start_ocp_easy.py`.
- Do not use `127.0.0.1` from the phone, because that points at the phone itself.

Port conflict:

- Use `OCP_PORT=8422` or another free port.
- Stop any older OCP process before restarting.

Empty deck:

- A fresh node may have no demo activity.
- Run `python3 scripts/seed_control_demo.py --base-url http://HOST_IP:8421` if you want sample UI data.

No worker available:

- Use `scripts/start_ocp_easy.py` on a full laptop/workstation node.
- Check `/mesh/app/status` for execution readiness.

Artifact replication denied:

- Private content needs operator authorization or public artifact policy.
- Use the app action or send explicit `remote_auth` only for trusted demos.

Token missing:

- Set `OCP_OPERATOR_TOKEN` before starting LAN mode.
- Open the phone app with `#ocp_operator_token=YOUR_TOKEN`.

Browser opened loopback URL from phone:

- Replace `127.0.0.1` with the Mac's LAN IP.
- Use the printed LAN share URL.
