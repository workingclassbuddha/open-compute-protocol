# Quickstart

This is the fastest way to get **The Open Compute Protocol** running locally.

## 1. Clone the repo

```bash
git clone https://github.com/workingclassbuddha/open-compute-protocol.git
cd open-compute-protocol
python3 -m pip install -e .
```

## 2. Start one local node

Easiest path:

```bash
python3 scripts/start_ocp_easy.py
```

That launcher:

- starts the standalone OCP node with local defaults
- opens the unified OCP app automatically in your browser
- keeps easy setup available at `/easy`
- keeps the advanced deck available at `/control`
- prints detected LAN share URLs automatically when the node is reachable from other devices

Mac beta app launcher:

```bash
python3 -m ocp_desktop.launcher
```

Native SwiftPM Mac Mission Control app:

```bash
swift run OCPDesktop
```

Use `Start Local Only` for a private node or `Start Mesh Mode` when you want your phone or spare laptop on the same Wi-Fi to connect. The launcher stores its beta app state under `~/Library/Application Support/OCP/`.
In Mesh Mode, `Copy Phone Link` includes a private operator token in the URL fragment so the phone app can safely run OCP actions such as `Activate Mesh`.

Build the unsigned Mac beta bundle:

```bash
python3 scripts/build_macos_app.py
open dist/OCP.app
```

Build the unsigned native SwiftPM beta bundle:

```bash
python3 scripts/build_swift_macos_app.py
open "dist/OCP Desktop.app"
```

If you prefer the shell starter instead:

```bash
./scripts/start_ocp.sh
```

That script:

- creates a local state directory under `./.local/ocp`
- creates identity and workspace folders
- starts the standalone OCP server with sensible defaults

By default, the node comes up on:

- OCP app: `http://127.0.0.1:8421/`
- installable app shell: `http://127.0.0.1:8421/app`
- easy setup module: `http://127.0.0.1:8421/easy`
- advanced control module: `http://127.0.0.1:8421/control`
- manifest: `http://127.0.0.1:8421/mesh/manifest`

## 3. Verify it is alive

In a second terminal:

```bash
curl http://127.0.0.1:8421/mesh/manifest
```

## 4. Seed the control deck with demo activity

A fresh standalone node starts empty. If you want the UI to show missions, queue activity, notifications, and approvals right away, run:

```bash
python3 scripts/seed_control_demo.py --base-url http://127.0.0.1:8421
```

Then refresh:

```text
http://127.0.0.1:8421/
```

## 5. Run the regression suite

```bash
python3 -m unittest tests.test_sovereign_mesh
python3 server.py --help
```

## Common variations

Use a different port:

```bash
OCP_PORT=8521 ./scripts/start_ocp.sh
```

Bind so another machine on your network can reach it:

```bash
OCP_HOST=0.0.0.0 ./scripts/start_ocp.sh
```

LAN mode exposes the alpha HTTP server to your local network. Read [Security Model](./SECURITY_MODEL.md) and [Operator Authorization](./OPERATOR_AUTH.md) before using LAN mode beyond a trusted demo network.

If you want phone/LAN control actions when starting manually, set an operator token and open the app with that token in the URL fragment:

```bash
OCP_OPERATOR_TOKEN=change-me OCP_HOST=0.0.0.0 python3 scripts/start_ocp_easy.py
```

```text
http://HOST_IP:8421/app#ocp_operator_token=change-me
```

On Windows PowerShell, set environment variables before starting Python:

```powershell
$env:OCP_HOST="0.0.0.0"; $env:OCP_NODE_ID="beta-node"; $env:OCP_DISPLAY_NAME="Beta"; python scripts/start_ocp_easy.py
```

Do not add a trailing `#` after flags in PowerShell. For example, use `--no-open-browser`, not `--no-open-browser#`.

If you are testing the UI from another machine and the deck is empty, seed activity against the LAN URL:

```bash
python3 scripts/seed_control_demo.py --base-url http://HOST_IP:8421
```

Set a custom node identity:

```bash
OCP_NODE_ID=alpha-node OCP_DISPLAY_NAME=Alpha ./scripts/start_ocp.sh
```

## First multi-machine test

On machine one:

```bash
OCP_HOST=0.0.0.0 OCP_NODE_ID=alpha-node OCP_DISPLAY_NAME=Alpha python3 scripts/start_ocp_easy.py
```

On machine two:

```bash
OCP_HOST=0.0.0.0 OCP_PORT=8422 OCP_NODE_ID=beta-node OCP_DISPLAY_NAME=Beta python3 scripts/start_ocp_easy.py
```

Then open `http://HOST_IP:8421/` on each machine, choose the `Setup Details` tab, use `Connect Everything`, then `Test Whole Mesh`.
For the polished flow, open `/app` from the phone and press `Activate Mesh`; it will scan, probe routes, plan safe helpers, run a whole-mesh proof, and explain what happened.
Full laptop/workstation nodes started with `scripts/start_ocp_easy.py` or the Mac launcher advertise a default worker automatically, so the app can also show execution readiness and run the scheduler-backed `Run on Best Device` demo. The native Mac app records local app-status samples through `/mesh/app/history/sample` and renders charts from `/mesh/app/history`.

Private proof artifacts stay protected. To replicate a private artifact from another node, use the app’s `Replicate Proof Artifact` action or send `remote_auth` explicitly:

```json
{
  "peer_id": "beta-node",
  "artifact_id": "REMOTE_ARTIFACT_ID",
  "pin": true,
  "remote_auth": {"type": "operator_token", "token": "BETA_OPERATOR_TOKEN"}
}
```

OCP uses that remote token only for the outbound fetch, records redacted audit metadata, verifies the digest, and does not store or echo the token.

If scan does not immediately find the other machine, use `Copy My Easy Link` on one computer and paste that address into the manual connect box on the other one.
You can also scan the QR code from the easy page on the other device and open the pairing link that way.

## Notes

- OCP is standalone.
- Personal Mirror can integrate with it, but is not required to run it.
- The main operator app is `/`.
- The app status API is `/mesh/app/status`.
- The app status API includes setup timeline, execution readiness, artifact sync, and protocol status.
- The app history APIs are `GET /mesh/app/history` and `POST /mesh/app/history/sample`; they are local operator/app-facing chart surfaces.
- Autonomic Mesh APIs are `/mesh/autonomy/status`, `/mesh/autonomy/activate`, `/mesh/routes/health`, and `/mesh/routes/probe`.
- Artifact replication APIs are `/mesh/artifacts/replicate` and `/mesh/artifacts/replicate-graph`; private remote pulls require explicit operator-mediated auth for now.
- The easy setup module remains at `/easy`.
- The advanced deck module remains at `/control`.
