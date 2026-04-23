# Quickstart

This is the fastest way to get **The Open Compute Protocol** running locally.

## 1. Clone the repo

```bash
git clone https://github.com/workingclassbuddha/open-compute-protocol.git
cd open-compute-protocol
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

Use `Start Local Only` for a private node or `Start Mesh Mode` when you want your phone or spare laptop on the same Wi-Fi to connect. The launcher stores its beta app state under `~/Library/Application Support/OCP/`.
In Mesh Mode, `Copy Phone Link` includes a private operator token in the URL fragment so the phone app can safely run OCP actions such as `Activate Autonomic Mesh`.

Build the unsigned Mac beta bundle:

```bash
python3 scripts/build_macos_app.py
open dist/OCP.app
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

Then open `http://HOST_IP:8421/` on each machine, choose the `Setup` tab, use `Connect Everything`, then `Test Whole Mesh`.
For the polished flow, open `/app` from the phone and press `Activate Autonomic Mesh`; it will scan, probe routes, plan safe helpers, run a whole-mesh proof, and explain what happened.

If scan does not immediately find the other machine, use `Copy My Easy Link` on one computer and paste that address into the manual connect box on the other one.
You can also scan the QR code from the easy page on the other device and open the pairing link that way.

## Notes

- OCP is standalone.
- Personal Mirror can integrate with it, but is not required to run it.
- The main operator app is `/`.
- The app status API is `/mesh/app/status`.
- Autonomic Mesh APIs are `/mesh/autonomy/status`, `/mesh/autonomy/activate`, `/mesh/routes/health`, and `/mesh/routes/probe`.
- The easy setup module remains at `/easy`.
- The advanced deck module remains at `/control`.
