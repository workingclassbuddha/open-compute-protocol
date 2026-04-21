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
- opens the easy setup page automatically in your browser
- keeps the advanced deck available at `/control`

If you prefer the shell starter instead:

```bash
./scripts/start_ocp.sh
```

That script:

- creates a local state directory under `./.local/ocp`
- creates identity and workspace folders
- starts the standalone OCP server with sensible defaults

By default, the node comes up on:

- easy setup: `http://127.0.0.1:8421/`
- advanced control deck: `http://127.0.0.1:8421/control`
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

Then open `http://HOST_IP:8421/` on each machine and use `Connect Everything`, then `Test Whole Mesh`.

If scan does not immediately find the other machine, use `Copy My Easy Link` on one computer and paste that address into the manual connect box on the other one.
You can also scan the QR code from the easy page on the other device and open the pairing link that way.

## Notes

- OCP is standalone.
- Personal Mirror can integrate with it, but is not required to run it.
- The easiest operator surface is `/`.
- The advanced deck remains at `/control`.
