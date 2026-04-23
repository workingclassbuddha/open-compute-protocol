# OCP v0.1.4 Release Notes

Date: 2026-04-23

`v0.1.4` is the Desktop Alpha RC release. It keeps the Python-first Sovereign Mesh runtime and SQLite substrate intact while making OCP feel more like one local-first personal compute app.

## Highlights

- Autonomic Mesh alpha: `Activate Autonomic Mesh` scans nearby devices, probes working routes, plans safe helpers, runs a whole-mesh proof, retries once after route repair, and returns plain-language summaries.
- Route health is first-class: OCP records route candidates, prefers recently proven reachable URLs, and exposes route state at `GET /mesh/routes/health` and `POST /mesh/routes/probe`.
- Polished app home: `/` and `/app` now show a Today panel with mesh strength, route health, latest proof state, next actions, phone link/QR, and direct activation.
- Mac beta launcher: `python3 -m ocp_desktop.launcher` starts Local Only or Mesh Mode nodes with state under `~/Library/Application Support/OCP/`.
- Unsigned Mac beta bundle: `python3 scripts/build_macos_app.py` creates `dist/OCP.app` and excludes local state, identities, DB files, `.git`, caches, and common secret files.
- Operator hardening: raw `/mesh/*` mutation routes require loopback access or `OCP_OPERATOR_TOKEN`; signed peer routes remain signed-envelope based.
- Safer execution defaults: host environment inheritance is off by default and can be enabled deliberately with `env_policy.inherit_env_allowlist`.
- Envelope crypto now uses the dependency-free Ed25519 implementation identified by `ed25519-sha512-v1`.

## Public Surfaces

- App shell: `GET /` and `GET /app`
- App status: `GET /mesh/app/status`
- Autonomic Mesh: `GET /mesh/autonomy/status` and `POST /mesh/autonomy/activate`
- Route health: `GET /mesh/routes/health` and `POST /mesh/routes/probe`
- Desktop launcher config: `~/Library/Application Support/OCP/launcher.json`
- Desktop launcher state: `~/Library/Application Support/OCP/state/`

## Upgrade Notes

- Existing local identities created with the older signature scheme may be regenerated on first start. Reconnect/re-pair trusted peers if an old node identity no longer matches.
- Phone/LAN POST actions need operator auth. The Mac launcher copies phone links with `#ocp_operator_token=...` so the browser can store the token locally and send `X-OCP-Operator-Token`.
- Manual LAN starts should set `OCP_OPERATOR_TOKEN` and open `http://HOST_IP:8421/app#ocp_operator_token=YOUR_TOKEN` from the phone.
- Private artifact content is no longer fetchable from off-loopback clients unless the request is operator-authenticated or the artifact policy is public.
- Jobs that relied on inherited host environment variables must declare `env_policy.inherit_host_env` and/or `env_policy.inherit_env_allowlist`.
- The Mac app is unsigned and not notarized in this release. It requires a local `python3` installation.

## Verification

Release candidates should pass:

```bash
git diff --check
python3 scripts/check_protocol_conformance.py
python3 -m unittest tests.test_sovereign_mesh -q
python3 server.py --help
./scripts/start_ocp.sh --help
python3 scripts/start_ocp_easy.py --help
python3 scripts/build_macos_app.py --help
python3 -m ocp_desktop.launcher --plan local
```

Manual RC demo:

1. Start Alpha in Mesh Mode.
2. Open the copied phone link on the same Wi-Fi.
3. Connect Beta or a spare laptop.
4. Press `Activate Autonomic Mesh`.
5. Confirm the proof completes and the app reports the mesh as strong.
6. Restart or kill Beta, activate again, and confirm OCP either repairs the route or gives one concrete fix.
