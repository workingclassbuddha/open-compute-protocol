# OCP v0.1.7 Trustworthy Alpha Roadmap

No major new concepts. Stabilize packaging, security docs, protocol contract, tests, and demo flow.

## 1. Goal

Make OCP easier to install, safer to understand, easier to test, and more credible as a protocol boundary while preserving the current v0.1.6 Desktop Alpha behavior.

## 2. Non-goals

- no large product features
- no route removals or public API renames
- no server replacement
- no required database beyond SQLite
- no cloud dependencies or external brokers
- no production-security claims
- no claim that OCP v0.1 is stable

## 3. Release Checklist

- packaging metadata exists
- editable install works
- operator auth and LAN safety docs exist
- threat model exists
- HTTP contract export script exists
- generated contract JSON is available
- HTTP API overview points to the code-owned contract
- test subsystem directories exist for future focused tests
- two-machine plus phone demo is documented
- README and Quickstart link the new stabilization docs

## 4. Packaging Work

- Add `pyproject.toml` for the current flat Python layout.
- Keep dependencies empty unless the runtime already requires them.
- Add `ocp = "server:main"`.
- Add `ocp-easy = "scripts.start_ocp_easy:main"` because `scripts/start_ocp_easy.py` exposes a clean `main()` function.
- Add `MANIFEST.in` to include docs, scripts, assets, Swift sources, tests, license, README, and security policy.

## 5. Security/Auth Work

- Document loopback fallback behavior.
- Document token-mode behavior and accepted headers.
- Explain why phone links use URL fragments.
- Warn about LAN binding, operator token leakage, remote artifact auth, executor risk, Docker mounts, and secrets in payloads or environment.

## 6. Protocol/Contract Work

- Keep `server_contract.py` as the code-owned source of truth.
- Add `scripts/export_contract.py`.
- Generate `docs/generated/OCP_CONTRACT_v0.1.json`.
- Keep `scripts/check_protocol_conformance.py` as the conformance smoke check.

## 7. Test-structure Work

- Preserve `tests.test_sovereign_mesh` as the broad regression baseline.
- Add subsystem directories for future focused tests.
- Prefer protocol and conformance tests that use `server_contract.py` and `mesh_protocol` schemas.

## 8. Demo Work

- Add a two-Macs-and-phone walkthrough.
- Use existing `scripts/start_ocp_easy.py` commands.
- Include LAN operator token guidance.
- Include troubleshooting for peer discovery, firewall, port, token, worker, deck, and artifact issues.

## 9. Known Risks

- The schema registry is descriptive and only partially enforcing.
- Signed scoped capability grants are schema-defined but not fully enforced yet.
- Operator-token remote artifact auth is an alpha bridge.
- Worker execution can run powerful local code.
- LAN demos depend on local firewall and router behavior.
- Swift build/test availability depends on local Xcode and SwiftPM setup.

## 10. Definition of Done

- `python3 -m pip install -e .`
- `python3 scripts/check_protocol_conformance.py`
- `python3 -m unittest tests.test_sovereign_mesh`
- `python3 server.py --help`
- `docs/SECURITY_MODEL.md` exists
- `docs/OPERATOR_AUTH.md` exists
- `docs/THREAT_MODEL.md` exists
- generated contract JSON exists or script exists to generate it
- two-device demo doc exists
