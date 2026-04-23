from __future__ import annotations

from typing import Any

from mesh import SovereignMesh
from server_control import build_control_bootstrap


def build_easy_bootstrap(mesh: SovereignMesh) -> str:
    return build_control_bootstrap(mesh)


def list_discovery_candidates(
    mesh: SovereignMesh,
    *,
    limit: int = 25,
    status: str = "",
) -> dict[str, Any]:
    return mesh.list_discovery_candidates(limit=limit, status=status)


def seek_discovery_peers(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.seek_peers(
        base_urls=list(data.get("base_urls") or []),
        hosts=list(data.get("hosts") or []),
        cidr=(data.get("cidr") or "").strip(),
        port=int(data.get("port") or 8421),
        trust_tier=(data.get("trust_tier") or "trusted").strip(),
        auto_connect=bool(data.get("auto_connect", False)),
        include_self=bool(data.get("include_self", False)),
        limit=int(data.get("limit") or 32),
        timeout=float(data.get("timeout") or 2.0),
        refresh_known=bool(data.get("refresh_known", True)),
    )


def scan_local_peers(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.scan_local_peers(
        trust_tier=(data.get("trust_tier") or "trusted").strip(),
        timeout=float(data.get("timeout") or 0.8),
        limit=int(data.get("limit") or 24),
        port=int(data.get("port") or 0),
    )


def connectivity_diagnostics(mesh: SovereignMesh, *, limit: int = 24) -> dict[str, Any]:
    return mesh.connectivity_diagnostics(limit=limit)


def connect_peer(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.connect_device(
        base_url=(data.get("base_url") or "").strip(),
        peer_id=(data.get("peer_id") or "").strip(),
        trust_tier=(data.get("trust_tier") or "trusted").strip(),
        timeout=float(data.get("timeout") or 3.0),
        refresh_manifest=bool(data.get("refresh_manifest", True)),
    )


def connect_all_peers(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.connect_all_devices(
        trust_tier=(data.get("trust_tier") or "trusted").strip(),
        timeout=float(data.get("timeout") or 3.0),
        scan_timeout=float(data.get("scan_timeout") or 0.8),
        limit=int(data.get("limit") or 24),
        port=int(data.get("port") or 0),
        refresh_manifest=bool(data.get("refresh_manifest", True)),
    )


def sync_peer(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.sync_peer(
        (data.get("peer_id") or "").strip(),
        limit=int(data.get("limit") or 50),
    )


def launch_test_mission(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.launch_test_mission(
        peer_id=(data.get("peer_id") or "").strip(),
        base_url=(data.get("base_url") or "").strip(),
        trust_tier=(data.get("trust_tier") or "trusted").strip(),
        timeout=float(data.get("timeout") or 3.0),
        request_id=(data.get("request_id") or "").strip() or None,
    )


def launch_mesh_test_mission(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.launch_mesh_test_mission(
        include_local=bool(data.get("include_local", True)),
        limit=int(data.get("limit") or 24),
        request_id=(data.get("request_id") or "").strip() or None,
    )


EASY_PAGE_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <title>OCP Easy Setup</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Space+Grotesk:wght@500;600;700&display=swap" rel="stylesheet">
  <script defer src="https://cdn.jsdelivr.net/npm/qrcode@1.5.4/build/qrcode.min.js"></script>
  <style>
    :root {
      --bg: #f7f0e6;
      --paper: #fffaf3;
      --paper-strong: #fffdfa;
      --ink: #132132;
      --muted: #5c6779;
      --line: rgba(19, 33, 50, 0.12);
      --cyan: #006f92;
      --gold: #b6772e;
      --green: #1c8c4d;
      --amber: #bd6f00;
      --red: #b7443c;
      --radius-lg: 28px;
      --radius-md: 18px;
      --shadow: 0 20px 50px rgba(46, 33, 12, 0.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background:
        radial-gradient(circle at top right, rgba(0, 111, 146, 0.12), transparent 24%),
        radial-gradient(circle at top left, rgba(182, 119, 46, 0.14), transparent 22%),
        linear-gradient(180deg, #fbf6ef 0%, var(--bg) 100%);
      color: var(--ink);
      font-family: "Inter", sans-serif;
    }
    a { color: inherit; }
    button, input { font: inherit; }
    button { cursor: pointer; }
    .easy-app {
      max-width: 1180px;
      margin: 0 auto;
      padding: 22px 18px 88px;
    }
    .easy-hero {
      display: grid;
      gap: 18px;
      padding: 26px;
      border-radius: 32px;
      background: linear-gradient(145deg, rgba(255, 253, 250, 0.98), rgba(255, 247, 236, 0.95));
      border: 1px solid rgba(19, 33, 50, 0.08);
      box-shadow: var(--shadow);
    }
    .easy-kicker {
      color: var(--gold);
      font-size: 12px;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      font-weight: 700;
    }
    .easy-title {
      margin: 0;
      font-family: "Space Grotesk", sans-serif;
      font-size: clamp(34px, 7vw, 58px);
      line-height: 0.98;
    }
    .easy-lead {
      margin: 0;
      max-width: 56rem;
      color: var(--muted);
      font-size: 18px;
      line-height: 1.65;
    }
    .easy-steps {
      display: grid;
      gap: 12px;
    }
    .easy-step {
      display: grid;
      grid-template-columns: 42px minmax(0, 1fr);
      gap: 12px;
      align-items: start;
      padding: 14px 16px;
      border-radius: var(--radius-md);
      background: rgba(19, 33, 50, 0.04);
      border: 1px solid rgba(19, 33, 50, 0.06);
    }
    .easy-step__number {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 42px;
      height: 42px;
      border-radius: 999px;
      background: rgba(0, 111, 146, 0.1);
      color: var(--cyan);
      font-weight: 700;
    }
    .easy-step strong {
      display: block;
      font-size: 16px;
      margin-bottom: 4px;
    }
    .easy-step span {
      color: var(--muted);
      line-height: 1.6;
      font-size: 14px;
    }
    .easy-layout {
      display: grid;
      gap: 18px;
      margin-top: 18px;
    }
    .easy-panel {
      display: grid;
      gap: 16px;
      padding: 22px;
      border-radius: var(--radius-lg);
      background: var(--paper);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
    }
    .easy-panel__head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 14px;
      flex-wrap: wrap;
    }
    .easy-panel__title {
      margin: 0;
      font-family: "Space Grotesk", sans-serif;
      font-size: 28px;
      line-height: 1.1;
    }
    .easy-panel__copy {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 15px;
      line-height: 1.65;
      max-width: 52rem;
    }
    .easy-toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .easy-button, .easy-link {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      min-height: 52px;
      padding: 0 18px;
      border-radius: 16px;
      border: 1px solid rgba(19, 33, 50, 0.12);
      background: white;
      color: var(--ink);
      text-decoration: none;
      transition: transform 180ms ease, box-shadow 180ms ease, border-color 180ms ease;
    }
    .easy-button:hover, .easy-link:hover {
      transform: translateY(-1px);
      box-shadow: 0 12px 24px rgba(19, 33, 50, 0.08);
      border-color: rgba(19, 33, 50, 0.18);
    }
    .easy-button--primary {
      background: linear-gradient(135deg, #0f6f92, #0b5f7c);
      color: white;
      border-color: rgba(0, 111, 146, 0.4);
    }
    .easy-button--gold {
      background: linear-gradient(135deg, #c1893e, #af6f22);
      color: white;
      border-color: rgba(182, 119, 46, 0.4);
    }
    .easy-button--soft {
      background: rgba(19, 33, 50, 0.04);
    }
    .easy-input {
      width: 100%;
      min-height: 56px;
      padding: 0 18px;
      border-radius: 16px;
      border: 1px solid rgba(19, 33, 50, 0.12);
      background: var(--paper-strong);
      color: var(--ink);
    }
    .easy-input::placeholder {
      color: rgba(19, 33, 50, 0.4);
    }
    .easy-manual {
      display: grid;
      gap: 12px;
    }
    .easy-manual__row {
      display: grid;
      gap: 10px;
    }
    .easy-note {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.6;
    }
    .easy-status {
      padding: 14px 16px;
      border-radius: var(--radius-md);
      background: rgba(0, 111, 146, 0.08);
      color: #0b556f;
      font-weight: 600;
    }
    .easy-pill-row {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .easy-pill {
      display: inline-flex;
      align-items: center;
      min-height: 34px;
      padding: 0 12px;
      border-radius: 999px;
      background: rgba(19, 33, 50, 0.05);
      color: var(--muted);
      font-size: 12px;
      border: 1px solid rgba(19, 33, 50, 0.06);
    }
    .easy-card-grid {
      display: grid;
      gap: 12px;
    }
    .easy-card {
      display: grid;
      gap: 12px;
      padding: 18px;
      border-radius: 20px;
      background: white;
      border: 1px solid rgba(19, 33, 50, 0.08);
    }
    .easy-card__head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
    }
    .easy-card__title {
      margin: 0;
      font-family: "Space Grotesk", sans-serif;
      font-size: 20px;
    }
    .easy-card__url {
      margin-top: 4px;
      color: var(--muted);
      font-size: 13px;
      word-break: break-all;
    }
    .easy-card__copy {
      color: var(--muted);
      line-height: 1.6;
      font-size: 14px;
    }
    .easy-badge {
      display: inline-flex;
      align-items: center;
      min-height: 30px;
      padding: 0 12px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      border: 1px solid transparent;
    }
    .easy-badge--ready {
      color: var(--green);
      background: rgba(28, 140, 77, 0.12);
      border-color: rgba(28, 140, 77, 0.24);
    }
    .easy-badge--warn {
      color: var(--amber);
      background: rgba(189, 111, 0, 0.12);
      border-color: rgba(189, 111, 0, 0.24);
    }
    .easy-badge--error {
      color: var(--red);
      background: rgba(183, 68, 60, 0.12);
      border-color: rgba(183, 68, 60, 0.24);
    }
    .easy-badge--calm {
      color: var(--cyan);
      background: rgba(0, 111, 146, 0.12);
      border-color: rgba(0, 111, 146, 0.24);
    }
    .easy-card__actions {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .easy-empty {
      padding: 24px;
      border-radius: 18px;
      border: 1px dashed rgba(19, 33, 50, 0.14);
      background: rgba(19, 33, 50, 0.02);
      color: var(--muted);
      text-align: center;
      line-height: 1.7;
    }
    .easy-errors {
      display: grid;
      gap: 10px;
    }
    .easy-share {
      display: grid;
      gap: 12px;
      padding: 16px;
      border-radius: 18px;
      background: rgba(19, 33, 50, 0.03);
      border: 1px solid rgba(19, 33, 50, 0.08);
    }
    .easy-share__label {
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }
    .easy-share__url {
      padding: 14px 16px;
      border-radius: 16px;
      background: white;
      border: 1px solid rgba(19, 33, 50, 0.08);
      color: var(--ink);
      font-family: monospace;
      font-size: 14px;
      line-height: 1.6;
      word-break: break-all;
    }
    .easy-qr {
      display: grid;
      justify-items: center;
      gap: 10px;
      padding: 14px;
      border-radius: 18px;
      background: rgba(19, 33, 50, 0.035);
      border: 1px solid rgba(19, 33, 50, 0.08);
    }
    .easy-qr__frame {
      display: grid;
      place-items: center;
      width: 230px;
      min-height: 230px;
      padding: 10px;
      border-radius: 20px;
      background: white;
      border: 1px solid rgba(19, 33, 50, 0.08);
      box-shadow: inset 0 1px 0 rgba(19, 33, 50, 0.03);
    }
    .easy-qr__frame img {
      display: block;
      width: 210px;
      height: 210px;
    }
    .easy-qr__note {
      color: var(--muted);
      text-align: center;
      font-size: 13px;
      line-height: 1.6;
      max-width: 22rem;
    }
    .easy-checklist {
      display: grid;
      gap: 10px;
      margin: 0;
      padding-left: 18px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.7;
    }
    .easy-error {
      padding: 14px 16px;
      border-radius: 16px;
      background: rgba(183, 68, 60, 0.08);
      color: #7a2b27;
      border: 1px solid rgba(183, 68, 60, 0.14);
      line-height: 1.6;
      font-size: 13px;
    }
    .easy-footer {
      margin-top: 18px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.7;
    }
    @media (min-width: 760px) {
      .easy-steps {
        grid-template-columns: repeat(3, minmax(0, 1fr));
      }
      .easy-manual__row {
        grid-template-columns: minmax(0, 1fr) auto auto;
      }
    }
    @media (min-width: 1040px) {
      .easy-layout {
        grid-template-columns: minmax(0, 1.18fr) minmax(320px, 0.82fr);
      }
      .easy-card-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }
  </style>
</head>
<body>
  <div class="easy-app">
    <section class="easy-hero">
      <div class="easy-kicker">OCP Easy Setup</div>
      <h1 class="easy-title">Connect two computers without becoming the network department.</h1>
      <p class="easy-lead">Open this page on both computers. On one computer, press <strong>Connect Everything</strong> to scan and join every reachable trusted device in one go. Then press <strong>Test Whole Mesh</strong> to prove the mesh can execute across all connected devices as one system.</p>
      <div class="easy-steps">
        <article class="easy-step">
          <span class="easy-step__number">1</span>
          <div>
            <strong>Open this page on both computers</strong>
            <span>Every node has the same easy setup page. No raw JSON, no scripts, no terminal copy-paste needed for normal use.</span>
          </div>
        </article>
        <article class="easy-step">
          <span class="easy-step__number">2</span>
          <div>
            <strong>Press Connect Everything</strong>
            <span>OCP scans nearby nodes, remembers what it finds, and connects every reachable trusted device it can reach without making you do them one by one.</span>
          </div>
        </article>
        <article class="easy-step">
          <span class="easy-step__number">3</span>
          <div>
            <strong>Press Test Whole Mesh</strong>
            <span>That launches one cooperative proof mission across the devices currently in your mesh so you can verify the whole fabric, not just one helper.</span>
          </div>
        </article>
      </div>
    </section>

    <div class="easy-layout">
      <section class="easy-panel">
        <div class="easy-panel__head">
          <div>
            <h2 class="easy-panel__title">Nearby Computers</h2>
            <p class="easy-panel__copy">The common case should be simple: start OCP on every machine, press one button, and let OCP pull the nearby mesh together for you.</p>
          </div>
          <div class="easy-toolbar">
            <button class="easy-button easy-button--primary" id="scan-button" type="button">Scan Nearby</button>
            <button class="easy-button easy-button--gold" id="connect-all-button" type="button">Connect Everything</button>
            <button class="easy-button easy-button--soft" id="test-whole-mesh-button" type="button">Test Whole Mesh</button>
            <a class="easy-link easy-button--soft" href="/control">Open Advanced Deck</a>
          </div>
        </div>

        <div class="easy-status" id="easy-status">Easy setup is ready.</div>
        <div class="easy-pill-row" id="easy-local-summary"></div>

        <div class="easy-manual">
          <div class="easy-manual__row">
            <input class="easy-input" id="manual-url" type="text" placeholder="If needed, paste an address like http://172.20.10.4:8431">
            <button class="easy-button easy-button--primary" id="manual-connect" type="button">Connect</button>
            <button class="easy-button easy-button--gold" id="manual-test" type="button">Send Test Mission</button>
          </div>
          <div class="easy-note">Manual address entry is the fallback. Most of the time, <strong>Scan Nearby</strong> should find the other computer for you.</div>
        </div>

        <div class="easy-card-grid" id="easy-card-grid"></div>
      </section>

      <aside class="easy-panel">
        <div>
          <h2 class="easy-panel__title">This Computer</h2>
          <p class="easy-panel__copy">These are the addresses and diagnostics you can share if another computer needs help finding this node.</p>
        </div>
        <div class="easy-share">
          <div class="easy-share__label">Share This Easy Link</div>
          <div class="easy-share__url" id="easy-share-url">Loading share link...</div>
          <div class="easy-toolbar">
            <button class="easy-button easy-button--primary" id="copy-share-url" type="button">Copy My Easy Link</button>
          </div>
        </div>
        <div class="easy-qr">
          <div class="easy-share__label">Scan This QR Code</div>
          <div class="easy-qr__frame" id="easy-qr-frame">Preparing QR code...</div>
          <div class="easy-qr__note" id="easy-qr-note">Open your phone camera or the other computer's QR scanner and point it at this code. If the QR preview does not load, use Copy My Easy Link instead.</div>
        </div>
        <div class="easy-pill-row" id="easy-addresses"></div>
        <ul class="easy-checklist" id="easy-checklist"></ul>
        <div class="easy-errors" id="easy-errors"></div>
        <div class="easy-footer">
          If another computer still cannot connect, the usual cause is firewall or network isolation. This page shows the most recent reachability errors so you do not have to guess what went wrong.
        </div>
      </aside>
    </div>
  </div>

  <script>
    const OCP_EASY_BOOTSTRAP = __OCP_EASY_BOOTSTRAP__;
    const OCP_OPERATOR_TOKEN_KEY = "ocp_operator_token";
    const easyApp = {
      state: OCP_EASY_BOOTSTRAP,
      refreshTimer: null
    };

    function consumeOperatorToken() {
      const hash = String(window.location.hash || "");
      let token = "";
      if (hash.indexOf("#ocp_operator_token=") === 0) {
        token = decodeURIComponent(hash.slice("#ocp_operator_token=".length));
      } else if (hash.indexOf("ocp_operator_token=") !== -1) {
        token = new URLSearchParams(hash.replace(/^#/, "")).get("ocp_operator_token") || "";
      }
      if (token) {
        try {
          window.localStorage.setItem(OCP_OPERATOR_TOKEN_KEY, token);
        } catch (error) {
        }
        history.replaceState(null, "", window.location.pathname + window.location.search);
      }
    }

    function operatorToken() {
      try {
        return String(window.localStorage.getItem(OCP_OPERATOR_TOKEN_KEY) || "").trim();
      } catch (error) {
        return "";
      }
    }

    function withOperatorAuth(options) {
      const next = Object.assign({}, options || {});
      const headers = new Headers(next.headers || {});
      const token = operatorToken();
      if (token && !headers.has("X-OCP-Operator-Token")) {
        headers.set("X-OCP-Operator-Token", token);
      }
      next.headers = headers;
      return next;
    }

    function withOperatorFragment(url) {
      const token = operatorToken();
      const target = String(url || "");
      if (!token || !target) {
        return target;
      }
      return target.replace(/#.*$/, "") + "#ocp_operator_token=" + encodeURIComponent(token);
    }

    consumeOperatorToken();

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"']/g, function (token) {
        return {
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;"
        }[token] || token;
      });
    }

    function compactUrl(value) {
      return String(value || "").replace(/^https?:\\/\\//, "");
    }

    function normalizedUrl(rawValue) {
      let token = String(rawValue || "").trim();
      if (!token) {
        return "";
      }
      if (token.indexOf("://") === -1) {
        token = "http://" + token;
      }
      return token.replace(/\\/+$/, "");
    }

    function easyRootUrl(value) {
      const token = normalizedUrl(value || "");
      if (!token) {
        return normalizedUrl(window.location.origin || "") + "/";
      }
      return token + "/";
    }

    function setStatus(text) {
      const target = document.getElementById("easy-status");
      if (target) {
        target.textContent = text;
      }
    }

    function treatyAdvisoryAction(advisory) {
      const payload = advisory || {};
      if (payload.recommended_action) {
        return payload.recommended_action;
      }
      const compatibility = payload.treaty_compatibility || payload;
      const state = String(compatibility.advisory_state || "limited").toLowerCase();
      if (state === "full") {
        return "Use this peer for treaty-aware continuity and custody review.";
      }
      if (state === "advisory") {
        return "Use this peer for treaty-aware visibility, but choose a custody-capable peer for protected restores.";
      }
      return "Keep this peer on normal sync until it advertises treaty validation.";
    }

    async function fetchJson(url, options) {
      const response = await fetch(url, withOperatorAuth(options));
      if (!response.ok) {
        let detail = response.statusText || "request failed";
        try {
          const payload = await response.json();
          detail = payload.error || payload.message || detail;
        } catch (error) {
        }
        throw new Error(detail);
      }
      return response.json();
    }

    async function copyText(text) {
      const token = String(text || "");
      if (!token) {
        throw new Error("nothing to copy");
      }
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(token);
        return;
      }
      const input = document.createElement("textarea");
      input.value = token;
      input.setAttribute("readonly", "readonly");
      input.style.position = "absolute";
      input.style.left = "-9999px";
      document.body.appendChild(input);
      input.select();
      document.execCommand("copy");
      document.body.removeChild(input);
    }

    function renderEasyQr(url) {
      const frame = document.getElementById("easy-qr-frame");
      const note = document.getElementById("easy-qr-note");
      if (!frame) {
        return;
      }
      frame.textContent = "Preparing QR code...";
      if (!(window.QRCode && typeof window.QRCode.toDataURL === "function")) {
        frame.textContent = "QR preview is still loading.";
        if (note) {
          note.textContent = "If the QR preview does not appear, use Copy My Easy Link instead. The pairing link is already shown above.";
        }
        return;
      }
      window.QRCode.toDataURL(url, {
        margin: 1,
        width: 210,
        color: {
          dark: "#132132",
          light: "#fffdfa"
        }
      }, function (error, dataUrl) {
        if (error || !dataUrl) {
          frame.textContent = "QR preview unavailable.";
          if (note) {
            note.textContent = "The QR generator could not render here. Use Copy My Easy Link instead.";
          }
          return;
        }
        frame.innerHTML = '<img alt="OCP easy pairing QR" src="' + dataUrl + '">';
        if (note) {
          note.textContent = "Open your phone camera or the other computer's QR scanner and point it at this code.";
        }
      });
    }

    function badgeClass(status) {
      const token = String(status || "").toLowerCase();
      if (["connected", "ready", "ok"].includes(token)) {
        return "easy-badge easy-badge--ready";
      }
      if (["error", "failed", "refused", "blocked"].includes(token)) {
        return "easy-badge easy-badge--error";
      }
      if (["discovered", "pending", "scanned", "degraded"].includes(token)) {
        return "easy-badge easy-badge--warn";
      }
      return "easy-badge easy-badge--calm";
    }

    function relativeTime(value) {
      const token = String(value || "").trim();
      if (!token) {
        return "just now";
      }
      const when = Date.parse(token);
      if (!when) {
        return token;
      }
      const seconds = Math.round((Date.now() - when) / 1000);
      const absSeconds = Math.abs(seconds);
      if (absSeconds < 60) {
        return seconds >= 0 ? "just now" : "in a moment";
      }
      const minutes = Math.round(absSeconds / 60);
      if (minutes < 60) {
        return seconds >= 0 ? minutes + " min ago" : "in " + minutes + " min";
      }
      const hours = Math.round(minutes / 60);
      if (hours < 24) {
        return seconds >= 0 ? hours + " hr ago" : "in " + hours + " hr";
      }
      const days = Math.round(hours / 24);
      return seconds >= 0 ? days + " day ago" : "in " + days + " day";
    }

    function renderEasy() {
      const state = easyApp.state || {};
      const peers = ((state.peers || {}).peers) || [];
      const candidates = ((state.discovery_candidates || {}).candidates) || [];
      const connectivity = state.connectivity || {};
      const shareUrlValue = withOperatorFragment(easyRootUrl(connectivity.share_url || connectivity.base_url));
      const lanUrls = (connectivity.lan_urls || []).map(function (item) {
        return withOperatorFragment(easyRootUrl(item));
      });
      const localSummary = document.getElementById("easy-local-summary");
      const localAddresses = document.getElementById("easy-addresses");
      const shareUrl = document.getElementById("easy-share-url");
      const checklist = document.getElementById("easy-checklist");
      const errors = document.getElementById("easy-errors");
      const grid = document.getElementById("easy-card-grid");
      const connectedPeerIds = new Set(peers.map(function (peer) {
        return String(peer.peer_id || "");
      }).filter(Boolean));
      const cards = [];

      peers.forEach(function (peer) {
        const profile = peer.device_profile || {};
        const compatibility = peer.treaty_compatibility || {};
        cards.push({
          key: "peer:" + String(peer.peer_id || ""),
          title: peer.display_name || peer.peer_id || "Connected computer",
          baseUrl: peer.endpoint_url || "",
          peerId: peer.peer_id || "",
          status: peer.status || "connected",
          copy: "This computer is already part of your mesh. " + String(compatibility.summary || "You can send a proof mission right now."),
          meta: [
            profile.device_class ? String(profile.device_class).toUpperCase() : "",
            profile.form_factor ? String(profile.form_factor).toUpperCase() : "",
            compatibility.remote_custody_review ? "CUSTODY READY" : "VALIDATION READY",
            "last seen " + relativeTime(peer.last_seen_at || peer.updated_at)
          ].filter(Boolean),
          connected: true
        });
      });

      candidates.forEach(function (candidate) {
        const peerId = String(candidate.peer_id || "");
        if (peerId && connectedPeerIds.has(peerId)) {
          return;
        }
        const profile = candidate.device_profile || {};
        const compatibility = candidate.treaty_compatibility || {};
        cards.push({
          key: "candidate:" + String(candidate.base_url || candidate.endpoint_url || ""),
          title: candidate.display_name || candidate.peer_id || compactUrl(candidate.endpoint_url || candidate.base_url || "") || "Discovered computer",
          baseUrl: candidate.endpoint_url || candidate.base_url || "",
          peerId: candidate.peer_id || "",
          status: candidate.status || "discovered",
          copy: candidate.last_error
            ? "Last problem: " + String(candidate.last_error)
            : (compatibility.summary || "Discovered and ready for one-click connect."),
          meta: [
            profile.device_class ? String(profile.device_class).toUpperCase() : "",
            profile.form_factor ? String(profile.form_factor).toUpperCase() : "",
            (candidate.treaty_capabilities || {}).continuity_validation ? "TREATY AWARE" : "",
            candidate.last_seen_at ? "seen " + relativeTime(candidate.last_seen_at) : ""
          ].filter(Boolean),
          connected: false
        });
      });

      localSummary.innerHTML = [
        shareUrlValue ? '<span class="easy-pill">' + escapeHtml("Share " + compactUrl(shareUrlValue)) + '</span>' : "",
        lanUrls.length ? '<span class="easy-pill">' + escapeHtml(String(lanUrls.length) + " LAN link(s) ready") + '</span>' : '<span class="easy-pill">' + escapeHtml("Local-only node") + '</span>',
        '<span class="easy-pill">' + escapeHtml(String(peers.length) + " connected computer(s)") + '</span>',
        '<span class="easy-pill">' + escapeHtml(String(candidates.length) + " discovered candidate(s)") + '</span>'
      ].filter(Boolean).join("");

      if (shareUrl) {
        shareUrl.textContent = shareUrlValue;
      }
      renderEasyQr(shareUrlValue);

      localAddresses.innerHTML = [
        connectivity.base_url ? '<span class="easy-pill">' + escapeHtml("Advertised " + compactUrl(connectivity.base_url)) + '</span>' : "",
        lanUrls.map(function (item) {
          return '<span class="easy-pill">' + escapeHtml("LAN " + compactUrl(item)) + '</span>';
        }).join(""),
        (connectivity.local_ipv4 || []).map(function (item) {
          return '<span class="easy-pill">' + escapeHtml("IP " + item) + '</span>';
        }).join("")
      ].filter(Boolean).join("");

      const recentErrors = connectivity.recent_errors || [];
      const checklistItems = [
        "Open this page on both computers and keep both of them on the same Wi-Fi.",
        "Press Scan Nearby first. If the other computer does not appear, copy your Easy Link and paste it into the other computer's manual connect box.",
        connectivity.share_advice || "",
        recentErrors.length
          ? "A recent connect attempt failed. The most common fix is allowing Python through the firewall on the other computer."
          : "If nothing shows up yet, the other computer may still be starting up or blocked by a firewall."
      ];
      if (!(connectivity.local_ipv4 || []).length) {
        checklistItems.push("This computer does not currently report a local IPv4 address. Check that it is connected to a real local network.");
      }
      checklist.innerHTML = checklistItems.filter(Boolean).map(function (item) {
        return "<li>" + escapeHtml(item) + "</li>";
      }).join("");

      errors.innerHTML = recentErrors.length ? recentErrors.map(function (item) {
        return '<div class="easy-error"><strong>' + escapeHtml(item.display_name || compactUrl(item.base_url || "")) + '</strong><br>' + escapeHtml(item.error || "reachability error") + '</div>';
      }).join("") : '<div class="easy-empty">No recent connect problems yet.</div>';

      grid.innerHTML = cards.length ? cards.map(function (card) {
        const connectLabel = card.connected ? "Reconnect" : "Connect";
        return '<article class="easy-card">' +
          '<div class="easy-card__head">' +
            '<div>' +
              '<h3 class="easy-card__title">' + escapeHtml(card.title) + '</h3>' +
              '<div class="easy-card__url">' + escapeHtml(compactUrl(card.baseUrl)) + '</div>' +
            '</div>' +
            '<span class="' + badgeClass(card.status) + '">' + escapeHtml(String(card.status || "ready").toUpperCase()) + '</span>' +
          '</div>' +
          '<div class="easy-pill-row">' + card.meta.map(function (item) {
            return '<span class="easy-pill">' + escapeHtml(item) + '</span>';
          }).join("") + '</div>' +
          '<div class="easy-card__copy">' + escapeHtml(card.copy) + '</div>' +
          '<div class="easy-card__actions">' +
            '<button class="easy-button easy-button--primary" type="button" data-action="connect" data-base-url="' + escapeHtml(card.baseUrl) + '" data-peer-id="' + escapeHtml(card.peerId) + '">' + escapeHtml(connectLabel) + '</button>' +
            '<button class="easy-button easy-button--gold" type="button" data-action="test" data-base-url="' + escapeHtml(card.baseUrl) + '" data-peer-id="' + escapeHtml(card.peerId) + '">Send Test Mission</button>' +
          '</div>' +
        '</article>';
      }).join("") : '<div class="easy-empty">No computers are visible yet. Start OCP on the other computer, then press <strong>Scan Nearby</strong>.</div>';
    }

    async function refreshEasy(options) {
      const silent = Boolean((options || {}).silent);
      const manifest = await fetchJson("/mesh/manifest");
      const peers = await fetchJson("/mesh/peers?limit=12");
      const candidates = await fetchJson("/mesh/discovery/candidates?limit=12");
      const connectivity = await fetchJson("/mesh/connectivity/diagnostics");
      easyApp.state = Object.assign({}, easyApp.state, {
        manifest: manifest,
        peers: peers,
        discovery_candidates: candidates,
        connectivity: connectivity
      });
      renderEasy();
      if (!silent) {
        setStatus("Easy setup refreshed.");
      }
    }

    async function scanNearby() {
      const result = await fetchJson("/mesh/discovery/scan-local", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ trust_tier: "trusted", timeout: 0.8, limit: 24 })
      });
      setStatus("Scan finished: " + String(result.discovered || 0) + " discovered, " + String(result.errors || 0) + " problem(s).");
      await refreshEasy({ silent: true });
    }

    async function connectComputer(payload) {
      const result = await fetchJson("/mesh/peers/connect", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const peer = result.peer || {};
      setStatus("Connected " + String(peer.display_name || peer.peer_id || "computer") + ". " + treatyAdvisoryAction(result.peer_advisory || peer));
      await refreshEasy({ silent: true });
    }

    async function connectEverything(payload) {
      const result = await fetchJson("/mesh/peers/connect-all", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(Object.assign({ trust_tier: "trusted", timeout: 3.0, scan_timeout: 0.8, limit: 24 }, payload || {}))
      });
      setStatus(
        result.operator_summary || (
          "Mesh connect complete: " +
          String(result.connected || 0) + " new, " +
          String(result.already_connected || 0) + " already ready, " +
          String(result.errors || 0) + " problem(s)."
        )
      );
      await refreshEasy({ silent: true });
    }

    async function sendTestMission(payload) {
      const result = await fetchJson("/mesh/missions/test-launch", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const mission = result.mission || {};
      setStatus("Test mission launched: " + String(mission.title || mission.id || "mission") + ".");
      await refreshEasy({ silent: true });
    }

    async function sendWholeMeshTest(payload) {
      const result = await fetchJson("/mesh/missions/test-mesh-launch", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(Object.assign({ include_local: true, limit: 24 }, payload || {}))
      });
      const mission = result.mission || {};
      const mesh = result.mesh || {};
      setStatus(
        "Whole mesh proof launched across " +
        String(mesh.peer_count || 0) +
        " device(s): " +
        String(mission.title || mission.id || "mission") +
        "."
      );
      await refreshEasy({ silent: true });
    }

    function manualUrl() {
      return normalizedUrl((document.getElementById("manual-url") || {}).value || "");
    }

    function initEasyActions() {
      document.getElementById("scan-button").addEventListener("click", function () {
        scanNearby().catch(function (error) {
          setStatus("Scan failed: " + error.message);
        });
      });
      document.getElementById("connect-all-button").addEventListener("click", function () {
        connectEverything().catch(function (error) {
          setStatus("Connect everything failed: " + error.message);
        });
      });
      document.getElementById("test-whole-mesh-button").addEventListener("click", function () {
        sendWholeMeshTest().catch(function (error) {
          setStatus("Whole mesh proof failed: " + error.message);
        });
      });
      document.getElementById("copy-share-url").addEventListener("click", function () {
        const connectivity = (easyApp.state || {}).connectivity || {};
        copyText(withOperatorFragment(easyRootUrl(connectivity.base_url))).then(function () {
          setStatus("Copied this computer's easy link.");
        }).catch(function (error) {
          setStatus("Copy failed: " + error.message);
        });
      });
      document.getElementById("manual-connect").addEventListener("click", function () {
        const url = manualUrl();
        if (!url) {
          setStatus("Please paste the other computer's address first.");
          return;
        }
        connectComputer({ base_url: url, trust_tier: "trusted" }).catch(function (error) {
          setStatus("Connect failed: " + error.message);
        });
      });
      document.getElementById("manual-test").addEventListener("click", function () {
        const url = manualUrl();
        if (!url) {
          setStatus("Please paste the other computer's address first.");
          return;
        }
        sendTestMission({ base_url: url, trust_tier: "trusted" }).catch(function (error) {
          setStatus("Test mission failed: " + error.message);
        });
      });
      document.addEventListener("click", function (event) {
        const button = event.target.closest("button[data-action]");
        if (!button) {
          return;
        }
        const baseUrl = normalizedUrl(button.getAttribute("data-base-url") || "");
        const peerId = String(button.getAttribute("data-peer-id") || "").trim();
        if (button.getAttribute("data-action") === "connect") {
          connectComputer({ base_url: baseUrl, peer_id: peerId, trust_tier: "trusted" }).catch(function (error) {
            setStatus("Connect failed: " + error.message);
          });
        }
        if (button.getAttribute("data-action") === "test") {
          sendTestMission({ base_url: baseUrl, peer_id: peerId, trust_tier: "trusted" }).catch(function (error) {
            setStatus("Test mission failed: " + error.message);
          });
        }
      });
    }

    function initEasy() {
      renderEasy();
      initEasyActions();
      setStatus("Easy setup is ready.");
      refreshEasy({ silent: true }).catch(function (error) {
        setStatus("Refresh failed: " + error.message);
      });
      easyApp.refreshTimer = setInterval(function () {
        if (document.visibilityState === "visible") {
          refreshEasy({ silent: true }).catch(function () {
          });
        }
      }, 15000);
    }

    document.addEventListener("DOMContentLoaded", initEasy);
  </script>
</body>
</html>"""


def build_easy_page(mesh: SovereignMesh) -> str:
    return EASY_PAGE_TEMPLATE.replace("__OCP_EASY_BOOTSTRAP__", build_easy_bootstrap(mesh))


__all__ = [
    "build_easy_bootstrap",
    "build_easy_page",
    "connect_all_peers",
    "connect_peer",
    "connectivity_diagnostics",
    "launch_mesh_test_mission",
    "launch_test_mission",
    "list_discovery_candidates",
    "scan_local_peers",
    "seek_discovery_peers",
    "sync_peer",
]
