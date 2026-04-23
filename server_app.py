from __future__ import annotations

import html
import json
from typing import Any

from mesh import SovereignMesh


def _node_summary(mesh: SovereignMesh) -> dict[str, Any]:
    manifest = mesh.get_manifest()
    card = dict(manifest.get("organism_card") or {})
    profile = dict(manifest.get("device_profile") or card.get("device_profile") or {})
    return {
        "node_id": card.get("node_id") or manifest.get("node_id") or getattr(mesh, "node_id", "ocp-node"),
        "display_name": card.get("display_name") or getattr(mesh, "display_name", "OCP Node"),
        "device_class": profile.get("device_class") or "unknown",
        "form_factor": profile.get("form_factor") or "device",
        "protocol_release": manifest.get("protocol_release") or "0.1",
        "protocol_version": manifest.get("protocol_version") or "",
    }


def build_app_manifest(mesh: SovereignMesh) -> dict[str, Any]:
    summary = _node_summary(mesh)
    display_name = str(summary.get("display_name") or "OCP Node")
    return {
        "name": f"OCP App - {display_name}",
        "short_name": "OCP",
        "description": "One local-first app for OCP setup, control, and protocol inspection.",
        "start_url": "/app",
        "scope": "/",
        "display": "standalone",
        "background_color": "#f7f0e6",
        "theme_color": "#112437",
        "categories": ["productivity", "utilities"],
    }


def build_app_page(mesh: SovereignMesh) -> str:
    summary = _node_summary(mesh)
    bootstrap_json = html.escape(json.dumps(summary, sort_keys=True), quote=True)
    node_id = html.escape(str(summary.get("node_id") or "ocp-node"))
    display_name = html.escape(str(summary.get("display_name") or "OCP Node"))
    device_class = html.escape(str(summary.get("device_class") or "unknown"))
    form_factor = html.escape(str(summary.get("form_factor") or "device"))
    protocol = html.escape(str(summary.get("protocol_release") or "0.1"))
    version = html.escape(str(summary.get("protocol_version") or ""))
    version_label = f" / {version}" if version else ""

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="theme-color" content="#112437">
  <meta name="mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-title" content="OCP">
  <link rel="manifest" href="/app.webmanifest">
  <script defer src="https://cdn.jsdelivr.net/npm/qrcode@1.5.4/build/qrcode.min.js"></script>
  <title>OCP App</title>
  <style>
    :root {{
      --ink: #132132;
      --muted: #5f6875;
      --paper: #fffaf3;
      --paper-strong: #fffdf8;
      --line: rgba(19, 33, 50, 0.13);
      --blue: #0c5c78;
      --green: #1d7d58;
      --gold: #a86c24;
      --shadow: 0 24px 70px rgba(45, 33, 18, 0.16);
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at 12% 6%, rgba(168, 108, 36, 0.18), transparent 25%),
        radial-gradient(circle at 88% 0%, rgba(12, 92, 120, 0.16), transparent 28%),
        linear-gradient(180deg, #fff8ec 0%, #f2eadf 100%);
    }}
    a {{ color: inherit; }}
    button {{ font: inherit; }}
    .app-shell {{
      width: min(1240px, 100%);
      margin: 0 auto;
      padding: 18px clamp(14px, 3vw, 28px) 32px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: minmax(0, 1.5fr) minmax(260px, 0.7fr);
      gap: 18px;
      align-items: stretch;
      margin-bottom: 16px;
    }}
    .panel {{
      background: rgba(255, 250, 243, 0.9);
      border: 1px solid var(--line);
      border-radius: 28px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(16px);
    }}
    .intro {{
      padding: clamp(22px, 4vw, 38px);
      position: relative;
      overflow: hidden;
    }}
    .intro::after {{
      content: "";
      position: absolute;
      width: 190px;
      height: 190px;
      right: -70px;
      top: -70px;
      border-radius: 999px;
      background: conic-gradient(from 180deg, rgba(12, 92, 120, 0.22), rgba(168, 108, 36, 0.28), transparent);
    }}
    .eyebrow {{
      margin: 0 0 12px;
      color: var(--blue);
      font-size: 0.78rem;
      font-weight: 800;
      letter-spacing: 0.14em;
      text-transform: uppercase;
    }}
    h1 {{
      max-width: 780px;
      margin: 0;
      font-size: clamp(2.25rem, 8vw, 5.75rem);
      line-height: 0.88;
      letter-spacing: -0.08em;
    }}
    .lead {{
      max-width: 760px;
      margin: 18px 0 0;
      color: var(--muted);
      font-size: clamp(1rem, 2.5vw, 1.22rem);
      line-height: 1.55;
    }}
    .node-card {{
      padding: 22px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      gap: 18px;
    }}
    .node-card h2 {{ margin: 0 0 6px; font-size: 1.3rem; }}
    .node-card p {{ margin: 0; color: var(--muted); line-height: 1.45; }}
    .chips {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 16px;
    }}
    .chip {{
      display: inline-flex;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 8px 10px;
      background: rgba(255, 255, 255, 0.58);
      color: #24364a;
      font-size: 0.82rem;
      font-weight: 700;
    }}
    .install {{
      border-radius: 20px;
      padding: 14px;
      background: #112437;
      color: #fff9ed;
    }}
    .install strong {{ display: block; margin-bottom: 4px; }}
    .install span {{ color: rgba(255, 249, 237, 0.75); font-size: 0.9rem; line-height: 1.4; }}
    .tabs {{
      position: sticky;
      top: 0;
      z-index: 20;
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 8px;
      padding: 10px;
      margin-bottom: 14px;
      border: 1px solid var(--line);
      border-radius: 24px;
      background: rgba(255, 250, 243, 0.88);
      backdrop-filter: blur(18px);
      box-shadow: 0 18px 40px rgba(45, 33, 18, 0.12);
    }}
    .tab {{
      min-height: 48px;
      border: 0;
      border-radius: 16px;
      background: transparent;
      color: #34455a;
      font-weight: 800;
      cursor: pointer;
    }}
    .tab[aria-selected="true"] {{
      color: #fff9ed;
      background: linear-gradient(135deg, #112437, #0c5c78);
      box-shadow: 0 10px 22px rgba(12, 92, 120, 0.22);
    }}
    .module {{
      display: none;
      overflow: hidden;
      min-height: 68vh;
    }}
    .module.active {{ display: block; }}
    .today {{
      padding: clamp(16px, 3vw, 24px);
    }}
    .today-grid {{
      display: grid;
      grid-template-columns: minmax(0, 1.35fr) minmax(260px, 0.65fr);
      gap: 16px;
    }}
    .today-card {{
      border: 1px solid var(--line);
      border-radius: 24px;
      background: rgba(255, 253, 248, 0.76);
      padding: 18px;
    }}
    .today-card h2,
    .today-card h3 {{
      margin: 0;
    }}
    .today-card p {{
      color: var(--muted);
      line-height: 1.5;
    }}
    .mesh-state {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
      margin: 18px 0;
    }}
    .mesh-stat {{
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
      background: rgba(255, 255, 255, 0.56);
    }}
    .mesh-stat span {{
      display: block;
      color: var(--muted);
      font-size: 0.76rem;
      font-weight: 800;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    .mesh-stat strong {{
      display: block;
      margin-top: 8px;
      font-size: 1.65rem;
    }}
    .primary-action {{
      display: inline-flex;
      border: 0;
      border-radius: 18px;
      padding: 14px 18px;
      background: linear-gradient(135deg, var(--green), var(--blue));
      color: white;
      font-weight: 900;
      cursor: pointer;
      box-shadow: 0 14px 30px rgba(12, 92, 120, 0.22);
    }}
    .secondary-action {{
      display: inline-flex;
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 13px 16px;
      background: rgba(255, 255, 255, 0.74);
      color: var(--blue);
      font-weight: 900;
      cursor: pointer;
      text-decoration: none;
    }}
    .today-actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
    }}
    .next-actions {{
      margin: 14px 0 0;
      padding: 0;
      list-style: none;
      display: grid;
      gap: 8px;
    }}
    .next-actions li {{
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 10px 12px;
      background: rgba(255, 255, 255, 0.55);
      color: #2a3c50;
      line-height: 1.35;
    }}
    .phone-link {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 12px;
      background: rgba(255,255,255,0.7);
      color: #24364a;
      overflow-wrap: anywhere;
      font-weight: 800;
    }}
    .qr-frame {{
      min-height: 170px;
      display: grid;
      place-items: center;
      border: 1px dashed rgba(19, 33, 50, 0.24);
      border-radius: 20px;
      background: rgba(255, 255, 255, 0.46);
      margin-top: 12px;
      overflow: hidden;
    }}
    .qr-frame img {{
      width: 156px;
      height: 156px;
      border-radius: 12px;
    }}
    .route-list {{
      display: grid;
      gap: 8px;
      margin-top: 12px;
    }}
    .route-item {{
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 10px 12px;
      background: rgba(255,255,255,0.55);
    }}
    .route-item strong {{ display: block; }}
    .route-item span {{ color: var(--muted); font-size: 0.9rem; }}
    .module-head {{
      display: flex;
      justify-content: space-between;
      gap: 14px;
      align-items: center;
      padding: 16px 18px;
      border-bottom: 1px solid var(--line);
      background: rgba(255, 253, 248, 0.8);
    }}
    .module-head h2 {{ margin: 0; font-size: 1.1rem; }}
    .module-head p {{ margin: 4px 0 0; color: var(--muted); font-size: 0.92rem; }}
    .open-link {{
      flex: 0 0 auto;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      padding: 10px 12px;
      background: #fff;
      border: 1px solid var(--line);
      color: var(--blue);
      font-size: 0.88rem;
      font-weight: 800;
      text-decoration: none;
    }}
    iframe {{
      display: block;
      width: 100%;
      min-height: 76vh;
      border: 0;
      background: #fffaf3;
    }}
    .protocol-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      padding: 16px;
    }}
    .protocol-card {{
      min-height: 170px;
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 22px;
      background: rgba(255, 253, 248, 0.8);
      text-decoration: none;
    }}
    .protocol-card span {{
      color: var(--muted);
      font-size: 0.82rem;
      font-weight: 800;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    .protocol-card strong {{
      display: block;
      margin: 10px 0 8px;
      font-size: 1.2rem;
    }}
    .protocol-card p {{
      margin: 0;
      color: var(--muted);
      line-height: 1.45;
    }}
    .protocol-actions {{
      padding: 0 16px 16px;
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }}
    .action {{
      border: 0;
      border-radius: 16px;
      padding: 12px 14px;
      background: var(--green);
      color: white;
      font-weight: 800;
      cursor: pointer;
    }}
    .contract-preview {{
      margin: 0 16px 16px;
      max-height: 340px;
      overflow: auto;
      white-space: pre-wrap;
      padding: 14px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: #101b2a;
      color: #e9f3ff;
      font-size: 0.78rem;
      line-height: 1.5;
    }}
    @media (max-width: 780px) {{
      .hero {{ grid-template-columns: 1fr; }}
      .tabs {{ border-radius: 20px; grid-template-columns: repeat(2, 1fr); }}
      .tab {{ min-height: 44px; font-size: 0.92rem; }}
      .module-head {{ align-items: flex-start; flex-direction: column; }}
      .open-link {{ width: 100%; }}
      .today-grid {{ grid-template-columns: 1fr; }}
      .mesh-state {{ grid-template-columns: 1fr; }}
      .protocol-grid {{ grid-template-columns: 1fr; }}
      iframe {{ min-height: 72vh; }}
    }}
  </style>
</head>
<body>
  <main class="app-shell" data-ocp-app="{bootstrap_json}">
    <section class="hero">
      <div class="panel intro">
        <p class="eyebrow">Open Compute Protocol</p>
        <h1>One app for the mesh.</h1>
        <p class="lead">
          OCP App brings setup, the phone control deck, and protocol inspection into one installable local-first surface.
          The old routes still work: <strong>/easy</strong> for OCP Easy Setup and <strong>/control</strong> for OCP Control Deck.
        </p>
      </div>
      <aside class="panel node-card" aria-label="Current OCP node">
        <div>
          <p class="eyebrow">This node</p>
          <h2>{display_name}</h2>
          <p>{node_id}</p>
          <div class="chips">
            <span class="chip">{device_class}</span>
            <span class="chip">{form_factor}</span>
            <span class="chip">OCP {protocol}{version_label}</span>
          </div>
        </div>
        <div class="install">
          <strong>Install this app</strong>
          <span>On your phone, open this page, choose Share or browser menu, then Add to Home Screen.</span>
        </div>
      </aside>
    </section>

    <nav class="tabs" aria-label="OCP app sections">
      <button class="tab" type="button" data-tab="today" aria-selected="true">Today</button>
      <button class="tab" type="button" data-tab="setup" aria-selected="false">Setup</button>
      <button class="tab" type="button" data-tab="control" aria-selected="false">Control</button>
      <button class="tab" type="button" data-tab="protocol" aria-selected="false">Protocol</button>
    </nav>

    <section id="today" class="panel module active today" aria-label="OCP Today module">
      <div class="today-grid">
        <div class="today-card">
          <p class="eyebrow">Autonomic Mesh</p>
          <h2 data-app-quality-label>Local node ready</h2>
          <p data-app-summary>Loading local mesh status...</p>
          <div class="mesh-state" aria-label="Mesh status metrics">
            <div class="mesh-stat">
              <span>Peers</span>
              <strong data-app-peer-count>0</strong>
            </div>
            <div class="mesh-stat">
              <span>Routes</span>
              <strong data-app-route-count>0/0</strong>
            </div>
            <div class="mesh-stat">
              <span>Proof</span>
              <strong data-app-proof-status>none</strong>
            </div>
          </div>
          <div class="today-actions">
            <button class="primary-action" type="button" data-activate-autonomic>Activate Autonomic Mesh</button>
            <button class="secondary-action" type="button" data-refresh-status>Refresh</button>
            <a class="secondary-action" href="/mesh/app/status" target="_blank" rel="noreferrer">Inspect App Status</a>
          </div>
          <ul class="next-actions" data-next-actions>
            <li>Press Activate Autonomic Mesh to discover, repair, enlist, prove, and explain this mesh.</li>
          </ul>
        </div>
        <aside class="today-card">
          <p class="eyebrow">Phone Link + QR</p>
          <h3>Open this mesh on your phone</h3>
          <p>Start Mesh Mode on the desktop launcher, then use this LAN link from your phone on the same Wi-Fi.</p>
          <div class="phone-link" data-phone-link>/app</div>
          <div class="today-actions" style="margin-top: 10px;">
            <button class="secondary-action" type="button" data-copy-phone-link>Copy Phone Link</button>
            <a class="secondary-action" href="/easy">Open Setup QR</a>
          </div>
          <div class="qr-frame" data-phone-qr>QR appears when a LAN phone link is available.</div>
          <div class="route-list" data-route-list></div>
        </aside>
      </div>
    </section>

    <section id="setup" class="panel module" aria-label="OCP Easy Setup module">
      <div class="module-head">
        <div>
          <h2>OCP Easy Setup</h2>
          <p>Pair nearby machines, copy the easy link, scan QR, and test the whole mesh.</p>
        </div>
        <a class="open-link" href="/easy">Open /easy directly</a>
      </div>
      <iframe title="OCP Easy Setup" src="/easy"></iframe>
    </section>

    <section id="control" class="panel module" aria-label="OCP Control Deck module">
      <div class="module-head">
        <div>
          <h2>OCP Control Deck</h2>
          <p>Operate missions, queues, approvals, helpers, artifacts, treaties, and live mesh state from the phone.</p>
        </div>
        <a class="open-link" href="/control">Open /control directly</a>
      </div>
      <iframe title="OCP Control Deck" src="/control" loading="lazy"></iframe>
    </section>

    <section id="protocol" class="panel module" aria-label="OCP protocol module">
      <div class="module-head">
        <div>
          <h2>Protocol</h2>
          <p>Inspect the live wire contract and node manifest without leaving the app.</p>
        </div>
        <a class="open-link" href="/mesh/contract">Open /mesh/contract</a>
      </div>
      <div class="protocol-grid">
        <a class="protocol-card" href="/mesh/manifest">
          <span>Runtime</span>
          <strong>Manifest</strong>
          <p>Identity, device profile, capabilities, and compatibility facts advertised by this node.</p>
        </a>
        <a class="protocol-card" href="/mesh/contract">
          <span>Protocol</span>
          <strong>HTTP Contract</strong>
          <p>The versioned route registry, schema references, and request validation surface.</p>
        </a>
        <a class="protocol-card" href="/mesh/device-profile">
          <span>Device</span>
          <strong>Profile</strong>
          <p>How this device describes its compute class, mobility, power, and sync posture.</p>
        </a>
      </div>
      <div class="protocol-actions">
        <button class="action" type="button" data-fetch-contract>Preview contract</button>
      </div>
      <pre class="contract-preview" data-contract-preview>Press "Preview contract" to fetch /mesh/contract.</pre>
    </section>
  </main>

  <script>
    const OCP_OPERATOR_TOKEN_KEY = "ocp_operator_token";
    const consumeOperatorToken = () => {{
      const hash = String(window.location.hash || "");
      let token = "";
      if (hash.startsWith("#ocp_operator_token=")) {{
        token = decodeURIComponent(hash.slice("#ocp_operator_token=".length));
      }} else if (hash.includes("ocp_operator_token=")) {{
        token = new URLSearchParams(hash.replace(/^#/, "")).get("ocp_operator_token") || "";
      }}
      if (token) {{
        try {{ window.localStorage.setItem(OCP_OPERATOR_TOKEN_KEY, token); }} catch (error) {{}}
        history.replaceState(null, "", window.location.pathname + window.location.search);
      }}
    }};
    const operatorToken = () => {{
      try {{ return String(window.localStorage.getItem(OCP_OPERATOR_TOKEN_KEY) || "").trim(); }} catch (error) {{ return ""; }}
    }};
    const withOperatorAuth = (options = {{}}) => {{
      const next = Object.assign({{}}, options || {{}});
      const headers = new Headers(next.headers || {{}});
      const token = operatorToken();
      if (token && !headers.has("X-OCP-Operator-Token")) {{
        headers.set("X-OCP-Operator-Token", token);
      }}
      next.headers = headers;
      return next;
    }};
    consumeOperatorToken();

    const tabs = Array.from(document.querySelectorAll("[data-tab]"));
    const modules = Array.from(document.querySelectorAll(".module"));
    const activate = (name) => {{
      tabs.forEach((tab) => tab.setAttribute("aria-selected", String(tab.dataset.tab === name)));
      modules.forEach((module) => module.classList.toggle("active", module.id === name));
      if (window.location.hash !== "#" + name) {{
        history.replaceState(null, "", name === "today" ? window.location.pathname : "#" + name);
      }}
    }};
    tabs.forEach((tab) => tab.addEventListener("click", () => activate(tab.dataset.tab)));
    const initial = window.location.hash.replace("#", "");
    if (["today", "setup", "control", "protocol"].includes(initial)) activate(initial);

    const appEls = {{
      quality: document.querySelector("[data-app-quality-label]"),
      summary: document.querySelector("[data-app-summary]"),
      peers: document.querySelector("[data-app-peer-count]"),
      routes: document.querySelector("[data-app-route-count]"),
      proof: document.querySelector("[data-app-proof-status]"),
      actions: document.querySelector("[data-next-actions]"),
      phone: document.querySelector("[data-phone-link]"),
      qr: document.querySelector("[data-phone-qr]"),
      routeList: document.querySelector("[data-route-list]")
    }};

    const text = (value, fallback = "") => String(value || fallback || "");
    const setText = (node, value) => {{
      if (node) node.textContent = value;
    }};
    const renderQr = (url) => {{
      if (!appEls.qr) return;
      appEls.qr.textContent = "QR appears when a LAN phone link is available.";
      if (!url || !(window.QRCode && typeof window.QRCode.toDataURL === "function")) return;
      window.QRCode.toDataURL(url, {{ margin: 1, width: 180 }}, (error, dataUrl) => {{
        if (error) {{
          appEls.qr.textContent = "QR unavailable. Use the phone link above.";
          return;
        }}
        appEls.qr.innerHTML = "";
        const img = document.createElement("img");
        img.src = dataUrl;
        img.alt = "OCP phone link QR";
        appEls.qr.appendChild(img);
      }});
    }};
    const renderStatus = (payload) => {{
      const quality = payload.mesh_quality || {{}};
      const routeHealth = payload.route_health || {{}};
      const proof = payload.latest_proof || {{}};
      const urls = payload.app_urls || {{}};
      setText(appEls.quality, text(quality.label, "Local node ready"));
      setText(appEls.summary, text(quality.operator_summary || (payload.autonomy || {{}}).operator_summary, "Press Activate Autonomic Mesh to discover, repair, enlist, prove, and explain this mesh."));
      setText(appEls.peers, String(quality.peer_count || 0));
      setText(appEls.routes, String(quality.healthy_routes || 0) + "/" + String(quality.route_count || 0));
      setText(appEls.proof, text(proof.status, "none"));
      const phoneUrl = text(urls.phone_url || urls.app_url, window.location.origin + "/app");
      setText(appEls.phone, phoneUrl);
      renderQr(phoneUrl);
      if (appEls.actions) {{
        const actions = payload.next_actions || [];
        appEls.actions.innerHTML = "";
        actions.forEach((item) => {{
          const li = document.createElement("li");
          li.textContent = String(item);
          appEls.actions.appendChild(li);
        }});
      }}
      if (appEls.routeList) {{
        const routes = routeHealth.routes || [];
        appEls.routeList.innerHTML = "";
        routes.slice(0, 4).forEach((route) => {{
          const item = document.createElement("div");
          item.className = "route-item";
          const name = document.createElement("strong");
          name.textContent = text(route.display_name || route.peer_id, "Peer");
          const status = document.createElement("span");
          status.textContent = text(route.operator_summary || route.status, "Route status unknown.");
          item.appendChild(name);
          item.appendChild(status);
          appEls.routeList.appendChild(item);
        }});
      }}
    }};
    const refreshAppStatus = async () => {{
      try {{
        const response = await fetch("/mesh/app/status", withOperatorAuth());
        const payload = await response.json();
        renderStatus(payload);
        return payload;
      }} catch (error) {{
        setText(appEls.summary, "Unable to refresh OCP app status: " + error.message);
        throw error;
      }}
    }};
    document.querySelector("[data-refresh-status]")?.addEventListener("click", () => refreshAppStatus());
    document.querySelector("[data-copy-phone-link]")?.addEventListener("click", async () => {{
      const value = text(appEls.phone?.textContent, window.location.origin + "/app");
      try {{
        await navigator.clipboard.writeText(value);
        setText(appEls.summary, "Copied phone link: " + value);
      }} catch (error) {{
        setText(appEls.summary, "Copy failed. Select the phone link manually.");
      }}
    }});
    document.querySelector("[data-activate-autonomic]")?.addEventListener("click", async (event) => {{
      const button = event.currentTarget;
      button.disabled = true;
      const original = button.textContent;
      button.textContent = "Activating...";
      setText(appEls.summary, "Autonomic Mesh is discovering, probing routes, planning helpers, and running a proof...");
      try {{
        const response = await fetch("/mesh/autonomy/activate", withOperatorAuth({{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{
            mode: "assisted",
            limit: 24,
            run_proof: true,
            repair: true,
            actor_agent_id: "ocp-app-home"
          }})
        }}));
        const result = await response.json();
        if (!response.ok) {{
          throw new Error(result.error || result.message || response.status + " " + response.statusText);
        }}
        setText(appEls.summary, result.operator_summary || result.summary || "Autonomic Mesh activation complete.");
        await refreshAppStatus();
      }} catch (error) {{
        setText(appEls.summary, "Autonomic Mesh activation failed: " + error.message);
      }} finally {{
        button.disabled = false;
        button.textContent = original;
      }}
    }});
    refreshAppStatus().catch(() => {{}});

    const preview = document.querySelector("[data-contract-preview]");
    const fetchButton = document.querySelector("[data-fetch-contract]");
    fetchButton?.addEventListener("click", async () => {{
      preview.textContent = "Loading /mesh/contract...";
      try {{
        const response = await fetch("/mesh/contract", withOperatorAuth());
        const payload = await response.json();
        preview.textContent = JSON.stringify(payload, null, 2);
      }} catch (error) {{
        preview.textContent = "Unable to fetch /mesh/contract: " + error;
      }}
    }});
  </script>
</body>
</html>"""


__all__ = ["build_app_manifest", "build_app_page"]
