from __future__ import annotations

import html
import json
from typing import Any

from mesh import SovereignMesh
from server_browser_client import build_browser_client_script


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
        "background_color": "#071217",
        "theme_color": "#071217",
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
    browser_client_js = build_browser_client_script()

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="theme-color" content="#071217">
  <meta name="mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-title" content="OCP">
  <link rel="manifest" href="/app.webmanifest">
  <script defer src="https://cdn.jsdelivr.net/npm/qrcode@1.5.4/build/qrcode.min.js"></script>
  <title>OCP App</title>
  <style>
    :root {{
      --ink: #ecf4f6;
      --muted: #91a8af;
      --paper: rgba(8, 16, 22, 0.92);
      --paper-strong: rgba(13, 25, 30, 0.96);
      --line: rgba(104, 242, 167, 0.14);
      --blue: #58c4f5;
      --green: #68f2a7;
      --gold: #f6b35f;
      --shadow: 0 28px 80px rgba(0, 0, 0, 0.42);
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at 12% 6%, rgba(246, 179, 95, 0.13), transparent 24%),
        radial-gradient(circle at 88% 0%, rgba(88, 196, 245, 0.18), transparent 28%),
        radial-gradient(circle at 72% 22%, rgba(104, 242, 167, 0.14), transparent 26%),
        linear-gradient(180deg, #071217 0%, #08161a 46%, #0d1713 100%);
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
      background: rgba(8, 16, 22, 0.84);
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
      color: var(--green);
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
      color: #f6f1dd;
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
      background: rgba(255, 255, 255, 0.05);
      color: #d8eceb;
      font-size: 0.82rem;
      font-weight: 700;
    }}
    .install {{
      border-radius: 20px;
      padding: 14px;
      background: rgba(0, 0, 0, 0.28);
      border: 1px solid rgba(104, 242, 167, 0.12);
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
      background: rgba(10, 16, 22, 0.86);
      backdrop-filter: blur(18px);
      box-shadow: 0 18px 40px rgba(0, 0, 0, 0.28);
    }}
    .tab {{
      min-height: 48px;
      border: 0;
      border-radius: 16px;
      background: transparent;
      color: #aac2ca;
      font-weight: 800;
      cursor: pointer;
    }}
    .tab[aria-selected="true"] {{
      color: #061015;
      background: linear-gradient(135deg, var(--green), var(--blue));
      box-shadow: 0 10px 22px rgba(88, 196, 245, 0.20);
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
      background: rgba(10, 18, 24, 0.84);
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
      background: rgba(255, 255, 255, 0.05);
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
      color: #f2f7f7;
    }}
    .meta-strip {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
      margin: 18px 0 0;
    }}
    .meta-chip {{
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 12px 14px;
      background: rgba(255, 255, 255, 0.04);
    }}
    .meta-chip span {{
      display: block;
      color: var(--muted);
      font-size: 0.72rem;
      font-weight: 800;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    .meta-chip strong {{
      display: block;
      margin-top: 8px;
      font-size: 1rem;
      line-height: 1.35;
      color: #f5f9f9;
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
      background: rgba(255, 255, 255, 0.05);
      color: #dbeef0;
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
      background: rgba(255, 255, 255, 0.04);
      color: #d7eaee;
      line-height: 1.35;
    }}
    .story-stack {{
      margin-top: 16px;
      display: grid;
      gap: 10px;
    }}
    .story-line {{
      display: grid;
      grid-template-columns: auto 1fr;
      gap: 10px;
      align-items: start;
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 10px 12px;
      background: rgba(255, 255, 255, 0.04);
    }}
    .story-line::before {{
      content: "";
      width: 10px;
      height: 10px;
      margin-top: 6px;
      border-radius: 999px;
      background: var(--green);
      box-shadow: 0 0 0 5px rgba(104, 242, 167, 0.12);
    }}
    .proof-timeline {{
      margin-top: 14px;
      display: grid;
      gap: 8px;
    }}
    .timeline-item {{
      display: grid;
      grid-template-columns: auto 1fr;
      gap: 10px;
      align-items: start;
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 10px 12px;
      background: rgba(255, 255, 255, 0.04);
    }}
    .timeline-dot {{
      width: 12px;
      height: 12px;
      margin-top: 4px;
      border-radius: 999px;
      background: var(--gold);
      box-shadow: 0 0 0 5px rgba(168, 108, 36, 0.12);
    }}
    .timeline-item[data-status="ok"] .timeline-dot,
    .timeline-item[data-status="completed"] .timeline-dot {{
      background: var(--green);
      box-shadow: 0 0 0 5px rgba(29, 125, 88, 0.12);
    }}
    .timeline-item[data-status="failed"] .timeline-dot,
    .timeline-item[data-status="warning"] .timeline-dot {{
      background: #c84332;
      box-shadow: 0 0 0 5px rgba(200, 67, 50, 0.12);
    }}
    .timeline-item strong {{ display: block; font-size: 0.92rem; }}
    .timeline-item span {{ display: block; color: var(--muted); font-size: 0.9rem; line-height: 1.35; }}
    .phone-link {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 12px;
      background: rgba(255,255,255,0.04);
      color: #e8f2f2;
      overflow-wrap: anywhere;
      font-weight: 800;
    }}
    .qr-frame {{
      min-height: 170px;
      display: grid;
      place-items: center;
      border: 1px dashed rgba(104, 242, 167, 0.24);
      border-radius: 20px;
      background: rgba(255, 255, 255, 0.03);
      margin-top: 12px;
      overflow: hidden;
    }}
    .qr-frame img {{
      width: 156px;
      height: 156px;
      border-radius: 12px;
    }}
    .peer-stage {{
      margin-top: 18px;
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.04);
    }}
    .peer-stage h3 {{
      margin: 0 0 8px;
      font-size: 1.2rem;
    }}
    .peer-stage p {{
      margin: 0;
    }}
    .role-wall {{
      margin-top: 14px;
      display: grid;
      gap: 10px;
    }}
    .role-pill {{
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 12px 14px;
      background: rgba(255,255,255,0.04);
    }}
    .role-pill strong {{
      display: block;
      margin-bottom: 4px;
    }}
    .role-pill span {{
      display: block;
      color: var(--muted);
      line-height: 1.4;
    }}
    .route-list-head {{
      margin-top: 16px;
      margin-bottom: 8px;
    }}
    .route-list-head span {{
      color: var(--muted);
      font-size: 0.92rem;
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
      background: rgba(255,255,255,0.04);
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
      background: rgba(255, 255, 255, 0.03);
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
      background: rgba(255,255,255,0.04);
      border: 1px solid var(--line);
      color: #eaf6f8;
      font-size: 0.88rem;
      font-weight: 800;
      text-decoration: none;
    }}
    iframe {{
      display: block;
      width: 100%;
      min-height: 76vh;
      border: 0;
      background: #081217;
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
      background: rgba(255, 255, 255, 0.04);
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
      background: #071217;
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
      .meta-strip {{ grid-template-columns: 1fr; }}
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
      <button class="tab" type="button" data-tab="setup" aria-selected="false">Setup Details</button>
      <button class="tab" type="button" data-tab="control" aria-selected="false">Advanced Control</button>
      <button class="tab" type="button" data-tab="protocol" aria-selected="false">Protocol</button>
    </nav>

    <section id="today" class="panel module active today" aria-label="OCP Today module">
      <div class="today-grid">
        <div class="today-card">
          <p class="eyebrow">Setup Doctor</p>
          <h2 data-app-quality-label>Local node ready</h2>
          <p data-app-summary>Loading local mesh status...</p>
          <div class="meta-strip" aria-label="Recovery, blocker, and primary peer">
            <div class="meta-chip">
              <span>Recovery</span>
              <strong data-app-recovery-state>healthy</strong>
            </div>
            <div class="meta-chip">
              <span>Blocker</span>
              <strong data-app-blocker-code>none</strong>
            </div>
            <div class="meta-chip">
              <span>Primary peer</span>
              <strong data-app-primary-peer-inline>No remote peer yet</strong>
            </div>
          </div>
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
          <div class="story-stack" data-story-lines>
            <div class="story-line">Press Activate Mesh to discover, repair, enlist, prove, and explain this mesh.</div>
          </div>
          <div class="today-actions">
            <button class="primary-action" type="button" data-activate-autonomic>Activate Mesh</button>
            <button class="secondary-action" type="button" data-run-best-device>Run on Best Device</button>
            <button class="secondary-action" type="button" data-replicate-proof-artifact>Replicate Proof Artifact</button>
            <button class="secondary-action" type="button" data-refresh-status>Refresh</button>
            <a class="secondary-action" href="/mesh/app/status" target="_blank" rel="noreferrer">Inspect App Status</a>
          </div>
          <ul class="next-actions" data-next-actions>
            <li>Press Activate Mesh to discover, repair, enlist, prove, and explain this mesh.</li>
          </ul>
          <div class="proof-timeline" data-proof-timeline aria-label="Proof timeline"></div>
        </div>
        <aside class="today-card">
          <p class="eyebrow">Phone Link + QR</p>
          <h3>Mission Control on your phone</h3>
          <p>Open the same trusted mesh story on your phone, then use this LAN link from the same Wi-Fi.</p>
          <div class="phone-link" data-phone-link>/app</div>
          <div class="today-actions" style="margin-top: 10px;">
            <button class="secondary-action" type="button" data-copy-phone-link>Copy Phone Link</button>
            <a class="secondary-action" href="/easy">Open Setup QR</a>
          </div>
          <div class="qr-frame" data-phone-qr>QR appears when a LAN phone link is available.</div>
          <div class="peer-stage">
            <p class="eyebrow">Primary Peer</p>
            <h3 data-primary-peer-label>No remote peer yet</h3>
            <p data-primary-peer-summary>Connect another trusted device to build the mesh.</p>
          </div>
          <div class="role-wall" data-device-roles></div>
          <div class="route-list-head">
            <p class="eyebrow">Route health</p>
            <span>Fresh, stale, and repaired peer paths.</span>
          </div>
          <div class="route-list" data-route-list></div>
        </aside>
      </div>
    </section>

    <section id="setup" class="panel module" aria-label="OCP Easy Setup module">
      <div class="module-head">
        <div>
          <h2>Setup Details</h2>
          <p>Pair nearby machines, copy the easy link, scan QR, and inspect setup diagnostics.</p>
        </div>
        <a class="open-link" href="/easy">Open setup details</a>
      </div>
      <iframe title="OCP Easy Setup" src="/easy"></iframe>
    </section>

    <section id="control" class="panel module" aria-label="OCP Control Deck module">
      <div class="module-head">
        <div>
          <h2>Advanced Control</h2>
          <p>Operate missions, queues, approvals, helpers, artifacts, treaties, and live mesh state from the phone.</p>
        </div>
        <a class="open-link" href="/control">Open advanced control</a>
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
{browser_client_js}

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
      recovery: document.querySelector("[data-app-recovery-state]"),
      blockerCode: document.querySelector("[data-app-blocker-code]"),
      primaryPeerInline: document.querySelector("[data-app-primary-peer-inline]"),
      peers: document.querySelector("[data-app-peer-count]"),
      routes: document.querySelector("[data-app-route-count]"),
      proof: document.querySelector("[data-app-proof-status]"),
      story: document.querySelector("[data-story-lines]"),
      actions: document.querySelector("[data-next-actions]"),
      phone: document.querySelector("[data-phone-link]"),
      qr: document.querySelector("[data-phone-qr]"),
      primaryPeerLabel: document.querySelector("[data-primary-peer-label]"),
      primaryPeerSummary: document.querySelector("[data-primary-peer-summary]"),
      deviceRoles: document.querySelector("[data-device-roles]"),
      routeList: document.querySelector("[data-route-list]"),
      timeline: document.querySelector("[data-proof-timeline]")
    }};

    const text = (value, fallback = "") => String(value || fallback || "");
    const humanize = (value) => text(value, "unknown").replace(/_/g, " ").replace(/\\b\\w/g, (match) => match.toUpperCase());
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
      const setup = payload.setup || {{}};
      const routeHealth = payload.route_health || {{}};
      const proof = payload.latest_proof || {{}};
      const urls = payload.app_urls || {{}};
      const primaryPeer = setup.primary_peer || {{}};
      const story = Array.isArray(setup.story) ? setup.story.filter((line) => String(line || "").trim()) : [];
      setText(appEls.quality, text(setup.label || quality.label, "Local node ready"));
      setText(appEls.summary, text(setup.operator_summary || story[0] || quality.operator_summary || (payload.autonomy || {{}}).operator_summary, "Press Activate Mesh to discover, repair, enlist, prove, and explain this mesh."));
      setText(appEls.recovery, humanize(text(setup.recovery_state, "healthy")));
      setText(appEls.blockerCode, text(setup.blocker_code ? humanize(setup.blocker_code) : "none"));
      setText(appEls.primaryPeerInline, text(primaryPeer.display_name || primaryPeer.peer_id, "No remote peer yet"));
      setText(appEls.peers, String(quality.peer_count || 0));
      setText(appEls.routes, String(quality.healthy_routes || 0) + "/" + String(quality.route_count || 0));
      setText(appEls.proof, humanize(text(proof.status, "none")));
      const phoneUrl = withOperatorFragment(text(setup.phone_url || urls.phone_url || urls.app_url, window.location.origin + "/app"));
      setText(appEls.phone, phoneUrl);
      setText(appEls.primaryPeerLabel, text(primaryPeer.display_name || primaryPeer.peer_id, "No remote peer yet"));
      setText(appEls.primaryPeerSummary, text(primaryPeer.summary, "Connect another trusted device to build the mesh."));
      renderQr(phoneUrl);
      if (appEls.story) {{
        appEls.story.innerHTML = "";
        const lines = story.length ? story : [text(setup.next_fix, "Press Activate Mesh to discover, repair, enlist, prove, and explain this mesh.")];
        lines.slice(0, 4).forEach((line) => {{
          const item = document.createElement("div");
          item.className = "story-line";
          item.textContent = String(line);
          appEls.story.appendChild(item);
        }});
      }}
      if (appEls.actions) {{
        const actions = payload.next_actions || [];
        appEls.actions.innerHTML = "";
        (actions.length ? actions : [text(setup.next_fix, "Press Activate Mesh to discover, repair, enlist, prove, and explain this mesh.")]).forEach((item) => {{
          const li = document.createElement("li");
          li.textContent = String(item);
          appEls.actions.appendChild(li);
        }});
      }}
      if (appEls.deviceRoles) {{
        const roles = Array.isArray(setup.device_roles) ? setup.device_roles : [];
        appEls.deviceRoles.innerHTML = "";
        roles.slice(0, 4).forEach((role) => {{
          const item = document.createElement("div");
          item.className = "role-pill";
          const title = document.createElement("strong");
          title.textContent = text(role.display_name || role.peer_id, "Peer") + " - " + humanize(text(role.role, "peer"));
          const summary = document.createElement("span");
          summary.textContent = text(role.summary, humanize(text(role.status, "unknown")));
          item.appendChild(title);
          item.appendChild(summary);
          appEls.deviceRoles.appendChild(item);
        }});
        if (!roles.length) {{
          const item = document.createElement("div");
          item.className = "role-pill";
          const title = document.createElement("strong");
          title.textContent = "This node - local command";
          const summary = document.createElement("span");
          summary.textContent = "Start Mesh Mode and connect another trusted device to expand the mesh.";
          item.appendChild(title);
          item.appendChild(summary);
          appEls.deviceRoles.appendChild(item);
        }}
      }}
      if (appEls.timeline) {{
        const timeline = (setup.timeline || []).slice(-8);
        appEls.timeline.innerHTML = "";
        timeline.forEach((event) => {{
          const item = document.createElement("div");
          item.className = "timeline-item";
          item.dataset.status = text(event.status, "info");
          const dot = document.createElement("div");
          dot.className = "timeline-dot";
          const body = document.createElement("div");
          const title = document.createElement("strong");
          title.textContent = humanize(text(event.kind, "event"));
          const summary = document.createElement("span");
          summary.textContent = text(event.summary, "OCP recorded a setup event.");
          body.appendChild(title);
          body.appendChild(summary);
          item.appendChild(dot);
          item.appendChild(body);
          appEls.timeline.appendChild(item);
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
          const freshness = text(route.freshness, "");
          status.textContent = text(route.operator_summary || route.status, "Route status unknown.") + (freshness ? " [" + freshness + "]" : "");
          item.appendChild(name);
          item.appendChild(status);
          appEls.routeList.appendChild(item);
        }});
        if (!routes.length) {{
          const item = document.createElement("div");
          item.className = "route-item";
          const name = document.createElement("strong");
          name.textContent = "No peer routes yet";
          const status = document.createElement("span");
          status.textContent = "Start Mesh Mode, connect another device, then press Activate Mesh.";
          item.appendChild(name);
          item.appendChild(status);
          appEls.routeList.appendChild(item);
        }}
      }}
    }};
    const refreshAppStatus = async () => {{
      try {{
        const payload = await fetchJson("/mesh/app/status");
        renderStatus(payload);
        return payload;
      }} catch (error) {{
        setText(appEls.summary, "Unable to refresh OCP app status: " + error.message);
        throw error;
      }}
    }};
    document.querySelector("[data-refresh-status]")?.addEventListener("click", () => refreshAppStatus());
    document.querySelector("[data-copy-phone-link]")?.addEventListener("click", async () => {{
      const value = withOperatorFragment(text(appEls.phone?.textContent, window.location.origin + "/app"));
      try {{
        await copyText(value);
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
      setText(appEls.summary, "OCP is discovering nearby devices, probing routes, planning safe helpers, and running a proof...");
      try {{
        const result = await fetchJson("/mesh/autonomy/activate", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{
            mode: "assisted",
            limit: 24,
            run_proof: true,
            repair: true,
            actor_agent_id: "ocp-app-home"
          }})
        }});
        setText(appEls.summary, result.operator_summary || result.summary || "Mesh activation complete.");
        await refreshAppStatus();
      }} catch (error) {{
        setText(appEls.summary, "Activate Mesh failed: " + error.message);
      }} finally {{
        button.disabled = false;
        button.textContent = original;
      }}
    }});
    document.querySelector("[data-run-best-device]")?.addEventListener("click", async () => {{
      setText(appEls.summary, "Asking the scheduler which device should run a small OCP-controlled proof job...");
      try {{
        const result = await fetchJson("/mesh/jobs/schedule", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{
            request_id: "app-best-device-" + Date.now(),
            allow_local: true,
            allow_remote: true,
            job: {{
              kind: "shell",
              requirements: {{ capabilities: ["shell"] }},
              policy: {{ classification: "trusted", mode: "batch", secret_scopes: [] }},
              metadata: {{ control_flow: "app_home", demo_action: "run_best_device" }},
              payload: {{ command: "echo OCP best-device proof" }}
            }}
          }})
        }});
        const decision = result.decision || result;
        const target = decision.target_peer_id || decision.peer_id || "selected target";
        setText(appEls.summary, "Scheduler selected " + target + " for this workload.");
        await refreshAppStatus();
      }} catch (error) {{
        setText(appEls.summary, "Run on Best Device failed: " + error.message);
      }}
    }});
    document.querySelector("[data-replicate-proof-artifact]")?.addEventListener("click", async () => {{
      try {{
        const status = await refreshAppStatus();
        const routes = (((status.route_health || {{}}).routes) || []);
        const proof = status.latest_proof || {{}};
        const defaultPeer = routes[0]?.peer_id || "";
        const defaultArtifact = proof.artifact_id || "";
        const peerId = window.prompt("Peer id to pull from", defaultPeer);
        if (!peerId) return;
        const artifactId = window.prompt("Remote artifact id to replicate", defaultArtifact);
        if (!artifactId) return;
        const remoteToken = window.prompt("Remote operator token for that peer (kept in memory only)", "");
        const payload = {{
          peer_id: peerId,
          artifact_id: artifactId,
          pin: true
        }};
        if (remoteToken) {{
          payload.remote_auth = {{ type: "operator_token", token: remoteToken }};
        }}
        setText(appEls.summary, "Replicating and verifying proof artifact from " + peerId + "...");
        const result = await fetchJson("/mesh/artifacts/replicate", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify(payload)
        }});
        setText(appEls.summary, "Replicated " + ((result.artifact || {{}}).digest || artifactId) + " with " + ((result.verification || {{}}).status || "verification") + ".");
        await refreshAppStatus();
      }} catch (error) {{
        setText(appEls.summary, "Replicate Proof Artifact failed: " + error.message);
      }}
    }});
    refreshAppStatus().catch(() => {{}});

    const preview = document.querySelector("[data-contract-preview]");
    const fetchButton = document.querySelector("[data-fetch-contract]");
    fetchButton?.addEventListener("click", async () => {{
      preview.textContent = "Loading /mesh/contract...";
      try {{
        const payload = await fetchJson("/mesh/contract");
        preview.textContent = JSON.stringify(payload, null, 2);
      }} catch (error) {{
        preview.textContent = "Unable to fetch /mesh/contract: " + error;
      }}
    }});
  </script>
</body>
</html>"""


__all__ = ["build_app_manifest", "build_app_page"]
