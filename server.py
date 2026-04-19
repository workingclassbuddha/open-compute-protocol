"""
Standalone HTTP host for the Sovereign Mesh OCP reference implementation.
"""

from __future__ import annotations

import argparse
import html
import json
import errno
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from mesh import SovereignMesh
from mesh.sovereign import _normalize_base_url, _preferred_local_base_url
from runtime import OCPRegistry, OCPStore

server_context = {
    "mesh": None,
    "runtime": None,
    "ready": False,
}


def _is_client_disconnect(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionResetError)):
        return True
    if isinstance(exc, OSError):
        return exc.errno in {errno.EPIPE, errno.ECONNRESET, 54, 104}
    return False


def _latest_event_cursor(mesh: SovereignMesh) -> int:
    try:
        with mesh._conn() as conn:
            row = conn.execute("SELECT MAX(seq) AS seq FROM mesh_events").fetchone()
        return int((row["seq"] if row is not None else 0) or 0)
    except Exception:
        return 0


def build_control_state(mesh: SovereignMesh) -> dict[str, Any]:
    manifest = mesh.get_manifest()
    organism_card = dict(manifest.get("organism_card") or {})
    node_id = organism_card.get("organism_id") or organism_card.get("node_id") or mesh.node_id
    display_name = organism_card.get("display_name") or mesh.display_name or node_id
    device_profile = dict(manifest.get("device_profile") or mesh.device_profile or {})
    implementation = dict(manifest.get("implementation") or {})
    peer_snapshot = dict(mesh.list_peers(limit=8) or {})
    notification_snapshot = dict(mesh.list_notifications(limit=8, target_peer_id=node_id) or {})
    approval_snapshot = dict(mesh.list_approvals(limit=8, target_peer_id=node_id) or {})
    queue_metrics = dict(mesh.queue_metrics() or {})
    queue_snapshot = dict(mesh.list_queue_messages(limit=8) or {})
    queue_messages = list(queue_snapshot.get("messages") or [])
    jobs_by_id: dict[str, dict] = {}
    for queue_message in queue_messages:
        job_id = str(queue_message.get("job_id") or "").strip()
        if not job_id:
            continue
        try:
            jobs_by_id[job_id] = mesh.get_job(job_id)
        except Exception:
            continue
    worker_snapshot = dict(mesh.list_workers(limit=8) or {})
    sync_policy = dict(manifest.get("sync_policy") or {})
    try:
        pressure = dict(mesh.mesh_pressure() or {})
    except Exception:
        pressure = {"pressure": "idle", "queued": 0, "inflight": 0, "total_slots": 0, "available_slots": 0, "reasons": [], "needs_help": False}
    try:
        helper_snapshot = dict(mesh.list_helpers(limit=12) or {})
    except Exception:
        helper_snapshot = {"helpers": []}
    try:
        coop_snapshot = dict(mesh.list_cooperative_tasks(limit=6) or {})
    except Exception:
        coop_snapshot = {"tasks": []}
    try:
        mission_snapshot = dict(mesh.list_missions(limit=6) or {})
    except Exception:
        mission_snapshot = {"missions": []}
    try:
        discovery_snapshot = dict(mesh.list_discovery_candidates(limit=12) or {})
    except Exception:
        discovery_snapshot = {"candidates": []}
    try:
        connectivity = dict(mesh.connectivity_diagnostics(limit=24) or {})
    except Exception:
        connectivity = {"status": "error", "local_ipv4": [], "scan_urls": [], "recent_errors": []}
    try:
        autonomy = dict(mesh.evaluate_autonomous_offload() or {})
    except Exception:
        autonomy = {"decision": "noop", "policy": {}, "pressure": pressure, "reasons": []}
    try:
        preference_snapshot = dict(mesh.list_offload_preferences(limit=6) or {})
    except Exception:
        preference_snapshot = {"preferences": []}
    version = " ".join(
        part
        for part in [
            str(implementation.get("name") or "OCP").strip(),
            str(manifest.get("protocol_release") or manifest.get("protocol_version") or "").strip(),
        ]
        if part
    ).strip()
    return {
        "node_id": node_id,
        "display_name": display_name,
        "role_label": str(organism_card.get("role") or "Sovereign Node").strip() or "Sovereign Node",
        "version": version or "OCP runtime",
        "device_class": device_profile.get("device_class") or "full",
        "device_profile": device_profile,
        "sync_policy": sync_policy,
        "manifest": manifest,
        "peers": peer_snapshot,
        "notifications": notification_snapshot,
        "approvals": approval_snapshot,
        "queue_metrics": queue_metrics,
        "workers": worker_snapshot,
        "queue": queue_snapshot,
        "pressure": pressure,
        "helpers": helper_snapshot,
        "missions": mission_snapshot,
        "discovery_candidates": discovery_snapshot,
        "connectivity": connectivity,
        "cooperative_tasks": coop_snapshot,
        "autonomy": autonomy,
        "preferences": preference_snapshot,
        "jobs": jobs_by_id,
        "control_stream": {
            "route": "/mesh/control/stream",
            "cursor": _latest_event_cursor(mesh),
            "transport": "sse",
            "fallback_refresh_seconds": 60,
        },
    }


def build_control_stream_payload(
    mesh: SovereignMesh,
    *,
    since_seq: int = 0,
    limit: int = 50,
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = snapshot or mesh.stream_snapshot(since_seq=max(0, int(since_seq)), limit=max(1, int(limit)))
    cursor = int(snapshot.get("next_cursor") or since_seq or 0)
    state = build_control_state(mesh)
    state["control_stream"] = {
        **dict(state.get("control_stream") or {}),
        "cursor": cursor,
        "recent_event_count": len(snapshot.get("events") or []),
    }
    return {
        "type": "control_state",
        "cursor": cursor,
        "events": list(snapshot.get("events") or []),
        "state": state,
        "generated_at": snapshot.get("generated_at") or "",
    }


def _render_control_stat(label: str, value: Any, tone: str = "neutral") -> str:
    return (
        '<div class="stat-card">'
        f'<span class="stat-label">{html.escape(str(label))}</span>'
        f'<strong class="stat-value stat-tone-{html.escape(str(tone))}">{html.escape(str(value))}</strong>'
        "</div>"
    )


def _render_peer_cards(peers: list[dict]) -> str:
    if not peers:
        return '<div class="empty-state">No peer connections yet.</div>'
    cards = []
    for peer in peers:
        profile = dict(peer.get("device_profile") or {})
        cards.append(
            '<article class="control-card peer-card">'
            f'<div class="card-kicker">{html.escape(profile.get("device_class") or "peer")} node</div>'
            f'<h3>{html.escape(peer.get("display_name") or peer.get("peer_id") or "Peer")}</h3>'
            '<div class="card-meta">'
            f'<span>{html.escape(peer.get("peer_id") or "")}</span>'
            f'<span>{html.escape(peer.get("status") or "unknown")}</span>'
            f'<span>{html.escape(profile.get("form_factor") or "device")}</span>'
            "</div>"
            f'<p>{html.escape((peer.get("endpoint_url") or "").strip() or "endpoint unavailable")}</p>'
            "</article>"
        )
    return "".join(cards)


def _render_notification_cards(notifications: list[dict]) -> str:
    if not notifications:
        return '<div class="empty-state">No notifications for this node.</div>'
    cards = []
    for notification in notifications:
        status = notification.get("status") or "unread"
        cards.append(
            '<article class="control-card notification-card">'
            f'<div class="card-kicker">{html.escape(notification.get("notification_type") or "notification")}</div>'
            f'<h3>{html.escape(notification.get("title") or notification.get("compact_title") or "Untitled notification")}</h3>'
            '<div class="card-meta">'
            f'<span>{html.escape(notification.get("priority") or "normal")}</span>'
            f'<span>{html.escape(status)}</span>'
            f'<span>{html.escape(notification.get("created_at") or "")}</span>'
            "</div>"
            f'<p>{html.escape(notification.get("body") or notification.get("compact_body") or "")}</p>'
            '<div class="card-actions">'
            f'<button class="action-button" data-action="ack" data-notification-id="{html.escape(notification.get("id") or "")}"'
            + (' disabled="disabled"' if status != "unread" else "")
            + f'>{html.escape("Acked" if status != "unread" else "Acknowledge")}</button>'
            "</div>"
            "</article>"
        )
    return "".join(cards)


def _render_approval_cards(approvals: list[dict]) -> str:
    if not approvals:
        return '<div class="empty-state">No approval requests for this node.</div>'
    cards = []
    for approval in approvals:
        status = approval.get("status") or "pending"
        disabled = ' disabled="disabled"' if status != "pending" else ""
        cards.append(
            '<article class="control-card approval-card">'
            f'<div class="card-kicker">{html.escape(approval.get("action_type") or "approval")}</div>'
            f'<h3>{html.escape(approval.get("title") or "Approval request")}</h3>'
            '<div class="card-meta">'
            f'<span>{html.escape(approval.get("severity") or "normal")}</span>'
            f'<span>{html.escape(status)}</span>'
            f'<span>{html.escape(approval.get("created_at") or "")}</span>'
            "</div>"
            f'<p>{html.escape(approval.get("summary") or approval.get("compact_summary") or "")}</p>'
            '<div class="card-actions">'
            f'<button class="action-button success" data-action="resolve" data-decision="approved" data-approval-id="{html.escape(approval.get("id") or "")}"{disabled}>Approve</button>'
            f'<button class="action-button warn" data-action="resolve" data-decision="deferred" data-approval-id="{html.escape(approval.get("id") or "")}"{disabled}>Defer</button>'
            f'<button class="action-button danger" data-action="resolve" data-decision="rejected" data-approval-id="{html.escape(approval.get("id") or "")}"{disabled}>Reject</button>'
            "</div>"
            "</article>"
        )
    return "".join(cards)


def _job_action_specs(queue_message: dict, job: dict) -> list[dict]:
    queue = dict(queue_message or {})
    current_job = dict(job or {})
    recovery = dict(current_job.get("recovery") or {})
    actions: list[dict] = []
    queue_status = str(queue.get("status") or "").strip().lower()
    job_status = str(current_job.get("status") or "").strip().lower()
    resumable = bool(recovery.get("resumable")) or bool(current_job.get("latest_checkpoint_ref") or {})
    if resumable and job_status in {"checkpointed", "retry_wait", "failed"}:
        actions.append({"action": "resume", "label": "Resume Latest", "tone": "success"})
    if job_status not in {"completed", "rejected"}:
        actions.append({"action": "restart", "label": "Restart Fresh", "tone": "warn"})
    if queue_status in {"dead_letter", "cancelled"} and job_status not in {"checkpointed"}:
        actions.append({"action": "replay", "label": "Replay Queue", "tone": "accent"})
    if queue_status in {"queued", "inflight"} and job_status not in {"completed", "failed", "rejected", "cancelled"}:
        actions.append({"action": "cancel", "label": "Cancel Job", "tone": "danger"})
    return actions


def _render_operation_cards(queue_messages: list[dict], jobs_by_id: dict[str, dict]) -> str:
    if not queue_messages:
        return '<div class="empty-state">No queue or recovery activity yet.</div>'
    cards = []
    for queue_message in queue_messages:
        job = dict(jobs_by_id.get(queue_message.get("job_id") or "", {}) or {})
        if not job:
            continue
        recovery = dict(job.get("recovery") or {})
        checkpoint_ref = dict(job.get("latest_checkpoint_ref") or {})
        queue_status = queue_message.get("status") or "queued"
        job_status = job.get("status") or "queued"
        cards.append(
            '<article class="control-card operation-card">'
            f'<div class="card-kicker">{html.escape(job.get("kind") or "job")} job</div>'
            f'<h3>{html.escape(job.get("id") or "job")}</h3>'
            '<div class="card-meta">'
            f'<span>{html.escape(job_status)}</span>'
            f'<span>{html.escape(queue_status)}</span>'
            f'<span>{html.escape(queue_message.get("queue_name") or "default")}</span>'
            f'<span>{html.escape(str(queue_message.get("delivery_attempts") or 0))} deliveries</span>'
            "</div>"
            f'<p>{html.escape(recovery.get("recovery_hint") or queue_message.get("last_error") or "Queue-backed job ready for operator action.")}</p>'
            '<div class="card-meta">'
            f'<span>Resume count {html.escape(str(job.get("resume_count") or 0))}</span>'
            f'<span>{"checkpoint ready" if checkpoint_ref else "no checkpoint"}</span>'
            f'<span>{html.escape(job.get("updated_at") or "")}</span>'
            "</div>"
            '<div class="card-actions">'
            + "".join(
                [
                    f'<button class="action-button {html.escape(spec["tone"])}" data-action="{html.escape(spec["action"])}" data-job-id="{html.escape(job.get("id") or "")}" data-queue-message-id="{html.escape(queue_message.get("id") or "")}">{html.escape(spec["label"])}</button>'
                    for spec in _job_action_specs(queue_message, job)
                ]
            )
            + f'<a class="action-link" href="/mesh/jobs/{html.escape(job.get("id") or "")}">Inspect JSON</a>'
            + "</div>"
            "</article>"
        )
    return "".join(cards) if cards else '<div class="empty-state">No queue or recovery activity yet.</div>'


def _render_mesh_pressure_card(pressure: dict) -> str:
    tone = "calm"
    state = str(pressure.get("pressure") or "idle").strip().lower()
    if state == "saturated":
        tone = "alert"
    elif state == "elevated":
        tone = "warn"
    needs_help = bool(pressure.get("needs_help"))
    reasons = ", ".join(pressure.get("reasons") or []) or "No pressure signals."
    return (
        f'<article class="control-card pressure-card pressure-{html.escape(state)}">'
        f'<div class="card-kicker">Mesh Pressure</div>'
        f'<h3>{html.escape(state.title())} <span class="pressure-badge tone-{html.escape(tone)}">'
        f'{html.escape(str(pressure.get("queued") or 0))} queued'
        "</span></h3>"
        '<div class="card-meta">'
        f'<span>{html.escape(str(pressure.get("total_slots") or 0))} slots</span>'
        f'<span>{html.escape(str(pressure.get("available_slots") or 0))} free</span>'
        f'<span>{html.escape(str(pressure.get("inflight") or 0))} inflight</span>'
        f'<span>backlog {html.escape(str(pressure.get("backlog_ratio") or 0))}</span>'
        "</div>"
        f'<p>{html.escape(reasons)}</p>'
        '<div class="card-actions">'
        f'<button class="action-button {"success" if needs_help else "accent"}" data-action="auto-seek-help">'
        f'{"Get Help Now" if needs_help else "Plan Help"}</button>'
        '<a class="action-link" href="/mesh/pressure">Inspect JSON</a>'
        "</div>"
        "</article>"
    )


def _render_offload_autonomy_card(autonomy: dict) -> str:
    policy = dict(autonomy.get("policy") or {})
    pressure = dict(autonomy.get("pressure") or {})
    decision = str(autonomy.get("decision") or "noop").strip().lower()
    tone = {"auto_enlist": "success", "request_approval": "warn", "suggest": "accent"}.get(decision, "neutral")
    eligible_count = int(autonomy.get("eligible_candidate_count") or 0)
    reasons = ", ".join(autonomy.get("reasons") or []) or "No autonomous action recommended right now."
    button_label = "Run Offload Policy"
    if decision == "request_approval":
        button_label = "Request Approval"
    elif decision == "auto_enlist":
        button_label = "Auto-Enlist Helpers"
    return (
        '<article class="control-card autonomy-card">'
        '<div class="card-kicker">Autonomous Offload</div>'
        f'<h3>{html.escape(str(policy.get("mode") or "manual").title())} '
        f'<span class="pressure-badge tone-{html.escape(tone)}">{html.escape(decision or "noop")}</span></h3>'
        '<div class="card-meta">'
        f'<span>threshold {html.escape(str(policy.get("pressure_threshold") or "elevated"))}</span>'
        f'<span>{html.escape(str(pressure.get("pressure") or "idle"))} pressure</span>'
        f'<span>{html.escape(str(eligible_count))} eligible helpers</span>'
        f'<span>max {html.escape(str(policy.get("max_auto_enlist") or 0))}</span>'
        "</div>"
        f'<p>{html.escape(reasons)}</p>'
        '<div class="card-actions">'
        f'<button class="action-button {"success" if decision == "auto_enlist" else "accent"}" data-action="run-autonomy">{html.escape(button_label)}</button>'
        '<a class="action-link" href="/mesh/helpers/autonomy">Inspect JSON</a>'
        '</div>'
        '</article>'
    )


def _render_offload_preference_cards(preferences: list[dict]) -> str:
    if not preferences:
        return '<div class="empty-state">No offload memory yet. Approved or saved helper choices will appear here.</div>'
    cards = []
    for item in preferences:
        pref = str(item.get("preference") or "allow").strip().lower()
        tone = {"prefer": "success", "allow": "accent", "approval": "warn", "avoid": "warn", "deny": "alert"}.get(pref, "neutral")
        cards.append(
            '<article class="control-card helper-card">'
            '<div class="card-kicker">Offload Memory</div>'
            f'<h3>{html.escape(item.get("peer_id") or "peer")} <span class="pressure-badge tone-{html.escape(tone)}">{html.escape(pref)}</span></h3>'
            '<div class="card-meta">'
            f'<span>{html.escape(item.get("workload_class") or "default")}</span>'
            f'<span>{html.escape(item.get("source") or "operator")}</span>'
            f'<span>{html.escape(item.get("updated_at") or "")}</span>'
            '</div>'
            f'<p>{html.escape(str((item.get("metadata") or {}).get("note") or "Preference memory for autonomous helper selection."))}</p>'
            '</article>'
        )
    return "".join(cards)


def _render_helper_cards(helpers: list[dict]) -> str:
    if not helpers:
        return '<div class="empty-state">No peer helpers discovered yet.</div>'
    cards = []
    for helper in helpers:
        compute = dict(helper.get("compute_profile") or {})
        state = str(helper.get("state") or "unenlisted").strip().lower()
        tone = {"enlisted": "success", "draining": "warn", "unenlisted": "neutral"}.get(state, "neutral")
        gpu_badge = ""
        if compute.get("gpu_capable"):
            gpu_class = str(compute.get("gpu_class") or "gpu").upper()
            gpu_vram = int(compute.get("gpu_vram_mb") or 0)
            gpu_badge = f'<span class="pressure-badge tone-accent">{html.escape(gpu_class)} {gpu_vram}MB</span>'
        actions = ""
        peer_id = html.escape(helper.get("peer_id") or "")
        if state in {"unenlisted", "idle", ""}:
            actions += (
                f'<button class="action-button success" data-action="helper-enlist" data-peer-id="{peer_id}">Enlist</button>'
            )
        if state == "enlisted":
            actions += (
                f'<button class="action-button warn" data-action="helper-drain" data-peer-id="{peer_id}">Drain</button>'
            )
        actions += (
            f'<button class="action-button danger" data-action="helper-retire" data-peer-id="{peer_id}">Retire</button>'
        )
        cards.append(
            '<article class="control-card helper-card">'
            f'<div class="card-kicker">Helper — {html.escape(state)}</div>'
            f'<h3>{html.escape(helper.get("display_name") or helper.get("peer_id") or "Helper")} {gpu_badge}</h3>'
            '<div class="card-meta">'
            f'<span>{html.escape(helper.get("device_class") or "full")}</span>'
            f'<span>{html.escape(helper.get("execution_tier") or "standard")}</span>'
            f'<span>{html.escape(str(compute.get("cpu_cores") or 0))} CPU</span>'
            f'<span>{html.escape(str(compute.get("memory_mb") or 0))} MB</span>'
            f'<span class="tone-{html.escape(tone)}">{html.escape(state)}</span>'
            "</div>"
            f'<p>{html.escape(helper.get("last_reason") or helper.get("source") or "Peer available for compute overflow.")}</p>'
            f'<div class="card-actions">{actions}</div>'
            "</article>"
        )
    return "".join(cards)


def _render_cooperative_task_cards(tasks: list[dict]) -> str:
    if not tasks:
        return '<div class="empty-state">No cooperative task groups yet.</div>'
    cards = []
    for task in tasks:
        summary = dict(task.get("summary") or {})
        counts = dict(summary.get("counts") or {})
        state = str(task.get("state") or "pending").strip().lower()
        tone = {"completed": "success", "active": "accent", "attention": "alert", "pending": "neutral"}.get(state, "neutral")
        children = list(task.get("children") or [])
        shard_chips = []
        for child in children[:6]:
            placement = dict(child.get("placement") or {})
            gpu_mark = " GPU" if placement.get("target_gpu_capable") else ""
            shard_status = str((child.get("job") or {}).get("status") or "").strip() or "pending"
            shard_label = child.get("label") or f"shard-{child.get('shard_index') or 0}"
            shard_chips.append(
                f'<span class="shard-chip tone-{html.escape(tone)}">'
                f'{html.escape(shard_label)}'
                f'{html.escape(gpu_mark)} · {html.escape(shard_status)}</span>'
            )
        cards.append(
            '<article class="control-card coop-card">'
            f'<div class="card-kicker">Cooperative task — {html.escape(state)}</div>'
            f'<h3>{html.escape(task.get("name") or task.get("id") or "Task")}</h3>'
            '<div class="card-meta">'
            f'<span>{html.escape(task.get("strategy") or "spread")}</span>'
            f'<span>{html.escape(str(task.get("shard_count") or 0))} shards</span>'
            f'<span>{html.escape(str(counts.get("completed") or 0))} done</span>'
            f'<span>{html.escape(str(counts.get("running") or 0))} running</span>'
            f'<span>{html.escape(str(counts.get("failed") or 0))} failed</span>'
            "</div>"
            f'<div class="shard-row">{"".join(shard_chips)}</div>'
            '<div class="card-actions">'
            f'<a class="action-link" href="/mesh/cooperative-tasks/{html.escape(task.get("id") or "")}">Inspect JSON</a>'
            "</div>"
            "</article>"
        )
    return "".join(cards)


def build_control_page(mesh: SovereignMesh) -> str:
    initial_state = build_control_state(mesh)
    bootstrap = json.dumps(initial_state).replace("</", "<\\/")
    control_html = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <title>OCP Control Deck — Sovereign Distributed Compute Cockpit</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600;700&family=Space+Grotesk:wght@500;600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --ocp-bg: #03060f;
      --ocp-bg-deep: #02040a;
      --ocp-surface: rgba(6, 9, 15, 0.92);
      --ocp-surface-strong: rgba(8, 13, 24, 0.94);
      --ocp-surface-soft: rgba(255, 255, 255, 0.025);
      --ocp-grid: rgba(255, 255, 255, 0.04);
      --ocp-line: rgba(255, 255, 255, 0.07);
      --ocp-line-soft: rgba(255, 255, 255, 0.05);
      --ocp-line-strong: rgba(255, 255, 255, 0.14);
      --ocp-gold: #c8a96e;
      --ocp-cyan: #00d4ff;
      --ocp-green: #00ff88;
      --ocp-amber: #ff9500;
      --ocp-coral: #ff4757;
      --ocp-violet: #8b7fe8;
      --ocp-text: #e8f0ff;
      --ocp-text-secondary: #6b7a9f;
      --ocp-text-dim: #2a3350;
      --ocp-radius-lg: 24px;
      --ocp-radius-md: 16px;
      --ocp-radius-sm: 12px;
      --ocp-shadow: 0 24px 60px rgba(0, 0, 0, 0.32);
      --ocp-shadow-soft: 0 18px 38px rgba(0, 0, 0, 0.24);
      --ocp-transition: 200ms ease;
      --ocp-max-width: 1440px;
      --ocp-hero-height: 72px;
    }
    * {
      box-sizing: border-box;
    }
    html {
      scroll-behavior: smooth;
    }
    body {
      margin: 0;
      min-height: 100vh;
      background:
        radial-gradient(circle at top left, rgba(200, 169, 110, 0.12), transparent 22%),
        radial-gradient(circle at center, rgba(18, 28, 52, 0.68) 0%, rgba(3, 6, 15, 0) 42%),
        radial-gradient(circle at bottom right, rgba(0, 212, 255, 0.12), transparent 24%),
        linear-gradient(180deg, rgba(0, 212, 255, 0.06), transparent 28%),
        linear-gradient(180deg, var(--ocp-bg) 0%, var(--ocp-bg-deep) 100%);
      color: var(--ocp-text);
      font-family: "Inter", sans-serif;
      padding-bottom: 88px;
      overflow-x: hidden;
    }
    a {
      color: inherit;
    }
    button,
    input,
    textarea,
    select {
      font: inherit;
    }
    button {
      cursor: pointer;
    }
    #mesh-bg {
      position: fixed;
      inset: 0;
      width: 100vw;
      height: 100vh;
      z-index: 0;
      pointer-events: none;
    }
    .ocp-app {
      position: relative;
      z-index: 1;
      max-width: var(--ocp-max-width);
      margin: 0 auto;
      padding: 0 16px 88px;
    }
    .ocp-hero {
      position: sticky;
      top: 0;
      z-index: 30;
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
      align-items: center;
      min-height: var(--ocp-hero-height);
      padding: 14px 0 12px;
      backdrop-filter: blur(24px);
      background:
        linear-gradient(180deg, rgba(5, 8, 14, 0.98), rgba(5, 8, 14, 0.92));
      border-bottom: 0.5px solid var(--ocp-line);
    }
    .ocp-hero::after {
      content: "";
      position: absolute;
      left: 0;
      right: 0;
      bottom: -1px;
      height: 1px;
      background: linear-gradient(90deg, transparent, rgba(0, 212, 255, 0.32), transparent);
      pointer-events: none;
    }
    .ocp-hero__bar {
      display: grid;
      gap: 12px;
      grid-template-columns: 1fr;
      align-items: center;
      padding: 10px 14px;
      border: 0.5px solid var(--ocp-line-strong);
      border-radius: 24px;
      background:
        radial-gradient(circle at top right, rgba(0, 212, 255, 0.14), transparent 24%),
        linear-gradient(135deg, rgba(10, 16, 29, 0.98), rgba(4, 8, 15, 0.94));
      box-shadow: var(--ocp-shadow), inset 0 1px 0 rgba(255, 255, 255, 0.05);
    }
    .ocp-hero__cluster {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      min-width: 0;
    }
    .ocp-hero__node {
      display: flex;
      flex-direction: column;
      gap: 6px;
      min-width: 0;
    }
    .ocp-node-name {
      margin: 0;
      font-family: "Space Grotesk", sans-serif;
      font-size: 17px;
      font-weight: 700;
      letter-spacing: 0.01em;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .ocp-node-meta {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .ocp-pill {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
      min-height: 28px;
      padding: 0 12px;
      border-radius: 999px;
      border: 0.5px solid var(--ocp-line);
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      white-space: nowrap;
    }
    .ocp-pill--role {
      color: var(--ocp-cyan);
      background: rgba(0, 212, 255, 0.12);
      border-color: rgba(0, 212, 255, 0.25);
    }
    .ocp-pill--manual {
      color: var(--ocp-violet);
      background: rgba(139, 127, 232, 0.15);
      border-color: rgba(139, 127, 232, 0.28);
    }
    .ocp-pill--assisted {
      color: var(--ocp-amber);
      background: rgba(255, 149, 0, 0.14);
      border-color: rgba(255, 149, 0, 0.26);
    }
    .ocp-pill--autonomous {
      color: var(--ocp-cyan);
      background: rgba(0, 212, 255, 0.12);
      border-color: rgba(0, 212, 255, 0.26);
      box-shadow: 0 0 14px rgba(0, 212, 255, 0.18);
    }
    .ocp-pill--eligible {
      color: var(--ocp-green);
      background: rgba(0, 255, 136, 0.12);
      border-color: rgba(0, 255, 136, 0.24);
    }
    .ocp-pill--blocked {
      color: var(--ocp-coral);
      background: rgba(255, 71, 87, 0.12);
      border-color: rgba(255, 71, 87, 0.24);
    }
    .ocp-pill--warn {
      color: var(--ocp-amber);
      background: rgba(255, 149, 0, 0.14);
      border-color: rgba(255, 149, 0, 0.28);
    }
    .ocp-pill--violet {
      color: var(--ocp-violet);
      background: rgba(139, 127, 232, 0.15);
      border-color: rgba(139, 127, 232, 0.28);
    }
    .ocp-mono {
      font-family: "JetBrains Mono", monospace;
    }
    .ocp-version {
      font-size: 10px;
      color: var(--ocp-text-secondary);
      letter-spacing: 0.16em;
      text-transform: uppercase;
    }
    .ocp-mesh {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      min-width: 0;
    }
    .ocp-mesh__block {
      display: flex;
      flex-direction: column;
      gap: 6px;
      min-width: 0;
    }
    .ocp-label {
      color: var(--ocp-text-dim);
      font-size: 9px;
      letter-spacing: 0.22em;
      text-transform: uppercase;
      font-weight: 700;
    }
    .ocp-mesh__readout {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
    }
    .ocp-mesh__count {
      font-size: 28px;
      font-weight: 700;
      color: var(--ocp-cyan);
      transform-origin: center;
      transition: transform var(--ocp-transition), color var(--ocp-transition);
    }
    .ocp-mesh__count.is-zero {
      color: var(--ocp-coral);
    }
    .ocp-live-dots {
      display: inline-flex;
      gap: 6px;
      align-items: center;
    }
    .ocp-live-dots span {
      width: 7px;
      height: 7px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.18);
    }
    .ocp-live-dots.is-live span {
      background: rgba(0, 212, 255, 0.88);
      box-shadow: 0 0 12px rgba(0, 212, 255, 0.24);
      animation: ocpPulseDots 1.8s infinite ease-in-out;
    }
    .ocp-live-dots.is-live span:nth-child(2) {
      animation-delay: 0.22s;
    }
    .ocp-live-dots.is-live span:nth-child(3) {
      animation-delay: 0.44s;
    }
    .ocp-mesh__quality {
      color: var(--ocp-text-secondary);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }
    .ocp-hero__right {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
    }
    .ocp-hero-gauge {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .ocp-hero-gauge canvas {
      width: 74px;
      height: 38px;
      display: block;
    }
    .ocp-hero-gauge__meta {
      display: flex;
      flex-direction: column;
      gap: 4px;
      align-items: flex-start;
    }
    .ocp-hero-gauge__value {
      font-size: 14px;
      font-weight: 600;
      color: var(--ocp-text);
    }
    .ocp-status-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
      min-height: 24px;
      margin-top: 6px;
    }
    .ocp-status-note {
      color: var(--ocp-text-secondary);
      font-size: 12px;
    }
    .ocp-hero__deck {
      display: grid;
      gap: 14px;
      padding: 4px 2px 0;
    }
    .ocp-hero-story {
      display: grid;
      gap: 6px;
      padding: 16px 18px;
      border-radius: 18px;
      border: 0.5px solid rgba(255, 255, 255, 0.08);
      background:
        linear-gradient(135deg, rgba(255, 255, 255, 0.055), rgba(255, 255, 255, 0.018));
      box-shadow: var(--ocp-shadow-soft);
    }
    .ocp-hero-story__kicker {
      color: var(--ocp-gold);
      font-size: 10px;
      letter-spacing: 0.2em;
      text-transform: uppercase;
      font-weight: 700;
    }
    .ocp-hero-story__body {
      margin: 0;
      font-family: "Space Grotesk", sans-serif;
      font-size: 16px;
      line-height: 1.55;
      max-width: 920px;
    }
    .ocp-command-ribbon {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .ocp-command-ribbon__card {
      position: relative;
      display: grid;
      gap: 10px;
      min-height: 132px;
      padding: 16px;
      border-radius: 20px;
      border: 0.5px solid rgba(255, 255, 255, 0.08);
      background:
        radial-gradient(circle at top right, rgba(255, 255, 255, 0.07), transparent 32%),
        linear-gradient(165deg, rgba(11, 17, 30, 0.96), rgba(6, 10, 18, 0.92));
      overflow: hidden;
      box-shadow: var(--ocp-shadow-soft);
      transition: transform var(--ocp-transition), border-color var(--ocp-transition), box-shadow var(--ocp-transition);
    }
    .ocp-command-ribbon__card::before {
      content: "";
      position: absolute;
      inset: 0 auto 0 0;
      width: 3px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.14);
    }
    .ocp-command-ribbon__card:hover {
      transform: translateY(-2px);
      border-color: rgba(255, 255, 255, 0.14);
      box-shadow: 0 20px 48px rgba(0, 0, 0, 0.3);
    }
    .ocp-command-ribbon__card.is-cyan::before {
      background: var(--ocp-cyan);
    }
    .ocp-command-ribbon__card.is-warn::before {
      background: var(--ocp-amber);
    }
    .ocp-command-ribbon__card.is-danger::before {
      background: var(--ocp-coral);
    }
    .ocp-command-ribbon__card.is-safe::before {
      background: var(--ocp-green);
    }
    .ocp-command-ribbon__label {
      color: var(--ocp-text-dim);
      font-size: 9px;
      letter-spacing: 0.22em;
      text-transform: uppercase;
      font-weight: 700;
    }
    .ocp-command-ribbon__value {
      font-family: "Space Grotesk", sans-serif;
      font-size: 34px;
      line-height: 1;
      font-weight: 700;
    }
    .ocp-command-ribbon__detail {
      color: var(--ocp-text-secondary);
      font-size: 13px;
      line-height: 1.6;
    }
    .ocp-connect-layout {
      display: grid;
      gap: 16px;
    }
    .ocp-connect-panel,
    .ocp-connect-diagnostics {
      display: grid;
      gap: 14px;
      padding: 18px;
      border-radius: 20px;
      border: 0.5px solid rgba(255, 255, 255, 0.08);
      background: linear-gradient(160deg, rgba(255, 255, 255, 0.045), rgba(255, 255, 255, 0.02));
      box-shadow: var(--ocp-shadow-soft);
    }
    .ocp-connect-panel__head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
    }
    .ocp-connect-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .ocp-connect-manual {
      display: grid;
      gap: 12px;
    }
    .ocp-connect-manual__row {
      display: grid;
      gap: 10px;
      grid-template-columns: minmax(0, 1fr);
    }
    .ocp-connect-input {
      width: 100%;
      min-height: 50px;
      padding: 0 16px;
      border-radius: 14px;
      border: 1px solid rgba(255, 255, 255, 0.1);
      background: rgba(4, 7, 14, 0.82);
      color: var(--ocp-text);
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
    }
    .ocp-connect-input::placeholder {
      color: rgba(232, 240, 255, 0.34);
    }
    .ocp-connect-summary {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .ocp-connect-summary span,
    .ocp-connect-diagnostics__list span {
      display: inline-flex;
      align-items: center;
      min-height: 30px;
      padding: 0 12px;
      border-radius: 999px;
      border: 0.5px solid rgba(255, 255, 255, 0.08);
      background: rgba(255, 255, 255, 0.03);
      color: var(--ocp-text-secondary);
      font-size: 11px;
    }
    .ocp-connect-grid {
      display: grid;
      gap: 12px;
    }
    .ocp-connect-card {
      display: grid;
      gap: 12px;
      padding: 16px;
      border-radius: 18px;
      border: 0.5px solid rgba(255, 255, 255, 0.08);
      background: linear-gradient(165deg, rgba(255, 255, 255, 0.04), rgba(255, 255, 255, 0.02));
      box-shadow: var(--ocp-shadow-soft);
    }
    .ocp-connect-card__head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
    }
    .ocp-connect-card__title {
      margin: 0;
      font-family: "Space Grotesk", sans-serif;
      font-size: 16px;
      line-height: 1.35;
    }
    .ocp-connect-card__endpoint {
      color: var(--ocp-text-secondary);
      font-size: 12px;
      line-height: 1.5;
      word-break: break-all;
    }
    .ocp-connect-card__meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .ocp-connect-card__meta span {
      display: inline-flex;
      align-items: center;
      min-height: 26px;
      padding: 0 10px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.03);
      color: var(--ocp-text-secondary);
      font-size: 11px;
    }
    .ocp-connect-card__copy {
      color: var(--ocp-text-secondary);
      font-size: 13px;
      line-height: 1.6;
    }
    .ocp-connect-card__actions {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .ocp-connect-diagnostics__list {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .ocp-connect-errors {
      display: grid;
      gap: 10px;
    }
    .ocp-connect-error {
      padding: 12px 14px;
      border-radius: 14px;
      border: 0.5px solid rgba(255, 149, 0, 0.18);
      background: rgba(255, 149, 0, 0.08);
      color: #ffd5a6;
      font-size: 12px;
      line-height: 1.6;
    }
    .ocp-toolbar {
      display: inline-flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }
    .ocp-button,
    .ocp-link-button,
    .ocp-chip-button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      min-height: 44px;
      padding: 0 16px;
      border-radius: 12px;
      border: 1px solid var(--ocp-line);
      background: rgba(255, 255, 255, 0.04);
      color: var(--ocp-text);
      text-decoration: none;
      transition: transform var(--ocp-transition), border-color var(--ocp-transition), box-shadow var(--ocp-transition), background var(--ocp-transition), opacity var(--ocp-transition);
    }
    .ocp-button:hover,
    .ocp-link-button:hover,
    .ocp-chip-button:hover {
      border-color: rgba(255, 255, 255, 0.18);
      box-shadow: 0 0 16px rgba(0, 212, 255, 0.08);
    }
    .ocp-button:active,
    .ocp-link-button:active,
    .ocp-chip-button:active {
      transform: scale(0.98);
    }
    .ocp-button[disabled] {
      opacity: 0.62;
      cursor: wait;
    }
    .ocp-button--ghost {
      min-height: 40px;
      padding: 0 14px;
      color: var(--ocp-text-secondary);
    }
    .ocp-button--primary {
      min-height: 52px;
      width: 100%;
      font-family: "Space Grotesk", sans-serif;
      font-size: 16px;
      font-weight: 600;
      background: linear-gradient(135deg, #1a2d4a, #0a1a30);
      border: 1px solid rgba(0, 212, 255, 0.4);
      box-shadow: 0 0 24px rgba(0, 212, 255, 0.12);
    }
    .ocp-button--primary:hover {
      border-color: rgba(0, 212, 255, 0.7);
      box-shadow: 0 0 30px rgba(0, 212, 255, 0.18);
    }
    .ocp-button--secondary {
      min-height: 44px;
      font-weight: 600;
      background: rgba(255, 255, 255, 0.05);
    }
    .ocp-button--cyan {
      background: rgba(0, 212, 255, 0.18);
      border-color: rgba(0, 212, 255, 0.32);
    }
    .ocp-button--coral {
      background: transparent;
      color: var(--ocp-coral);
      border-color: rgba(255, 71, 87, 0.42);
    }
    .ocp-button--amber {
      background: rgba(255, 149, 0, 0.12);
      border-color: rgba(255, 149, 0, 0.28);
    }
    .ocp-spinner {
      width: 14px;
      height: 14px;
      border-radius: 999px;
      border: 2px solid rgba(255, 255, 255, 0.2);
      border-top-color: currentColor;
      animation: ocpSpin 0.8s linear infinite;
    }
    .ocp-main {
      display: grid;
      grid-template-columns: 1fr;
      gap: 20px;
      padding-top: 22px;
    }
    .ocp-panorama {
      margin-top: 18px;
      padding-bottom: 20px;
      background:
        radial-gradient(circle at top right, rgba(0, 212, 255, 0.1), transparent 34%),
        radial-gradient(circle at top left, rgba(200, 169, 110, 0.08), transparent 28%),
        linear-gradient(180deg, rgba(12, 20, 37, 0.98), rgba(5, 9, 16, 0.94));
    }
    .ocp-panorama__layout {
      display: grid;
      gap: 16px;
    }
    .ocp-panorama__summary,
    .ocp-panorama-feed {
      position: relative;
      z-index: 1;
      border-radius: 24px;
      border: 0.5px solid rgba(255, 255, 255, 0.08);
      background:
        linear-gradient(180deg, rgba(255, 255, 255, 0.045), rgba(255, 255, 255, 0.02));
      overflow: hidden;
      box-shadow: var(--ocp-shadow-soft);
    }
    .ocp-panorama__summary {
      padding: 16px;
    }
    .ocp-panorama__summary-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .ocp-panorama-card {
      position: relative;
      overflow: hidden;
      padding: 16px;
      border-radius: 18px;
      border: 0.5px solid rgba(255, 255, 255, 0.06);
      background:
        linear-gradient(155deg, rgba(255, 255, 255, 0.05), rgba(255, 255, 255, 0.02));
      min-height: 110px;
    }
    .ocp-panorama-card::before {
      content: "";
      position: absolute;
      inset: 0 auto 0 0;
      width: 3px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.18);
    }
    .ocp-panorama-card.is-safe::before {
      background: var(--ocp-green);
    }
    .ocp-panorama-card.is-warn::before {
      background: var(--ocp-amber);
    }
    .ocp-panorama-card.is-danger::before {
      background: var(--ocp-coral);
    }
    .ocp-panorama-card.is-cyan::before {
      background: var(--ocp-cyan);
    }
    .ocp-panorama-card.is-violet::before {
      background: var(--ocp-violet);
    }
    .ocp-panorama-card__eyebrow {
      color: var(--ocp-text-dim);
      font-size: 9px;
      font-weight: 700;
      letter-spacing: 0.18em;
      text-transform: uppercase;
    }
    .ocp-panorama-card__value {
      display: block;
      margin-top: 10px;
      font-size: 28px;
      font-weight: 700;
      line-height: 1;
      font-family: "Space Grotesk", sans-serif;
    }
    .ocp-panorama-card__detail {
      display: block;
      margin-top: 10px;
      color: var(--ocp-text-secondary);
      font-size: 12px;
      line-height: 1.5;
    }
    .ocp-panorama-headlines {
      display: grid;
      gap: 10px;
      margin-top: 14px;
    }
    .ocp-panorama-headline {
      display: grid;
      gap: 6px;
      padding: 16px 18px;
      border-radius: 18px;
      border: 0.5px solid rgba(255, 255, 255, 0.06);
      background: linear-gradient(135deg, rgba(255, 255, 255, 0.042), rgba(255, 255, 255, 0.015));
    }
    .ocp-panorama-headline__label {
      color: var(--ocp-text-dim);
      font-size: 9px;
      font-weight: 700;
      letter-spacing: 0.18em;
      text-transform: uppercase;
    }
    .ocp-panorama-headline__body {
      font-size: 14px;
      line-height: 1.55;
      color: var(--ocp-text);
    }
    .ocp-panorama-feed {
      display: grid;
      gap: 0;
    }
    .ocp-panorama-feed__header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 18px 18px 14px;
      border-bottom: 0.5px solid rgba(255, 255, 255, 0.06);
    }
    .ocp-panorama-feed__list {
      display: grid;
      max-height: 540px;
      overflow-y: auto;
    }
    .ocp-panorama-feed__row {
      display: grid;
      grid-template-columns: 4px minmax(0, 1fr);
      gap: 14px;
      align-items: stretch;
      padding: 16px 18px;
      text-decoration: none;
      color: inherit;
      border-top: 0.5px solid rgba(255, 255, 255, 0.05);
      background: rgba(255, 255, 255, 0.015);
      transition: background var(--ocp-transition), transform var(--ocp-transition), border-color var(--ocp-transition), box-shadow var(--ocp-transition);
    }
    .ocp-panorama-feed__row:hover {
      background: rgba(255, 255, 255, 0.04);
      transform: translateY(-1px);
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
    }
    .ocp-panorama-feed__row.is-fresh {
      background: linear-gradient(90deg, rgba(0, 212, 255, 0.08), rgba(255, 255, 255, 0.015));
    }
    .ocp-panorama-feed__row.is-danger.is-fresh {
      background: linear-gradient(90deg, rgba(255, 71, 87, 0.12), rgba(255, 255, 255, 0.015));
    }
    .ocp-panorama-feed__strip {
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.12);
    }
    .ocp-panorama-feed__row.is-safe .ocp-panorama-feed__strip {
      background: rgba(0, 255, 136, 0.82);
    }
    .ocp-panorama-feed__row.is-warn .ocp-panorama-feed__strip {
      background: rgba(255, 149, 0, 0.88);
    }
    .ocp-panorama-feed__row.is-danger .ocp-panorama-feed__strip {
      background: rgba(255, 71, 87, 0.94);
    }
    .ocp-panorama-feed__row.is-cyan .ocp-panorama-feed__strip {
      background: rgba(0, 212, 255, 0.88);
    }
    .ocp-panorama-feed__row.is-violet .ocp-panorama-feed__strip {
      background: rgba(139, 127, 232, 0.88);
    }
    .ocp-panorama-feed__body {
      display: grid;
      gap: 8px;
      min-width: 0;
    }
    .ocp-panorama-feed__head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
    }
    .ocp-panorama-feed__identity {
      display: grid;
      gap: 6px;
      min-width: 0;
    }
    .ocp-panorama-feed__title {
      margin: 0;
      font-family: "Space Grotesk", sans-serif;
      font-size: 15px;
      font-weight: 600;
      line-height: 1.4;
      word-break: break-word;
    }
    .ocp-panorama-feed__detail {
      margin: 0;
      color: var(--ocp-text-secondary);
      font-size: 13px;
      line-height: 1.55;
    }
    .ocp-panorama-feed__meta {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .ocp-panorama-feed__meta span {
      display: inline-flex;
      align-items: center;
      min-height: 28px;
      padding: 0 10px;
      border-radius: 999px;
      border: 0.5px solid rgba(255, 255, 255, 0.07);
      background: rgba(255, 255, 255, 0.03);
      color: var(--ocp-text-secondary);
      font-size: 11px;
    }
    .ocp-surface-tag {
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      width: fit-content;
      padding: 0 9px;
      border-radius: 999px;
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      border: 0.5px solid rgba(255, 255, 255, 0.08);
      background: rgba(255, 255, 255, 0.03);
      color: var(--ocp-text-secondary);
    }
    .ocp-surface-tag.is-safe {
      color: var(--ocp-green);
      border-color: rgba(0, 255, 136, 0.24);
      background: rgba(0, 255, 136, 0.12);
    }
    .ocp-surface-tag.is-warn {
      color: var(--ocp-amber);
      border-color: rgba(255, 149, 0, 0.24);
      background: rgba(255, 149, 0, 0.12);
    }
    .ocp-surface-tag.is-danger {
      color: var(--ocp-coral);
      border-color: rgba(255, 71, 87, 0.24);
      background: rgba(255, 71, 87, 0.12);
    }
    .ocp-surface-tag.is-cyan {
      color: var(--ocp-cyan);
      border-color: rgba(0, 212, 255, 0.24);
      background: rgba(0, 212, 255, 0.12);
    }
    .ocp-surface-tag.is-violet {
      color: #b7b0ff;
      border-color: rgba(139, 127, 232, 0.24);
      background: rgba(139, 127, 232, 0.12);
    }
    .ocp-column {
      display: grid;
      gap: 18px;
      align-content: start;
    }
    .ocp-section {
      position: relative;
      overflow: hidden;
      padding: 20px;
      border-radius: var(--ocp-radius-lg);
      border: 0.5px solid rgba(255, 255, 255, 0.09);
      background:
        radial-gradient(circle at top right, rgba(255, 255, 255, 0.045), transparent 30%),
        linear-gradient(180deg, rgba(8, 12, 21, 0.94), rgba(5, 9, 16, 0.92));
      box-shadow: var(--ocp-shadow);
      opacity: 0;
      transform: translateY(12px);
    }
    .ocp-section.is-visible {
      animation: ocpFadeUp 0.5s forwards ease;
      animation-delay: var(--ocp-delay, 0ms);
    }
    .ocp-section::before {
      content: "";
      position: absolute;
      inset: 0;
      background:
        linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.015), transparent),
        linear-gradient(180deg, rgba(255, 255, 255, 0.015), transparent 40%);
      pointer-events: none;
    }
    .ocp-section__header {
      position: relative;
      z-index: 1;
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 18px;
    }
    .ocp-section__title {
      margin: 0;
      font-family: "Space Grotesk", sans-serif;
      font-size: 21px;
      font-weight: 700;
      letter-spacing: -0.01em;
    }
    .ocp-section__subtitle {
      margin: 8px 0 0;
      color: var(--ocp-text-secondary);
      font-size: 13px;
      line-height: 1.65;
      max-width: 66ch;
    }
    .ocp-error {
      display: none;
      margin-top: 10px;
      padding: 10px 12px;
      border-radius: 12px;
      border: 0.5px solid rgba(255, 71, 87, 0.24);
      background: rgba(255, 71, 87, 0.08);
      color: #ffb2ba;
      font-size: 12px;
    }
    .ocp-error.is-visible {
      display: block;
    }
    .ocp-centerpiece {
      display: grid;
      gap: 18px;
    }
    .ocp-centerpiece__body {
      display: grid;
      grid-template-columns: 1fr;
      gap: 18px;
      align-items: center;
    }
    .ocp-gauge-panel {
      display: grid;
      gap: 12px;
      justify-items: center;
      text-align: center;
      padding: 8px 0 4px;
    }
    .ocp-gauge-wrap {
      position: relative;
      width: min(100%, 320px);
      aspect-ratio: 1;
    }
    .ocp-gauge-wrap canvas {
      width: 100%;
      height: 100%;
      display: block;
      filter: drop-shadow(0 0 20px rgba(0, 212, 255, 0.12));
    }
    .ocp-gauge-center {
      position: absolute;
      inset: 0;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      pointer-events: none;
    }
    .ocp-gauge-value {
      font-size: 36px;
      font-weight: 700;
      line-height: 1;
    }
    .ocp-gauge-caption {
      margin-top: 8px;
      color: var(--ocp-text-secondary);
      font-size: 9px;
      letter-spacing: 0.22em;
      text-transform: uppercase;
    }
    .ocp-readiness {
      display: grid;
      gap: 14px;
      align-content: start;
    }
    .ocp-readiness__summary {
      color: var(--ocp-text-secondary);
      font-size: 14px;
      line-height: 1.6;
    }
    .ocp-readiness__list {
      display: grid;
      gap: 10px;
    }
    .ocp-readiness__item {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      padding: 14px 16px;
      border-radius: 16px;
      border: 0.5px solid var(--ocp-line-soft);
      background: linear-gradient(135deg, rgba(255, 255, 255, 0.05), rgba(255, 255, 255, 0.02));
    }
    .ocp-readiness__item strong {
      font-family: "Space Grotesk", sans-serif;
      font-size: 14px;
      font-weight: 600;
    }
    .ocp-readiness__item span {
      color: var(--ocp-text-secondary);
      font-size: 12px;
    }
    .ocp-metric-strip {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-top: 4px;
    }
    .ocp-metric-pill {
      padding: 14px;
      border-radius: 16px;
      border: 0.5px solid var(--ocp-line-soft);
      background: linear-gradient(135deg, rgba(255, 255, 255, 0.045), rgba(255, 255, 255, 0.02));
    }
    .ocp-metric-pill__label {
      display: block;
      color: var(--ocp-text-dim);
      font-size: 9px;
      letter-spacing: 0.16em;
      text-transform: uppercase;
      margin-bottom: 6px;
    }
    .ocp-metric-pill__value {
      display: block;
      font-size: 16px;
      font-weight: 600;
    }
    .ocp-card-grid {
      display: grid;
      gap: 12px;
    }
    .ocp-helper-grid,
    .ocp-peer-grid {
      display: grid;
      gap: 12px;
      grid-auto-flow: column;
      grid-auto-columns: minmax(280px, 82vw);
      overflow-x: auto;
      scroll-snap-type: x mandatory;
      padding-bottom: 2px;
    }
    .ocp-helper-grid > *,
    .ocp-peer-grid > * {
      scroll-snap-align: start;
    }
    .ocp-helper-card,
    .ocp-peer-card {
      position: relative;
      padding: 18px;
      border-radius: 18px;
      border: 0.5px solid rgba(255, 255, 255, 0.06);
      background:
        radial-gradient(circle at top right, rgba(0, 212, 255, 0.08), transparent 28%),
        linear-gradient(180deg, rgba(255, 255, 255, 0.04), rgba(255, 255, 255, 0.022));
      min-width: 0;
      box-shadow: var(--ocp-shadow-soft);
    }
    .ocp-helper-card__top,
    .ocp-peer-card__top {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
      margin-bottom: 12px;
    }
    .ocp-helper-card__name,
    .ocp-peer-card__id {
      margin: 0;
      font-family: "Space Grotesk", sans-serif;
      font-size: 13px;
      font-weight: 600;
      line-height: 1.4;
    }
    .ocp-status-dot {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.2);
      flex: 0 0 auto;
      margin-top: 4px;
    }
    .ocp-status-dot--live {
      background: var(--ocp-green);
      box-shadow: 0 0 0 rgba(0, 255, 136, 0.35);
      animation: ocpStatusPulse 2s infinite;
    }
    .ocp-status-dot--busy {
      background: var(--ocp-amber);
      box-shadow: 0 0 12px rgba(255, 149, 0, 0.16);
    }
    .ocp-status-dot--offline {
      background: rgba(107, 122, 159, 0.4);
    }
    .ocp-helper-card__caps,
    .ocp-peer-tags {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 12px;
    }
    .ocp-mini-tag {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.04);
      color: var(--ocp-text-secondary);
      font-size: 11px;
    }
    .ocp-mini-tag--gpu {
      background: rgba(0, 212, 255, 0.15);
      color: var(--ocp-cyan);
    }
    .ocp-mini-tag--trust {
      background: rgba(139, 127, 232, 0.16);
      color: #b7b0ff;
    }
    .ocp-helper-card__meta,
    .ocp-peer-card__meta {
      display: grid;
      gap: 8px;
      color: var(--ocp-text-secondary);
      font-size: 12px;
      line-height: 1.5;
    }
    .ocp-helper-card__actions {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-top: 14px;
      flex-wrap: wrap;
    }
    .ocp-helper-card__memory {
      display: grid;
      gap: 10px;
      margin-top: 14px;
      padding-top: 14px;
      border-top: 0.5px solid rgba(255, 255, 255, 0.06);
    }
    .ocp-helper-card__memory-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
    }
    .ocp-helper-card__memory-title {
      color: var(--ocp-text-secondary);
      font-size: 11px;
      letter-spacing: 0.16em;
      text-transform: uppercase;
    }
    .ocp-helper-card__memory-note {
      color: var(--ocp-text-secondary);
      font-size: 12px;
      line-height: 1.5;
    }
    .ocp-helper-pref-group {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .ocp-chip-button.is-active {
      border-color: rgba(0, 212, 255, 0.34);
      background: rgba(0, 212, 255, 0.14);
      color: var(--ocp-text);
      box-shadow: 0 0 16px rgba(0, 212, 255, 0.12);
    }
    .ocp-chip-button.is-danger {
      border-color: rgba(255, 71, 87, 0.34);
      color: var(--ocp-coral);
    }
    .ocp-rank {
      color: var(--ocp-text-secondary);
      font-size: 11px;
      letter-spacing: 0.1em;
      text-transform: uppercase;
    }
    .ocp-relative {
      color: var(--ocp-text-secondary);
      font-size: 11px;
    }
    .ocp-task-list,
    .ocp-operation-list,
    .ocp-offload-table,
    .ocp-notification-list {
      display: grid;
      gap: 10px;
      border-radius: 0;
      border: 0;
      overflow: visible;
      background: transparent;
    }
    .ocp-task-row,
    .ocp-operation-row,
    .ocp-offload-row,
    .ocp-notification-row {
      display: grid;
      gap: 12px;
      align-items: center;
      padding: 16px 18px;
      border-radius: 18px;
      border: 0.5px solid rgba(255, 255, 255, 0.07);
      background:
        linear-gradient(160deg, rgba(255, 255, 255, 0.04), rgba(255, 255, 255, 0.016));
      box-shadow: var(--ocp-shadow-soft);
    }
    .ocp-task-row:nth-child(even),
    .ocp-operation-row:nth-child(even),
    .ocp-offload-row:nth-child(even),
    .ocp-notification-row:nth-child(even) {
      background:
        linear-gradient(160deg, rgba(255, 255, 255, 0.03), rgba(255, 255, 255, 0.012));
    }
    .ocp-task-row:hover,
    .ocp-operation-row:hover,
    .ocp-offload-row:hover,
    .ocp-notification-row:hover {
      background:
        linear-gradient(160deg, rgba(255, 255, 255, 0.055), rgba(255, 255, 255, 0.022));
      border-color: rgba(255, 255, 255, 0.12);
    }
    .ocp-task-row {
      grid-template-columns: 18px minmax(0, 1fr);
    }
    .ocp-task-main,
    .ocp-operation-main,
    .ocp-notification-main {
      display: grid;
      gap: 10px;
    }
    .ocp-task-head,
    .ocp-operation-head,
    .ocp-notification-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
    }
    .ocp-task-id,
    .ocp-operation-id,
    .ocp-peer-id {
      font-family: "JetBrains Mono", monospace;
      font-size: 12px;
      color: var(--ocp-text);
    }
    .ocp-progress {
      height: 8px;
      border-radius: 999px;
      overflow: hidden;
      background: rgba(255, 255, 255, 0.08);
    }
    .ocp-progress > span {
      display: block;
      height: 100%;
      border-radius: inherit;
      background: linear-gradient(90deg, var(--ocp-cyan), var(--ocp-green));
      box-shadow: 0 0 18px rgba(0, 212, 255, 0.16);
    }
    .ocp-task-meta,
    .ocp-operation-meta {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      color: var(--ocp-text-secondary);
      font-size: 12px;
    }
    .ocp-task-icon {
      width: 16px;
      height: 16px;
      opacity: 0.75;
    }
    .ocp-segmented {
      position: relative;
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 6px;
      padding: 6px;
      border-radius: 16px;
      border: 0.5px solid var(--ocp-line);
      background: rgba(255, 255, 255, 0.03);
      overflow: hidden;
    }
    .ocp-segmented__indicator {
      position: absolute;
      top: 6px;
      bottom: 6px;
      left: 6px;
      width: calc((100% - 24px) / 3);
      border-radius: 12px;
      background: rgba(139, 127, 232, 0.2);
      border: 0.5px solid rgba(255, 255, 255, 0.08);
      transition: transform 200ms ease, background 200ms ease;
    }
    .ocp-segmented__button {
      position: relative;
      z-index: 1;
      min-height: 44px;
      border: 0;
      background: transparent;
      color: var(--ocp-text-secondary);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      cursor: default;
    }
    .ocp-segmented__button.is-active {
      color: var(--ocp-text);
    }
    .ocp-autonomy-copy {
      color: var(--ocp-text-secondary);
      font-size: 14px;
      line-height: 1.6;
    }
    .ocp-approvals--hot {
      border-color: rgba(255, 149, 0, 0.18);
      box-shadow: 0 0 24px rgba(255, 149, 0, 0.08), var(--ocp-shadow);
    }
    .ocp-approvals__pulse {
      display: inline-flex;
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: var(--ocp-amber);
      box-shadow: 0 0 0 rgba(255, 149, 0, 0.42);
      animation: ocpStatusPulse 2s infinite;
    }
    .ocp-approval-card {
      display: grid;
      gap: 12px;
      padding: 16px;
      border-left: 2px solid rgba(255, 149, 0, 0.72);
      border-radius: 18px;
      background: linear-gradient(165deg, rgba(255, 255, 255, 0.04), rgba(255, 255, 255, 0.02));
      border-top: 0.5px solid var(--ocp-line-soft);
      border-right: 0.5px solid var(--ocp-line-soft);
      border-bottom: 0.5px solid var(--ocp-line-soft);
      box-shadow: var(--ocp-shadow-soft);
    }
    .ocp-approval-card.is-dim {
      opacity: 0.56;
      border-left-color: rgba(255, 255, 255, 0.12);
    }
    .ocp-approval-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
    }
    .ocp-approval-head h4,
    .ocp-notification-title {
      margin: 0;
      font-size: 15px;
      font-weight: 600;
      font-family: "Space Grotesk", sans-serif;
    }
    .ocp-approval-summary {
      margin: 0;
      color: var(--ocp-text-secondary);
      font-size: 13px;
      line-height: 1.55;
    }
    .ocp-approval-meta,
    .ocp-notification-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      color: var(--ocp-text-secondary);
      font-size: 12px;
    }
    .ocp-approval-actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    .ocp-notification-list {
      max-height: 620px;
      overflow-y: auto;
    }
    .ocp-notification-row {
      grid-template-columns: 4px minmax(0, 1fr) auto;
    }
    .ocp-notification-strip {
      align-self: stretch;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.14);
    }
    .ocp-notification-row.is-low .ocp-notification-strip {
      background: rgba(0, 212, 255, 0.44);
    }
    .ocp-notification-row.is-normal .ocp-notification-strip {
      background: rgba(0, 255, 136, 0.44);
    }
    .ocp-notification-row.is-high .ocp-notification-strip {
      background: rgba(255, 149, 0, 0.72);
    }
    .ocp-notification-row.is-critical .ocp-notification-strip {
      background: rgba(255, 71, 87, 0.9);
    }
    .ocp-notification-row.is-dim {
      opacity: 0.52;
    }
    .ocp-developer details {
      border-radius: 16px;
      border: 0.5px solid var(--ocp-line);
      background: rgba(255, 255, 255, 0.02);
      overflow: hidden;
    }
    .ocp-developer summary {
      list-style: none;
      cursor: pointer;
      padding: 14px 16px;
      font-family: "Space Grotesk", sans-serif;
      font-size: 14px;
      color: var(--ocp-text-secondary);
    }
    .ocp-developer summary::-webkit-details-marker {
      display: none;
    }
    .ocp-inspect-overlay {
      position: fixed;
      inset: 0;
      z-index: 120;
      display: grid;
      align-items: stretch;
      justify-items: end;
      padding: 0;
      background: rgba(2, 5, 12, 0.72);
      backdrop-filter: blur(20px);
      opacity: 0;
      pointer-events: none;
      transition: opacity var(--ocp-transition);
    }
    .ocp-inspect-overlay.is-open {
      opacity: 1;
      pointer-events: auto;
    }
    .ocp-inspect-backdrop {
      position: absolute;
      inset: 0;
      border: 0;
      padding: 0;
      margin: 0;
      background: transparent;
      cursor: pointer;
    }
    .ocp-inspect-panel {
      position: relative;
      z-index: 1;
      width: min(760px, 100vw);
      height: 100vh;
      overflow-y: auto;
      padding: 24px 20px 36px;
      background:
        radial-gradient(circle at top right, rgba(0, 212, 255, 0.16), transparent 24%),
        radial-gradient(circle at top left, rgba(200, 169, 110, 0.12), transparent 20%),
        linear-gradient(180deg, rgba(12, 18, 33, 0.985), rgba(6, 10, 19, 0.985));
      border-left: 0.5px solid rgba(255, 255, 255, 0.08);
      box-shadow: -24px 0 64px rgba(0, 0, 0, 0.34);
      transform: translateX(24px);
      transition: transform var(--ocp-transition);
    }
    .ocp-inspect-overlay.is-open .ocp-inspect-panel {
      transform: translateX(0);
    }
    .ocp-inspect-panel__head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 18px;
      position: sticky;
      top: -24px;
      z-index: 2;
      padding: 0 0 12px;
      background: linear-gradient(180deg, rgba(10, 15, 28, 0.98), rgba(10, 15, 28, 0.8), transparent);
      backdrop-filter: blur(12px);
    }
    .ocp-inspect-panel__title {
      margin: 8px 0 0;
      font-family: "Space Grotesk", sans-serif;
      font-size: 24px;
      line-height: 1.2;
    }
    .ocp-inspect-panel__subtitle {
      margin: 8px 0 0;
      color: var(--ocp-text-secondary);
      font-size: 13px;
      line-height: 1.6;
    }
    .ocp-inspect-panel__body {
      display: grid;
      gap: 14px;
      position: relative;
      z-index: 1;
    }
    .ocp-inspect-close {
      min-width: 44px;
      min-height: 44px;
      padding: 0 14px;
      border-radius: 14px;
      border: 1px solid var(--ocp-line);
      background: rgba(255, 255, 255, 0.04);
      color: var(--ocp-text-secondary);
    }
    .ocp-inspect-section {
      padding: 18px;
      border-radius: 20px;
      border: 0.5px solid rgba(255, 255, 255, 0.06);
      background:
        linear-gradient(160deg, rgba(255, 255, 255, 0.045), rgba(255, 255, 255, 0.022));
      box-shadow: var(--ocp-shadow-soft);
    }
    .ocp-inspect-section__title {
      margin: 0 0 12px;
      font-family: "Space Grotesk", sans-serif;
      font-size: 15px;
      font-weight: 600;
    }
    .ocp-inspect-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .ocp-inspect-stat {
      padding: 14px;
      border-radius: 16px;
      border: 0.5px solid rgba(255, 255, 255, 0.06);
      background: rgba(255, 255, 255, 0.028);
    }
    .ocp-inspect-stat__label {
      display: block;
      color: var(--ocp-text-dim);
      font-size: 9px;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      margin-bottom: 8px;
    }
    .ocp-inspect-stat__value {
      display: block;
      color: var(--ocp-text);
      font-size: 13px;
      line-height: 1.5;
      word-break: break-word;
    }
    .ocp-inspect-copy {
      color: var(--ocp-text-secondary);
      font-size: 13px;
      line-height: 1.65;
    }
    .ocp-inspect-list {
      display: grid;
      gap: 10px;
    }
    .ocp-inspect-item {
      display: grid;
      gap: 8px;
      padding: 14px;
      border-radius: 16px;
      border: 0.5px solid rgba(255, 255, 255, 0.06);
      background: rgba(255, 255, 255, 0.028);
    }
    .ocp-inspect-item__head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
    }
    .ocp-inspect-item__title {
      font-family: "JetBrains Mono", monospace;
      font-size: 12px;
      color: var(--ocp-text);
    }
    .ocp-inspect-item__meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      color: var(--ocp-text-secondary);
      font-size: 12px;
    }
    .ocp-json-preview {
      margin: 0;
      padding: 14px;
      border-radius: 16px;
      border: 0.5px solid rgba(255, 255, 255, 0.06);
      background: rgba(4, 7, 14, 0.84);
      color: #bfe8ff;
      font-family: "JetBrains Mono", monospace;
      font-size: 11px;
      line-height: 1.65;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .ocp-button--inspect {
      min-height: 38px;
      padding: 0 12px;
      font-size: 11px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--ocp-text-secondary);
    }
    .ocp-panorama-feed__row.ocp-panorama-feed__row--button {
      width: 100%;
      border: 0;
      text-align: left;
      cursor: pointer;
      font: inherit;
    }
    .ocp-inspect-overlay .ocp-empty {
      background: rgba(255, 255, 255, 0.025);
    }
    .ocp-surface-links {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      padding: 0 16px 16px;
    }
    .ocp-chip-button {
      min-height: 38px;
      padding: 0 12px;
      font-size: 11px;
      font-family: "JetBrains Mono", monospace;
      color: var(--ocp-text-secondary);
    }
    .ocp-empty {
      padding: 20px;
      border-radius: 18px;
      border: 0.5px dashed rgba(255, 255, 255, 0.12);
      text-align: center;
      color: var(--ocp-text-secondary);
      background: linear-gradient(160deg, rgba(255, 255, 255, 0.032), rgba(255, 255, 255, 0.016));
      font-size: 13px;
      line-height: 1.6;
    }
    .ocp-empty--fleet {
      padding: 24px;
    }
    .ocp-mono-link {
      font-family: "JetBrains Mono", monospace;
    }
    .ocp-scale-pop {
      animation: ocpScalePulse 240ms ease;
    }
    @keyframes ocpFadeUp {
      from {
        opacity: 0;
        transform: translateY(12px);
      }
      to {
        opacity: 1;
        transform: translateY(0);
      }
    }
    @keyframes ocpPulseDots {
      0%, 100% {
        opacity: 0.42;
        transform: scale(0.9);
      }
      50% {
        opacity: 1;
        transform: scale(1.08);
      }
    }
    @keyframes ocpStatusPulse {
      0% {
        box-shadow: 0 0 0 0 rgba(0, 255, 136, 0.35);
      }
      70% {
        box-shadow: 0 0 0 10px rgba(0, 255, 136, 0);
      }
      100% {
        box-shadow: 0 0 0 0 rgba(0, 255, 136, 0);
      }
    }
    @keyframes ocpSpin {
      to {
        transform: rotate(360deg);
      }
    }
    @keyframes ocpScalePulse {
      0% {
        transform: scale(1);
      }
      50% {
        transform: scale(1.1);
      }
      100% {
        transform: scale(1);
      }
    }
    @media (min-width: 640px) {
      :root {
        --ocp-hero-height: 80px;
      }
      .ocp-app {
        padding-left: 20px;
        padding-right: 20px;
      }
      .ocp-hero__bar {
        grid-template-columns: minmax(0, 1.2fr) minmax(0, 1fr) auto;
      }
      .ocp-command-ribbon {
        grid-template-columns: repeat(4, minmax(0, 1fr));
      }
      .ocp-connect-manual__row {
        grid-template-columns: minmax(0, 1fr) auto auto;
      }
      .ocp-hero__cluster--center,
      .ocp-hero__cluster--right {
        justify-content: center;
      }
      .ocp-centerpiece__body {
        grid-template-columns: minmax(280px, 360px) minmax(0, 1fr);
      }
      .ocp-panorama__summary-grid {
        grid-template-columns: repeat(3, minmax(0, 1fr));
      }
      .ocp-inspect-panel {
        padding-left: 22px;
        padding-right: 22px;
      }
      .ocp-metric-strip {
        grid-template-columns: repeat(4, minmax(0, 1fr));
      }
      .ocp-task-row,
      .ocp-operation-row {
        grid-template-columns: 18px minmax(0, 1fr);
      }
      .ocp-offload-row {
        grid-template-columns: minmax(0, 1.3fr) 90px minmax(0, 1fr) 110px auto;
      }
    }
    @media (min-width: 1024px) {
      .ocp-app {
        padding-left: 24px;
        padding-right: 24px;
      }
      .ocp-hero-story__body {
        font-size: 18px;
      }
      .ocp-connect-layout {
        grid-template-columns: minmax(0, 1.2fr) minmax(320px, 0.8fr);
      }
      .ocp-connect-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .ocp-panorama__layout {
        grid-template-columns: minmax(0, 1.05fr) minmax(360px, 0.95fr);
      }
      .ocp-main {
        grid-template-columns: minmax(0, 1.45fr) minmax(360px, 0.95fr);
      }
      .ocp-helper-grid,
      .ocp-peer-grid {
        grid-auto-flow: initial;
        grid-auto-columns: initial;
        overflow: visible;
      }
      .ocp-helper-grid {
        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      }
      .ocp-peer-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }
  </style>
</head>
<body>
  <canvas id="mesh-bg" aria-hidden="true"></canvas>
  <div class="ocp-app">
    <header class="ocp-hero">
      <div class="ocp-hero__bar ocp-section is-visible" style="--ocp-delay: 0ms;">
        <div class="ocp-hero__cluster ocp-hero__cluster--node">
          <div class="ocp-hero__node">
            <span class="ocp-version">OCP Control Deck</span>
            <h1 class="ocp-node-name" id="hero-node-name">Node</h1>
            <div class="ocp-node-meta">
              <span class="ocp-pill ocp-pill--role" id="hero-role-pill">SOVEREIGN NODE</span>
              <span class="ocp-version ocp-mono" id="hero-version">OCP runtime</span>
            </div>
          </div>
          <div class="ocp-toolbar">
            <button class="ocp-button ocp-button--ghost" id="refresh-button" type="button">Refresh Deck</button>
          </div>
        </div>
        <div class="ocp-hero__cluster ocp-hero__cluster--center">
          <div class="ocp-mesh">
            <div class="ocp-mesh__block">
              <span class="ocp-label">Mesh</span>
              <div class="ocp-mesh__readout">
                <span class="ocp-mesh__count ocp-mono" id="hero-peer-count">0</span>
                <span class="ocp-live-dots" id="hero-live-dots"><span></span><span></span><span></span></span>
              </div>
            </div>
            <div class="ocp-mesh__block">
              <span class="ocp-label">Quality</span>
              <span class="ocp-mesh__quality" id="hero-quality">isolated</span>
            </div>
          </div>
        </div>
        <div class="ocp-hero__cluster ocp-hero__cluster--right">
          <div class="ocp-hero-gauge">
            <canvas id="hero-pressure-canvas" width="148" height="76" aria-hidden="true"></canvas>
            <div class="ocp-hero-gauge__meta">
              <span class="ocp-label">Pressure</span>
              <span class="ocp-hero-gauge__value ocp-mono" id="hero-pressure-value">0%</span>
            </div>
          </div>
          <span class="ocp-pill ocp-pill--manual" id="hero-autonomy-pill">MANUAL</span>
        </div>
      </div>
      <div class="ocp-status-row">
        <span class="ocp-status-note" id="status-note">Sovereign mesh cockpit online.</span>
        <span class="ocp-status-note ocp-mono" id="hero-observed-at"></span>
      </div>
      <div class="ocp-error" data-error-for="hero"></div>
    </header>

    <section class="ocp-section ocp-panorama" data-section>
      <div class="ocp-section__header">
        <div>
          <h2 class="ocp-section__title">Mesh Pulse</h2>
          <p class="ocp-section__subtitle">A live wall of mission, queue, helper, approval, and notification activity so you can see the whole mesh move at once.</p>
        </div>
        <span class="ocp-pill ocp-pill--eligible" id="pulse-state-pill">STEADY</span>
      </div>
      <div class="ocp-panorama__layout">
        <div class="ocp-panorama__summary">
          <div class="ocp-panorama__summary-grid" id="pulse-summary-grid"></div>
          <div class="ocp-panorama-headlines" id="pulse-headlines"></div>
        </div>
        <div class="ocp-panorama-feed">
          <div class="ocp-panorama-feed__header">
            <span class="ocp-label">Live Mission Stream</span>
            <span class="ocp-status-note" id="pulse-feed-note">Waiting for activity.</span>
          </div>
          <div class="ocp-panorama-feed__list" id="pulse-activity-feed"></div>
        </div>
      </div>
    </section>

    <section class="ocp-section" data-section>
      <div class="ocp-section__header">
        <div>
          <h2 class="ocp-section__title">Connect Devices</h2>
          <p class="ocp-section__subtitle">Make trusted machines feel like one sovereign mesh: scan nearby nodes, connect in one click, and launch a proof mission without dropping into raw JSON.</p>
        </div>
        <span class="ocp-pill ocp-pill--violet" id="connect-state-pill">DISCOVERY READY</span>
      </div>
      <div class="ocp-connect-layout">
        <div class="ocp-connect-panel">
          <div class="ocp-connect-panel__head">
            <div>
              <span class="ocp-version">Pair + Verify</span>
              <div class="ocp-connect-summary" id="connect-summary"></div>
            </div>
            <div class="ocp-connect-actions">
              <button class="ocp-button ocp-button--secondary" type="button" data-action="scan-local-peers">Scan Nearby</button>
              <button class="ocp-button ocp-button--cyan" type="button" data-action="connect-all-peers">Connect Everything</button>
            </div>
          </div>
          <div class="ocp-connect-manual">
            <div class="ocp-connect-manual__row">
              <input class="ocp-connect-input" id="connect-device-url" type="text" placeholder="Paste a device URL like http://172.20.10.4:8431">
              <button class="ocp-button ocp-button--cyan" type="button" data-action="connect-peer-manual">Connect</button>
              <button class="ocp-button ocp-button--amber" type="button" data-action="send-test-mission-manual">Send Test Mission</button>
            </div>
            <div class="ocp-status-note" id="connect-manual-note">Use manual URL entry only when local scan misses a device or a firewall needs explicit troubleshooting.</div>
          </div>
          <div class="ocp-connect-grid" id="connect-grid"></div>
        </div>
        <aside class="ocp-connect-diagnostics">
          <div>
            <span class="ocp-version">Diagnostics</span>
            <h3 class="ocp-section__title" style="font-size:18px; margin-top:8px;">Reachability Hints</h3>
            <p class="ocp-section__subtitle" style="margin-top:8px;">We surface local IPs, advertised address, and recent discovery failures so connecting a second computer stops feeling like guesswork.</p>
          </div>
          <div class="ocp-connect-diagnostics__list" id="connect-diagnostics"></div>
          <div class="ocp-connect-errors" id="connect-errors"></div>
        </aside>
      </div>
      <div class="ocp-error" data-error-for="connect"></div>
    </section>

    <main class="ocp-main">
      <div class="ocp-column">
        <section class="ocp-section" data-section>
          <div class="ocp-section__header">
            <div>
              <h2 class="ocp-section__title">Pressure &amp; Offload Centerpiece</h2>
              <p class="ocp-section__subtitle">Distributed compute pressure, offload readiness, and the fastest path to extra capacity.</p>
            </div>
            <span class="ocp-pill ocp-pill--eligible" id="offload-state-pill">NO OFFLOAD</span>
          </div>
          <div class="ocp-centerpiece">
            <div class="ocp-centerpiece__body">
              <div class="ocp-gauge-panel">
                <div class="ocp-gauge-wrap">
                  <canvas id="pressure-gauge-canvas" width="420" height="420" aria-hidden="true"></canvas>
                  <div class="ocp-gauge-center">
                    <div class="ocp-gauge-value ocp-mono" id="pressure-gauge-value">0%</div>
                    <div class="ocp-gauge-caption">Load</div>
                  </div>
                </div>
                <div class="ocp-readiness__summary" id="pressure-summary">No pressure signals.</div>
              </div>
              <div class="ocp-readiness">
                <div class="ocp-readiness__summary" id="offload-summary">No helper recommendations are active.</div>
                <div class="ocp-readiness__list" id="offload-candidates"></div>
                <button class="ocp-button ocp-button--primary" data-action="auto-seek-help" type="button">Get Help Now</button>
              </div>
            </div>
            <div class="ocp-metric-strip" id="centerpiece-metrics"></div>
          </div>
          <div class="ocp-error" data-error-for="centerpiece"></div>
        </section>

        <section class="ocp-section" data-section>
          <div class="ocp-section__header">
            <div>
              <h2 class="ocp-section__title">Recovery + Queue</h2>
              <p class="ocp-section__subtitle">Resume, restart, replay, or cancel queue-backed jobs without leaving the cockpit.</p>
            </div>
          </div>
          <div class="ocp-operation-list" id="operation-grid"></div>
          <div class="ocp-error" data-error-for="operations"></div>
        </section>

        <section class="ocp-section" data-section>
          <div class="ocp-section__header">
            <div>
              <h2 class="ocp-section__title">Mission Layer</h2>
              <p class="ocp-section__subtitle">Durable mission intent above jobs and cooperative tasks, with lifecycle, continuity, and lineage hints.</p>
            </div>
          </div>
          <div class="ocp-task-list" id="mission-grid"></div>
          <div class="ocp-error" data-error-for="missions"></div>
        </section>

        <section class="ocp-section" data-section>
          <div class="ocp-section__header">
            <div>
              <h2 class="ocp-section__title">Cooperative Tasks</h2>
              <p class="ocp-section__subtitle">Shard groups spread across the mesh with clear status, helper placement, and progress.</p>
            </div>
          </div>
          <div class="ocp-task-list" id="coop-grid"></div>
          <div class="ocp-error" data-error-for="tasks"></div>
        </section>

        <section class="ocp-section" data-section>
          <div class="ocp-section__header">
            <div>
              <h2 class="ocp-section__title">Autonomy Posture</h2>
              <p class="ocp-section__subtitle">Current offload policy posture, decision logic, and the last autonomy pass.</p>
            </div>
          </div>
          <div class="ocp-segmented">
            <div class="ocp-segmented__indicator" id="autonomy-indicator"></div>
            <button class="ocp-segmented__button" type="button" id="autonomy-manual" tabindex="-1">Manual</button>
            <button class="ocp-segmented__button" type="button" id="autonomy-assisted" tabindex="-1">Assisted</button>
            <button class="ocp-segmented__button" type="button" id="autonomy-autonomous" tabindex="-1">Autonomous</button>
          </div>
          <p class="ocp-autonomy-copy" id="autonomy-description"></p>
          <div class="ocp-toolbar">
            <button class="ocp-button ocp-button--secondary" data-action="run-autonomy" type="button">Run Autonomy Pass</button>
            <span class="ocp-status-note" id="autonomy-last-run"></span>
          </div>
          <div class="ocp-error" data-error-for="autonomy"></div>
        </section>

        <section class="ocp-section" data-section>
          <div class="ocp-section__header">
            <div>
              <h2 class="ocp-section__title">Offload Memory</h2>
              <p class="ocp-section__subtitle">Saved helper preferences currently steering offload decisions on this node.</p>
            </div>
          </div>
          <div class="ocp-offload-table" id="offload-grid"></div>
          <div class="ocp-error" data-error-for="offload"></div>
        </section>
      </div>

      <div class="ocp-column">
        <section class="ocp-section" data-section>
          <div class="ocp-section__header">
            <div>
              <h2 class="ocp-section__title">Helper Fleet</h2>
              <p class="ocp-section__subtitle">Trusted devices available to absorb pressure across the sovereign mesh.</p>
            </div>
            <span class="ocp-pill ocp-pill--violet ocp-mono" id="helper-count-pill">0 helpers</span>
          </div>
          <div class="ocp-helper-grid" id="helper-grid"></div>
          <div class="ocp-error" data-error-for="fleet"></div>
        </section>

        <section class="ocp-section" data-section>
          <div class="ocp-section__header">
            <div>
              <h2 class="ocp-section__title">Peers</h2>
              <p class="ocp-section__subtitle">Compact peer registry reference with identity, endpoints, and current mesh posture.</p>
            </div>
          </div>
          <div class="ocp-peer-grid" id="peer-grid"></div>
          <div class="ocp-error" data-error-for="peers"></div>
        </section>
      </div>
    </main>

    <section class="ocp-section" data-section id="approvals-section">
      <div class="ocp-section__header">
        <div>
          <h2 class="ocp-section__title" id="approvals-title">Approvals Queue</h2>
          <p class="ocp-section__subtitle">Pending approvals become the most visible manual checkpoints in the cockpit.</p>
        </div>
        <span class="ocp-pill ocp-pill--warn ocp-mono" id="approval-count-pill">0 pending</span>
      </div>
      <div class="ocp-card-grid" id="approval-grid"></div>
      <div class="ocp-error" data-error-for="approvals"></div>
    </section>

    <section class="ocp-section" data-section>
      <div class="ocp-section__header">
        <div>
          <h2 class="ocp-section__title">Notifications</h2>
          <p class="ocp-section__subtitle">Recent operator signals, with unread items remaining bright and older alerts dimming into the background.</p>
        </div>
      </div>
      <div class="ocp-notification-list" id="notification-grid"></div>
      <div class="ocp-error" data-error-for="notifications"></div>
    </section>

    <section class="ocp-section ocp-developer" data-section>
      <div class="ocp-section__header">
        <div>
          <h2 class="ocp-section__title">Raw JSON Surfaces</h2>
          <p class="ocp-section__subtitle">Protocol-native links stay available under a quiet developer disclosure.</p>
        </div>
      </div>
      <details>
        <summary>Developer surfaces</summary>
        <div class="ocp-surface-links" id="json-surfaces"></div>
      </details>
    </section>
  </div>

  <div class="ocp-inspect-overlay" id="inspect-overlay" aria-hidden="true">
    <button class="ocp-inspect-backdrop" id="inspect-backdrop" type="button" aria-label="Close operator inspect"></button>
    <aside class="ocp-inspect-panel" role="dialog" aria-modal="true" aria-labelledby="inspect-title">
      <div class="ocp-inspect-panel__head">
        <div>
          <span class="ocp-version">Operator Inspect</span>
          <h2 class="ocp-inspect-panel__title" id="inspect-title">Select a live surface</h2>
          <p class="ocp-inspect-panel__subtitle" id="inspect-subtitle">Mission, job, and cooperative-task drill-down stays inside the cockpit now.</p>
        </div>
        <button class="ocp-inspect-close" id="inspect-close" type="button">Close</button>
      </div>
      <div class="ocp-inspect-panel__body" id="inspect-body">
        <div class="ocp-empty">Choose a mission, queue job, or cooperative task to inspect its state, continuity, lineage, and raw JSON without leaving the deck.</div>
      </div>
    </aside>
  </div>

  <script>
    const OCP_CONTROL_BOOTSTRAP = __OCP_CONTROL_BOOTSTRAP__;
    const JSON_SURFACES = [
      { label: "/mesh/manifest", href: "/mesh/manifest" },
      { label: "/mesh/control/stream", href: "/mesh/control/stream" },
      { label: "/mesh/peers", href: "/mesh/peers" },
      { label: "/mesh/discovery/candidates", href: "/mesh/discovery/candidates" },
      { label: "/mesh/connectivity/diagnostics", href: "/mesh/connectivity/diagnostics" },
      { label: "/mesh/queue", href: "/mesh/queue" },
      { label: "/mesh/notifications", href: "/mesh/notifications" },
      { label: "/mesh/approvals", href: "/mesh/approvals" },
      { label: "/mesh/peers/connect", href: "/mesh/peers/connect" },
      { label: "/mesh/peers/connect-all", href: "/mesh/peers/connect-all" },
      { label: "/mesh/missions", href: "/mesh/missions" },
      { label: "/mesh/missions/test-launch", href: "/mesh/missions/test-launch" },
      { label: "/mesh/cooperative-tasks", href: "/mesh/cooperative-tasks" },
      { label: "/mesh/queue/metrics", href: "/mesh/queue/metrics" },
      { label: "/mesh/pressure", href: "/mesh/pressure" },
      { label: "/mesh/helpers", href: "/mesh/helpers" },
      { label: "/mesh/helpers/autonomy", href: "/mesh/helpers/autonomy" },
      { label: "/mesh/helpers/preferences", href: "/mesh/helpers/preferences" }
    ];
    const app = {
      state: OCP_CONTROL_BOOTSTRAP,
      meshScene: null,
      refreshTimer: null,
      activityMemory: {},
      inspect: {
        surface: "",
        id: "",
        href: "",
        title: ""
      },
      stream: {
        source: null,
        cursor: Number((((OCP_CONTROL_BOOTSTRAP || {}).control_stream || {}).cursor) || 0),
        reconnectTimer: null
      },
      gauges: {
        hero: { current: 0, target: 0, canvas: null, type: "hero" },
        main: { current: 0, target: 0, canvas: null, type: "main" }
      }
    };

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

    function setStatus(text) {
      const target = document.getElementById("status-note");
      if (target) {
        target.textContent = text;
      }
    }

    function showError(section, message) {
      document.querySelectorAll('[data-error-for="' + section + '"]').forEach(function (node) {
        node.textContent = message;
        node.classList.add("is-visible");
      });
    }

    function clearError(section) {
      document.querySelectorAll('[data-error-for="' + section + '"]').forEach(function (node) {
        node.textContent = "";
        node.classList.remove("is-visible");
      });
    }

    async function fetchJson(url, options) {
      const response = await fetch(url, options);
      if (!response.ok) {
        let message = response.status + " " + response.statusText;
        try {
          const payload = await response.json();
          if (payload && payload.error) {
            message = payload.error;
          }
        } catch (error) {
        }
        throw new Error(message);
      }
      return response.json();
    }

    function utcValue(input) {
      if (!input) {
        return null;
      }
      const normalized = String(input).trim().replace(" ", "T");
      const withZone = /Z$|[+-]\\d\\d:\\d\\d$/.test(normalized) ? normalized : normalized + "Z";
      const value = new Date(withZone);
      return Number.isNaN(value.getTime()) ? null : value;
    }

    function relativeTime(input) {
      const value = utcValue(input);
      if (!value) {
        return "just now";
      }
      const delta = Math.round((Date.now() - value.getTime()) / 1000);
      const future = delta < 0;
      const abs = Math.abs(delta);
      if (abs < 5) {
        return "just now";
      }
      if (abs < 60) {
        return (future ? "in " : "") + abs + "s" + (future ? "" : " ago");
      }
      if (abs < 3600) {
        const minutes = Math.floor(abs / 60);
        return (future ? "in " : "") + minutes + "m" + (future ? "" : " ago");
      }
      if (abs < 86400) {
        const hours = Math.floor(abs / 3600);
        return (future ? "in " : "") + hours + "h" + (future ? "" : " ago");
      }
      const days = Math.floor(abs / 86400);
      return (future ? "in " : "") + days + "d" + (future ? "" : " ago");
    }

    function truncateId(value, limit) {
      const text = String(value || "");
      if (text.length <= limit) {
        return text;
      }
      return text.slice(0, Math.max(4, limit - 3)) + "...";
    }

    function capitalize(value) {
      const text = String(value || "").replace(/[_-]+/g, " ");
      return text ? text.charAt(0).toUpperCase() + text.slice(1) : "";
    }

    function compactText(value, limit) {
      const text = String(value || "");
      if (text.length <= limit) {
        return text;
      }
      return text.slice(0, Math.max(8, limit - 3)) + "...";
    }

    function animateCount(node, nextValue) {
      if (!node) {
        return;
      }
      const value = String(nextValue);
      if (node.textContent !== value) {
        node.textContent = value;
        node.classList.remove("ocp-scale-pop");
        void node.offsetWidth;
        node.classList.add("ocp-scale-pop");
      } else {
        node.textContent = value;
      }
    }

    function computePressurePercent(pressure) {
      const current = pressure || {};
      const totalSlots = Math.max(0, Number(current.total_slots || 0));
      const queued = Math.max(0, Number(current.queued || 0));
      const inflight = Math.max(0, Number(current.inflight || 0));
      const backlogRatio = current.backlog_ratio == null ? null : Number(current.backlog_ratio);
      let percent = 0;
      if (totalSlots > 0) {
        percent = ((queued + inflight) / totalSlots) * 100;
      } else if (queued > 0 || inflight > 0) {
        percent = 88;
      }
      if (backlogRatio != null) {
        percent = Math.max(percent, backlogRatio * 45);
      }
      const pressureState = String(current.pressure || "idle").toLowerCase();
      if (pressureState === "elevated") {
        percent = Math.max(percent, 58);
      } else if (pressureState === "saturated") {
        percent = Math.max(percent, 86);
      } else if (pressureState === "nominal") {
        percent = Math.max(percent, 22);
      }
      return Math.max(0, Math.min(100, Math.round(percent)));
    }

    function hexToRgb(hex) {
      const token = String(hex || "").replace("#", "");
      return {
        r: parseInt(token.slice(0, 2), 16),
        g: parseInt(token.slice(2, 4), 16),
        b: parseInt(token.slice(4, 6), 16)
      };
    }

    function interpolateColor(leftHex, rightHex, amount) {
      const left = hexToRgb(leftHex);
      const right = hexToRgb(rightHex);
      const mix = Math.max(0, Math.min(1, amount));
      const r = Math.round(left.r + (right.r - left.r) * mix);
      const g = Math.round(left.g + (right.g - left.g) * mix);
      const b = Math.round(left.b + (right.b - left.b) * mix);
      return "rgb(" + r + ", " + g + ", " + b + ")";
    }

    function gaugeColor(percent) {
      if (percent <= 60) {
        return interpolateColor("#00D4FF", "#FF9500", percent / 60);
      }
      if (percent <= 85) {
        return interpolateColor("#FF9500", "#FF4757", (percent - 60) / 25);
      }
      return "#FF4757";
    }

    function pressureTone(percent) {
      if (percent > 70) {
        return "danger";
      }
      if (percent >= 40) {
        return "warn";
      }
      return "safe";
    }

    function meshQuality(peersResponse) {
      const peers = (peersResponse && peersResponse.peers) || [];
      const health = (peersResponse && peersResponse.health) || {};
      const connected = Number(health.connected || peers.filter(function (peer) { return peer.status === "connected"; }).length);
      const degraded = Number(health.degraded || peers.filter(function (peer) { return peer.status === "degraded"; }).length);
      if (!peers.length || connected <= 0) {
        return "isolated";
      }
      if (degraded > 0 || connected < peers.length) {
        return "degraded";
      }
      return "strong";
    }

    function autonomyPosture(policy) {
      const mode = String((policy && policy.mode) || "manual").toLowerCase();
      if (mode === "auto" || mode === "automatic" || mode === "autonomous") {
        return { label: "AUTONOMOUS", tone: "autonomous", index: 2 };
      }
      if (mode === "approval" || mode === "assisted") {
        return { label: "ASSISTED", tone: "assisted", index: 1 };
      }
      return { label: "MANUAL", tone: "manual", index: 0 };
    }

    function pressureStateLabel(pressure) {
      const current = String((pressure && pressure.pressure) || "idle").toLowerCase();
      return current === "nominal" ? "nominal" : current;
    }

    function taskStatus(task) {
      const state = String((task && task.state) || "pending").toLowerCase();
      if (state === "completed") {
        return { label: "DONE", tone: "success" };
      }
      if (state === "active") {
        return { label: "RUNNING", tone: "cyan" };
      }
      if (state === "attention") {
        return { label: "FAILED", tone: "danger" };
      }
      return { label: "PENDING", tone: "warn" };
    }

    function taskGlyph() {
      return '<svg class="ocp-task-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4" aria-hidden="true"><path d="M2 5h5V2H2v3Zm7 9h5v-3H9v3Zm0-5h5V2H9v7ZM2 14h5V7H2v7Z"/></svg>';
    }

    function latencyValue(peer) {
      const heartbeat = (peer && peer.heartbeat) || {};
      const metadata = (peer && peer.metadata) || {};
      const candidates = [
        heartbeat.latency_ms,
        heartbeat.rtt_ms,
        metadata.latency_ms,
        metadata.rtt_ms,
        metadata.last_latency_ms
      ];
      for (let index = 0; index < candidates.length; index += 1) {
        const numeric = Number(candidates[index]);
        if (Number.isFinite(numeric) && numeric >= 0) {
          return Math.round(numeric);
        }
      }
      return null;
    }

    function latencyColor(latency) {
      if (latency == null) {
        return "var(--ocp-text-secondary)";
      }
      if (latency < 50) {
        return "var(--ocp-green)";
      }
      if (latency < 150) {
        return "var(--ocp-amber)";
      }
      return "var(--ocp-coral)";
    }

    function jobActionSpecs(queueMessage, job) {
      const recovery = (job && job.recovery) || {};
      const jobStatus = String((job && job.status) || "").toLowerCase();
      const queueStatus = String((queueMessage && queueMessage.status) || "").toLowerCase();
      const resumable = Boolean(recovery.resumable) || Boolean(job && job.latest_checkpoint_ref && Object.keys(job.latest_checkpoint_ref).length);
      const actions = [];
      if (resumable && ["checkpointed", "retry_wait", "failed"].includes(jobStatus)) {
        actions.push({ action: "resume", label: "Resume Latest", tone: "ocp-button--cyan" });
      }
      if (!["completed", "rejected"].includes(jobStatus)) {
        actions.push({ action: "restart", label: "Restart Fresh", tone: "ocp-button--amber" });
      }
      if (["dead_letter", "cancelled"].includes(queueStatus) && jobStatus !== "checkpointed") {
        actions.push({ action: "replay", label: "Replay Queue", tone: "ocp-button--secondary" });
      }
      if (["queued", "inflight"].includes(queueStatus) && !["completed", "failed", "rejected", "cancelled"].includes(jobStatus)) {
        actions.push({ action: "cancel", label: "Cancel Job", tone: "ocp-button--coral" });
      }
      return actions;
    }

    function updateSectionReveals() {
      document.querySelectorAll("[data-section]").forEach(function (section, index) {
        section.style.setProperty("--ocp-delay", String(index * 60) + "ms");
        section.classList.add("is-visible");
      });
    }

    function renderHero(state) {
      const policy = (state.autonomy && state.autonomy.policy) || {};
      const peersResponse = state.peers || {};
      const peers = peersResponse.peers || [];
      const quality = meshQuality(peersResponse);
      const pressurePercent = computePressurePercent(state.pressure);
      const posture = autonomyPosture(policy);
      document.getElementById("hero-node-name").textContent = state.display_name || state.node_id || "Node";
      document.getElementById("hero-role-pill").textContent = String(state.role_label || "Sovereign Node").toUpperCase();
      document.getElementById("hero-version").textContent = state.version || "OCP runtime";
      const peerCount = document.getElementById("hero-peer-count");
      animateCount(peerCount, peers.length);
      peerCount.classList.toggle("is-zero", peers.length === 0);
      const dots = document.getElementById("hero-live-dots");
      dots.classList.toggle("is-live", peers.length > 0);
      document.getElementById("hero-quality").textContent = quality;
      document.getElementById("hero-pressure-value").textContent = pressurePercent + "%";
      const pill = document.getElementById("hero-autonomy-pill");
      pill.textContent = posture.label;
      pill.className = "ocp-pill ocp-pill--" + posture.tone;
      document.getElementById("hero-observed-at").textContent = relativeTime((state.pressure && state.pressure.observed_at) || "");
      clearError("hero");
      setGaugeTarget("hero", pressurePercent);
    }

    function renderCenterpiece(state) {
      const pressure = state.pressure || {};
      const autonomy = state.autonomy || {};
      const queueMetrics = state.queue_metrics || {};
      const counts = queueMetrics.counts || {};
      const workers = queueMetrics.workers || {};
      const percent = computePressurePercent(pressure);
      const summary = ((pressure.reasons || []).join(", ") || "No pressure signals.").replace(/_/g, " ");
      document.getElementById("pressure-summary").textContent = capitalize(pressureStateLabel(pressure)) + " pressure. " + summary;
      document.getElementById("pressure-gauge-value").textContent = percent + "%";
      const eligible = (autonomy.eligible_candidates || []).slice(0, 3);
      const offloadPill = document.getElementById("offload-state-pill");
      const hasEligible = eligible.length > 0;
      offloadPill.textContent = hasEligible ? "OFFLOAD ELIGIBLE" : "NO OFFLOAD";
      offloadPill.className = "ocp-pill " + (hasEligible ? "ocp-pill--eligible" : "ocp-pill--blocked");
      const offloadSummary = document.getElementById("offload-summary");
      if (hasEligible) {
        offloadSummary.textContent = "Eligible helpers can absorb " + eligible.length + " near-term offload slot(s) with policy mode " + String(((autonomy.policy || {}).mode) || "manual") + ".";
      } else {
        offloadSummary.textContent = ((autonomy.reasons || []).join(", ") || "No helper candidates are currently eligible.").replace(/_/g, " ");
      }
      document.getElementById("offload-candidates").innerHTML = hasEligible
        ? eligible.map(function (candidate) {
            const compute = candidate.compute_profile || {};
            const destination = candidate.display_name || candidate.peer_id || "helper";
            const jobHint = compute.gpu_capable ? "GPU capacity ready" : "CPU overflow ready";
            return '<div class="ocp-readiness__item">' +
              '<div><strong>' + escapeHtml(destination) + '</strong><br><span>' + escapeHtml(jobHint + " • trust " + String(candidate.trust_tier || "trusted")) + '</span></div>' +
              '<span class="ocp-mono">' + escapeHtml("score " + String(candidate.score || 0)) + '</span>' +
            '</div>';
          }).join("")
        : '<div class="ocp-empty">No helper recommendations are currently available from the autonomy evaluator.</div>';
      const queued = Number(counts.queued || pressure.queued || 0);
      const inflight = Number(counts.inflight || pressure.inflight || 0);
      const recoveryCount = Object.values(state.jobs || {}).filter(function (job) {
        const recovery = job.recovery || {};
        return Boolean(recovery.resumable) || Boolean(job.latest_checkpoint_ref && Object.keys(job.latest_checkpoint_ref).length);
      }).length;
      document.getElementById("centerpiece-metrics").innerHTML = [
        { label: "Queue Depth", value: queued + inflight },
        { label: "Worker Slots", value: String(workers.available_slots || pressure.available_slots || 0) + "/" + String(workers.total_slots || pressure.total_slots || 0) },
        { label: "Recovery Ready", value: recoveryCount },
        { label: "Redeliveries", value: queueMetrics.redelivery_count || 0 }
      ].map(function (item) {
        return '<div class="ocp-metric-pill"><span class="ocp-metric-pill__label">' + escapeHtml(item.label) + '</span><span class="ocp-metric-pill__value ocp-mono">' + escapeHtml(item.value) + '</span></div>';
      }).join("");
      clearError("centerpiece");
      setGaugeTarget("main", percent);
    }

    function toneClass(level) {
      if (level === "danger") {
        return "is-danger";
      }
      if (level === "warn") {
        return "is-warn";
      }
      if (level === "safe" || level === "success") {
        return "is-safe";
      }
      if (level === "cyan") {
        return "is-cyan";
      }
      return "is-violet";
    }

    function pillClass(level) {
      if (level === "danger") {
        return "ocp-pill--blocked";
      }
      if (level === "warn") {
        return "ocp-pill--warn";
      }
      if (level === "safe" || level === "success") {
        return "ocp-pill--eligible";
      }
      if (level === "cyan") {
        return "ocp-pill--role";
      }
      return "ocp-pill--violet";
    }

    function recordTimestamp(record) {
      if (!record) {
        return null;
      }
      const heartbeat = record.heartbeat || {};
      return record.updated_at
        || record.created_at
        || record.generated_at
        || record.requested_at
        || record.observed_at
        || heartbeat.observed_at
        || heartbeat.last_seen_at
        || heartbeat.last_seen
        || null;
    }

    function recordTimeMs(record) {
      const value = utcValue(recordTimestamp(record));
      return value ? value.getTime() : 0;
    }

    function approvalTone(status, severity) {
      const currentStatus = String(status || "pending").toLowerCase();
      const currentSeverity = String(severity || "normal").toLowerCase();
      if (currentStatus === "pending" && ["critical", "high"].includes(currentSeverity)) {
        return "danger";
      }
      if (currentStatus === "pending") {
        return "warn";
      }
      if (currentStatus === "approved") {
        return "safe";
      }
      return "violet";
    }

    function notificationTone(priority, status) {
      const currentPriority = String(priority || "normal").toLowerCase();
      const currentStatus = String(status || "unread").toLowerCase();
      if (currentStatus !== "acked" && currentPriority === "critical") {
        return "danger";
      }
      if (currentStatus !== "acked" && currentPriority === "high") {
        return "warn";
      }
      if (currentStatus === "acked") {
        return "violet";
      }
      return "cyan";
    }

    function jobTone(job, queueMessage) {
      const jobStatus = String((job && job.status) || "").toLowerCase();
      const queueStatus = String((queueMessage && queueMessage.status) || "").toLowerCase();
      if (["failed", "rejected", "cancelled"].includes(jobStatus)) {
        return "danger";
      }
      if (["checkpointed", "retry_wait"].includes(jobStatus) || ["dead_letter"].includes(queueStatus)) {
        return "warn";
      }
      if (["completed"].includes(jobStatus)) {
        return "safe";
      }
      if (["running", "inflight"].includes(jobStatus) || ["inflight"].includes(queueStatus)) {
        return "cyan";
      }
      return "violet";
    }

    function cooperativeTone(task) {
      const state = String((task && task.state) || "pending").toLowerCase();
      if (state === "attention" || state === "failed") {
        return "danger";
      }
      if (state === "pending") {
        return "warn";
      }
      if (state === "completed") {
        return "safe";
      }
      return "cyan";
    }

    function collectPulseItems(state) {
      const items = [];
      const missionMap = {};
      const missions = (state.missions && state.missions.missions) || [];
      missions.forEach(function (mission) {
        missionMap[String(mission.id || "")] = mission;
        const summary = mission.summary || {};
        const continuity = mission.continuity || {};
        const status = String(mission.status || "planned").toLowerCase();
        const level = status === "completed"
          ? "safe"
          : (["failed", "cancelled"].includes(status) ? "danger" : (["checkpointed", "waiting"].includes(status) ? "warn" : "cyan"));
        items.push({
          key: "mission:" + String(mission.id || ""),
          surface: "Mission",
          level: level,
          title: mission.title || mission.id || "Mission",
          detail: mission.intent || "Mission intent not provided.",
          meta: [
            capitalize(status || "planned"),
            String(summary.job_count || 0) + " jobs",
            String(summary.cooperative_task_count || 0) + " coop",
            continuity.checkpoint_ready ? "checkpoint ready" : "",
            mission.target_strategy ? String(mission.target_strategy).replace(/_/g, " ") : ""
          ].filter(Boolean),
          href: "/mesh/missions/" + encodeURIComponent(mission.id || ""),
          inspect_surface: "mission",
          inspect_id: String(mission.id || ""),
          updated_at: recordTimestamp(mission),
          time_ms: recordTimeMs(mission),
          signature: JSON.stringify([
            mission.status,
            continuity.checkpoint_ready,
            continuity.resumable,
            mission.latest_checkpoint_ref && mission.latest_checkpoint_ref.id,
            mission.result_bundle_ref && mission.result_bundle_ref.id,
            mission.updated_at
          ])
        });
      });

      const queueMessages = (state.queue && state.queue.messages) || [];
      const jobs = state.jobs || {};
      queueMessages.forEach(function (message) {
        const job = jobs[message.job_id];
        if (!job) {
          return;
        }
        const recovery = job.recovery || {};
        const missionMeta = job.mission || {};
        const mission = missionMap[String(missionMeta.mission_id || "")] || null;
        items.push({
          key: "job:" + String(job.id || message.job_id || ""),
          surface: "Queue",
          level: jobTone(job, message),
          title: job.kind || job.id || "Job",
          detail: recovery.recovery_hint || message.last_error || "Queue-backed execution is moving through the runtime.",
          meta: [
            String(job.status || "queued"),
            String(message.status || "queued"),
            String(message.delivery_attempts || 0) + " deliveries",
            mission ? "mission " + (mission.title || mission.id || "") : ""
          ].filter(Boolean),
          href: "/mesh/jobs/" + encodeURIComponent(job.id || message.job_id || ""),
          inspect_surface: "job",
          inspect_id: String(job.id || message.job_id || ""),
          updated_at: recordTimestamp(job) || recordTimestamp(message),
          time_ms: Math.max(recordTimeMs(job), recordTimeMs(message)),
          signature: JSON.stringify([
            job.status,
            message.status,
            message.delivery_attempts,
            recovery.recovery_hint,
            job.updated_at,
            message.updated_at
          ])
        });
      });

      const tasks = (state.cooperative_tasks && state.cooperative_tasks.tasks) || [];
      tasks.forEach(function (task) {
        const summary = task.summary || {};
        const counts = summary.counts || {};
        const shards = Math.max(1, Number(task.shard_count || 0));
        const completed = Math.max(0, Number(counts.completed || 0));
        const firstChild = (task.children || []).find(function (child) { return child && child.job; }) || {};
        const placement = firstChild.placement || {};
        const helperName = placement.target_peer_id || (firstChild.job && firstChild.job.target) || "local";
        items.push({
          key: "coop:" + String(task.id || task.name || ""),
          surface: "Coop",
          level: cooperativeTone(task),
          title: task.name || task.id || "Cooperative task",
          detail: "Distributed shard group progressing across the mesh.",
          meta: [
            capitalize(task.state || "pending"),
            completed + "/" + shards + " shards",
            "helper " + truncateId(helperName, 18)
          ],
          href: "/mesh/cooperative-tasks/" + encodeURIComponent(task.id || ""),
          inspect_surface: "cooperative-task",
          inspect_id: String(task.id || ""),
          updated_at: recordTimestamp(task),
          time_ms: recordTimeMs(task),
          signature: JSON.stringify([task.state, counts.completed, shards, task.updated_at])
        });
      });

      const approvals = (state.approvals && state.approvals.approvals) || [];
      approvals.forEach(function (approval) {
        items.push({
          key: "approval:" + String(approval.id || ""),
          surface: "Approval",
          level: approvalTone(approval.status, approval.severity),
          title: approval.title || approval.action_type || "Approval request",
          detail: approval.summary || "Manual decision required before work continues.",
          meta: [
            String(approval.status || "pending"),
            String(approval.severity || "normal"),
            approval.target_peer_id ? "target " + approval.target_peer_id : ""
          ].filter(Boolean),
          href: "/mesh/approvals?target_peer_id=" + encodeURIComponent(approval.target_peer_id || ""),
          updated_at: recordTimestamp(approval),
          time_ms: recordTimeMs(approval),
          signature: JSON.stringify([approval.status, approval.severity, approval.updated_at])
        });
      });

      const notifications = (state.notifications && state.notifications.notifications) || [];
      notifications.forEach(function (notification) {
        items.push({
          key: "notification:" + String(notification.id || ""),
          surface: "Notification",
          level: notificationTone(notification.priority, notification.status),
          title: notification.title || notification.notification_type || "Notification",
          detail: notification.body || "Operator-facing signal published to the cockpit.",
          meta: [
            String(notification.priority || "normal"),
            String(notification.status || "unread"),
            notification.notification_type ? String(notification.notification_type).replace(/_/g, " ") : ""
          ].filter(Boolean),
          href: "/mesh/notifications?target_peer_id=" + encodeURIComponent(notification.target_peer_id || state.node_id || ""),
          updated_at: recordTimestamp(notification),
          time_ms: recordTimeMs(notification),
          signature: JSON.stringify([notification.priority, notification.status, notification.updated_at])
        });
      });

      const helpers = (state.helpers && state.helpers.helpers) || [];
      helpers.forEach(function (helper) {
        const helperState = String(helper.state || "idle").toLowerCase();
        if (!["enlisted", "draining", "retiring"].includes(helperState)) {
          return;
        }
        items.push({
          key: "helper:" + String(helper.peer_id || ""),
          surface: "Helper",
          level: helperState === "draining" ? "warn" : "cyan",
          title: helper.display_name || helper.peer_id || "Helper",
          detail: "Helper lifecycle is currently " + helperState + ".",
          meta: [
            capitalize(helperState),
            helper.trust_tier ? String(helper.trust_tier) : "",
            helper.compute_profile && helper.compute_profile.gpu_capable ? "GPU" : "CPU"
          ].filter(Boolean),
          href: "/mesh/helpers?peer_id=" + encodeURIComponent(helper.peer_id || ""),
          updated_at: recordTimestamp(helper),
          time_ms: recordTimeMs(helper),
          signature: JSON.stringify([helper.state, helper.updated_at])
        });
      });

      const peers = (state.peers && state.peers.peers) || [];
      peers.forEach(function (peer) {
        const status = String(peer.status || "").toLowerCase();
        if (!["degraded", "disconnected", "offline"].includes(status)) {
          return;
        }
        items.push({
          key: "peer:" + String(peer.peer_id || ""),
          surface: "Peer",
          level: status === "degraded" ? "warn" : "danger",
          title: peer.display_name || peer.peer_id || "Peer",
          detail: "Mesh peer status is " + status + ".",
          meta: [
            capitalize(status),
            peer.endpoint ? peer.endpoint : ""
          ].filter(Boolean),
          href: "/mesh/peers",
          updated_at: recordTimestamp(peer),
          time_ms: recordTimeMs(peer),
          signature: JSON.stringify([peer.status, peer.updated_at, peer.endpoint])
        });
      });

      const levelRank = { danger: 0, warn: 1, cyan: 2, safe: 3, success: 3, violet: 4 };
      items.sort(function (left, right) {
        const leftRank = levelRank[left.level] == null ? 5 : levelRank[left.level];
        const rightRank = levelRank[right.level] == null ? 5 : levelRank[right.level];
        if (leftRank !== rightRank) {
          return leftRank - rightRank;
        }
        return Number(right.time_ms || 0) - Number(left.time_ms || 0);
      });
      return items.slice(0, 12);
    }

    function renderPulse(state) {
      const pressure = state.pressure || {};
      const peers = (state.peers && state.peers.peers) || [];
      const missions = (state.missions && state.missions.missions) || [];
      const notifications = (state.notifications && state.notifications.notifications) || [];
      const approvals = (state.approvals && state.approvals.approvals) || [];
      const helpers = (state.helpers && state.helpers.helpers) || [];
      const tasks = (state.cooperative_tasks && state.cooperative_tasks.tasks) || [];
      const queueMessages = (state.queue && state.queue.messages) || [];
      const jobs = state.jobs || {};
      const workersResponse = state.workers || {};
      const workerList = workersResponse.workers || [];
      const queueMetrics = state.queue_metrics || {};
      const queueCounts = queueMetrics.counts || {};
      const activeMissionCount = missions.filter(function (mission) {
        return !["completed", "cancelled"].includes(String(mission.status || "").toLowerCase());
      }).length;
      const recoveryMissionCount = missions.filter(function (mission) {
        return ["checkpointed", "failed", "waiting"].includes(String(mission.status || "").toLowerCase());
      }).length;
      const recoveryReadyJobs = Object.values(jobs).filter(function (job) {
        const recovery = job.recovery || {};
        return Boolean(recovery.resumable) || Boolean(job.latest_checkpoint_ref && Object.keys(job.latest_checkpoint_ref).length);
      }).length;
      const pendingApprovals = approvals.filter(function (approval) {
        return String(approval.status || "pending").toLowerCase() === "pending";
      }).length;
      const unreadNotifications = notifications.filter(function (notification) {
        return !["acked", "resolved"].includes(String(notification.status || "unread").toLowerCase());
      }).length;
      const criticalNotifications = notifications.filter(function (notification) {
        return ["critical", "high"].includes(String(notification.priority || "normal").toLowerCase())
          && !["acked", "resolved"].includes(String(notification.status || "unread").toLowerCase());
      }).length;
      const readyHelpers = ((state.autonomy && state.autonomy.eligible_candidates) || []).length;
      const enlistedHelpers = helpers.filter(function (helper) {
        return ["enlisted", "draining"].includes(String(helper.state || "").toLowerCase());
      }).length;
      const connectedPeers = peers.filter(function (peer) {
        return String(peer.status || "").toLowerCase() === "connected";
      }).length;
      const queued = Number(queueCounts.queued || pressure.queued || 0);
      const inflight = Number(queueCounts.inflight || pressure.inflight || 0);
      const totalTasks = tasks.length;
      const completedTasks = tasks.filter(function (task) {
        return String(task.state || "").toLowerCase() === "completed";
      }).length;
      const activeWorkers = workerList.filter(function (worker) {
        return !["offline", "retired"].includes(String(worker.status || "").toLowerCase());
      }).length;

      const summaryCards = [
        {
          label: "Mission Layer",
          value: activeMissionCount,
          detail: recoveryMissionCount > 0
            ? String(recoveryMissionCount) + " mission(s) need continuity attention."
            : "Durable mission intent is steady.",
          level: recoveryMissionCount > 0 ? "warn" : "cyan"
        },
        {
          label: "Queue + Recovery",
          value: queued + inflight,
          detail: String(recoveryReadyJobs) + " job(s) are recovery-ready across queued work.",
          level: recoveryReadyJobs > 0 ? "warn" : "violet"
        },
        {
          label: "Human Attention",
          value: pendingApprovals + unreadNotifications,
          detail: String(pendingApprovals) + " approvals pending and " + String(unreadNotifications) + " live alerts.",
          level: pendingApprovals > 0 || criticalNotifications > 0 ? "danger" : (unreadNotifications > 0 ? "warn" : "safe")
        },
        {
          label: "Helper Fleet",
          value: readyHelpers || enlistedHelpers,
          detail: String(readyHelpers) + " ready now, " + String(enlistedHelpers) + " already engaged.",
          level: readyHelpers > 0 ? "safe" : (enlistedHelpers > 0 ? "cyan" : "violet")
        },
        {
          label: "Cooperative Tasks",
          value: totalTasks,
          detail: totalTasks > 0
            ? String(completedTasks) + " completed, " + String(totalTasks - completedTasks) + " still moving."
            : "No shard groups are currently active.",
          level: totalTasks > 0 ? "cyan" : "violet"
        },
        {
          label: "Mesh Reach",
          value: connectedPeers,
          detail: String(activeWorkers) + " workers visible with mesh quality " + meshQuality(state.peers) + ".",
          level: connectedPeers > 0 ? "safe" : "danger"
        }
      ];
      document.getElementById("pulse-summary-grid").innerHTML = summaryCards.map(function (card) {
        return '<article class="ocp-panorama-card ' + toneClass(card.level) + '">' +
          '<span class="ocp-panorama-card__eyebrow">' + escapeHtml(card.label) + '</span>' +
          '<span class="ocp-panorama-card__value ocp-mono">' + escapeHtml(card.value) + '</span>' +
          '<span class="ocp-panorama-card__detail">' + escapeHtml(card.detail) + '</span>' +
        '</article>';
      }).join("");

      const headlines = [];
      if (recoveryMissionCount > 0) {
        headlines.push({
          label: "Continuity",
          body: String(recoveryMissionCount) + " mission(s) are paused, checkpointed, failed, or waiting and may need resume or restart decisions."
        });
      }
      if (pendingApprovals > 0 || criticalNotifications > 0) {
        headlines.push({
          label: "Operator Focus",
          body: String(pendingApprovals) + " approval(s) and " + String(criticalNotifications) + " high-priority alert(s) are currently competing for attention."
        });
      }
      if (queued + inflight > 0) {
        headlines.push({
          label: "Execution Load",
          body: String(queued) + " queued and " + String(inflight) + " inflight jobs are pushing against " + String((queueMetrics.workers || {}).total_slots || pressure.total_slots || 0) + " visible slot(s)."
        });
      }
      if (readyHelpers > 0 || enlistedHelpers > 0) {
        headlines.push({
          label: "Fleet Motion",
          body: String(readyHelpers) + " helper candidate(s) are offload-eligible and " + String(enlistedHelpers) + " helper(s) are already enlisted or draining."
        });
      }
      if (!headlines.length) {
        headlines.push({
          label: "Calm Mesh",
          body: "No urgent blockers are visible right now. The queue, mission layer, and helper fleet are all in a steady posture."
        });
      }
      document.getElementById("pulse-headlines").innerHTML = headlines.slice(0, 4).map(function (headline) {
        return '<article class="ocp-panorama-headline">' +
          '<span class="ocp-panorama-headline__label">' + escapeHtml(headline.label) + '</span>' +
          '<div class="ocp-panorama-headline__body">' + escapeHtml(headline.body) + '</div>' +
        '</article>';
      }).join("");

      const pressurePercent = computePressurePercent(pressure);
      const pulseLevel = pendingApprovals > 0 || criticalNotifications > 0
        ? "danger"
        : (recoveryMissionCount > 0 || pressurePercent >= 58 || queued + inflight > 0 ? "warn" : "safe");
      const pulseLabel = pulseLevel === "danger"
        ? "ATTENTION"
        : (pulseLevel === "warn" ? "ACTIVE" : "STEADY");
      const pulsePill = document.getElementById("pulse-state-pill");
      pulsePill.textContent = pulseLabel;
      pulsePill.className = "ocp-pill " + pillClass(pulseLevel);

      const items = collectPulseItems(state);
      const nextMemory = {};
      document.getElementById("pulse-feed-note").textContent = items.length
        ? String(items.length) + " live signals across missions, queue, helpers, and operator surfaces."
        : "Watching for activity.";
      document.getElementById("pulse-activity-feed").innerHTML = items.length
        ? items.map(function (item) {
            const previousSignature = app.activityMemory[item.key];
            const fresh = previousSignature && previousSignature !== item.signature;
            nextMemory[item.key] = item.signature;
            const tagName = item.inspect_surface ? "button" : "a";
            const hrefAttr = item.inspect_surface ? "" : ' href="' + escapeHtml(item.href || "/control") + '"';
            const inspectAttrs = item.inspect_surface
              ? ' type="button" data-inspect-surface="' + escapeHtml(item.inspect_surface) + '" data-inspect-id="' + escapeHtml(item.inspect_id || "") + '" data-inspect-title="' + escapeHtml(item.title || item.surface || "Inspect") + '" data-inspect-href="' + escapeHtml(item.href || "") + '"'
              : "";
            return '<' + tagName + ' class="ocp-panorama-feed__row ocp-panorama-feed__row--button ' + toneClass(item.level) + (fresh ? ' is-fresh' : '') + '"' + hrefAttr + inspectAttrs + '>' +
              '<span class="ocp-panorama-feed__strip"></span>' +
              '<div class="ocp-panorama-feed__body">' +
                '<div class="ocp-panorama-feed__head">' +
                  '<div class="ocp-panorama-feed__identity">' +
                    '<span class="ocp-surface-tag ' + toneClass(item.level) + '">' + escapeHtml(item.surface) + '</span>' +
                    '<h3 class="ocp-panorama-feed__title">' + escapeHtml(item.title) + '</h3>' +
                  '</div>' +
                  '<span class="ocp-relative">' + escapeHtml(relativeTime(item.updated_at)) + '</span>' +
                '</div>' +
                '<p class="ocp-panorama-feed__detail">' + escapeHtml(item.detail) + '</p>' +
                '<div class="ocp-panorama-feed__meta">' + item.meta.map(function (metaItem) {
                  return '<span>' + escapeHtml(metaItem) + '</span>';
                }).join("") + '</div>' +
              '</div>' +
            '</' + tagName + '>';
          }).join("")
        : '<div class="ocp-empty">No live mesh activity yet. Missions, jobs, notifications, and helpers will light this stream up as soon as they move.</div>';
      app.activityMemory = nextMemory;
    }

    function inspectEndpoint(surface, identifier) {
      if (surface === "mission") {
        return "/mesh/missions/" + encodeURIComponent(identifier);
      }
      if (surface === "job") {
        return "/mesh/jobs/" + encodeURIComponent(identifier);
      }
      if (surface === "cooperative-task") {
        return "/mesh/cooperative-tasks/" + encodeURIComponent(identifier);
      }
      return "";
    }

    function inspectLabel(surface) {
      if (surface === "mission") {
        return "Mission";
      }
      if (surface === "job") {
        return "Queue Job";
      }
      if (surface === "cooperative-task") {
        return "Cooperative Task";
      }
      return "Surface";
    }

    function inspectStatusTone(status) {
      const token = String(status || "").toLowerCase();
      if (["completed", "approved", "connected"].includes(token)) {
        return "ocp-pill--eligible";
      }
      if (["failed", "cancelled", "rejected", "offline", "disconnected", "attention"].includes(token)) {
        return "ocp-pill--blocked";
      }
      if (["checkpointed", "waiting", "pending", "queued", "retry_wait", "degraded"].includes(token)) {
        return "ocp-pill--warn";
      }
      return "ocp-pill--role";
    }

    function renderInspectStats(items) {
      return '<div class="ocp-inspect-grid">' + items.filter(function (item) {
        return item && item.value !== undefined && item.value !== null && String(item.value) !== "";
      }).map(function (item) {
        return '<div class="ocp-inspect-stat">' +
          '<span class="ocp-inspect-stat__label">' + escapeHtml(item.label) + '</span>' +
          '<span class="ocp-inspect-stat__value">' + escapeHtml(item.value) + '</span>' +
        '</div>';
      }).join("") + '</div>';
    }

    function renderInspectList(title, items) {
      const rows = (items || []).filter(Boolean);
      if (!rows.length) {
        return "";
      }
      return '<section class="ocp-inspect-section">' +
        '<h3 class="ocp-inspect-section__title">' + escapeHtml(title) + '</h3>' +
        '<div class="ocp-inspect-list">' + rows.join("") + '</div>' +
      '</section>';
    }

    function renderInspectActionButton(label, surface, identifier, href) {
      if (!surface || !identifier) {
        return "";
      }
      return '<button class="ocp-button ocp-button--inspect" type="button" data-inspect-surface="' + escapeHtml(surface) + '" data-inspect-id="' + escapeHtml(identifier) + '" data-inspect-title="' + escapeHtml(label) + '" data-inspect-href="' + escapeHtml(href || inspectEndpoint(surface, identifier)) + '">' + escapeHtml(label) + '</button>';
    }

    function renderArtifactLink(label, artifactRef) {
      if (!artifactRef || !artifactRef.id) {
        return "";
      }
      return '<a class="ocp-link-button ocp-mono-link" href="/mesh/artifacts/' + escapeHtml(artifactRef.id) + '" target="_blank" rel="noreferrer">' + escapeHtml(label) + '</a>';
    }

    function renderMissionInspect(payload) {
      const summary = payload.summary || {};
      const continuity = payload.continuity || {};
      const lineage = payload.lineage || {};
      const childJobs = payload.child_jobs || [];
      const jobs = lineage.jobs || [];
      const tasks = lineage.cooperative_tasks || [];
      return [
        '<section class="ocp-inspect-section">' +
          '<h3 class="ocp-inspect-section__title">Mission Summary</h3>' +
          '<div class="ocp-inspect-copy">' + escapeHtml(payload.intent || "Mission intent not provided.") + '</div>' +
          '<div class="ocp-toolbar" style="margin-top:12px;">' +
            '<span class="ocp-pill ' + inspectStatusTone(payload.status) + '">' + escapeHtml(String(payload.status || "planned").toUpperCase()) + '</span>' +
            renderArtifactLink("Checkpoint Artifact", payload.latest_checkpoint_ref) +
            renderArtifactLink("Result Bundle", payload.result_bundle_ref) +
            '<a class="ocp-link-button ocp-mono-link" href="/mesh/missions/' + escapeHtml(payload.id || "") + '" target="_blank" rel="noreferrer">Open JSON</a>' +
          '</div>' +
        '</section>',
        '<section class="ocp-inspect-section">' +
          '<h3 class="ocp-inspect-section__title">Continuity + Policy</h3>' +
          renderInspectStats([
            { label: "Priority", value: payload.priority || "normal" },
            { label: "Workload", value: payload.workload_class || "default" },
            { label: "Target Strategy", value: String(payload.target_strategy || "local").replace(/_/g, " ") },
            { label: "Origin Peer", value: payload.origin_peer_id || app.state.node_id || "" },
            { label: "Resumable", value: continuity.resumable ? "yes" : "no" },
            { label: "Checkpoint Ready", value: continuity.checkpoint_ready ? "yes" : "no" }
          ]) +
        '</section>',
        renderInspectList("Child Jobs", childJobs.map(function (job) {
          return '<div class="ocp-inspect-item">' +
            '<div class="ocp-inspect-item__head">' +
              '<span class="ocp-inspect-item__title">' + escapeHtml(job.kind || compactText(job.id || "job", 20)) + '</span>' +
              '<span class="ocp-pill ' + inspectStatusTone(job.status) + '">' + escapeHtml(String(job.status || "queued").toUpperCase()) + '</span>' +
            '</div>' +
            '<div class="ocp-inspect-item__meta">' +
              '<span>' + escapeHtml(compactText(job.id || "", 30)) + '</span>' +
              '<span>' + escapeHtml(job.target || "local") + '</span>' +
              '<span>' + escapeHtml(relativeTime(job.updated_at || job.created_at)) + '</span>' +
            '</div>' +
            '<div class="ocp-toolbar">' +
              renderInspectActionButton("Inspect Job", "job", job.id || "", "/mesh/jobs/" + encodeURIComponent(job.id || "")) +
            '</div>' +
          '</div>';
        })) || "",
        renderInspectList("Cooperative Tasks", tasks.map(function (task) {
          return '<div class="ocp-inspect-item">' +
            '<div class="ocp-inspect-item__head">' +
              '<span class="ocp-inspect-item__title">' + escapeHtml(task.name || compactText(task.id || "task", 22)) + '</span>' +
              '<span class="ocp-pill ' + inspectStatusTone(task.state || task.status) + '">' + escapeHtml(String(task.state || task.status || "active").toUpperCase()) + '</span>' +
            '</div>' +
            '<div class="ocp-inspect-item__meta">' +
              '<span>' + escapeHtml(compactText(task.id || "", 30)) + '</span>' +
              '<span>' + escapeHtml(String(task.shard_count || 0) + " shards") + '</span>' +
            '</div>' +
            '<div class="ocp-toolbar">' +
              renderInspectActionButton("Inspect Task", "cooperative-task", task.id || "", "/mesh/cooperative-tasks/" + encodeURIComponent(task.id || "")) +
            '</div>' +
          '</div>';
        })),
        renderInspectList("Mission Lineage", [
          jobs.length ? '<div class="ocp-inspect-item"><div class="ocp-inspect-item__head"><span class="ocp-inspect-item__title">Job Lineage</span></div><div class="ocp-inspect-item__meta">' + jobs.map(function (job) { return '<span>' + escapeHtml(compactText(job.id || "", 26)) + '</span>'; }).join("") + '</div></div>' : "",
          payload.latest_checkpoint_ref && payload.latest_checkpoint_ref.id ? '<div class="ocp-inspect-item"><div class="ocp-inspect-item__head"><span class="ocp-inspect-item__title">Checkpoint</span></div><div class="ocp-toolbar">' + renderArtifactLink("Open Artifact", payload.latest_checkpoint_ref) + '</div></div>' : "",
          payload.result_bundle_ref && payload.result_bundle_ref.id ? '<div class="ocp-inspect-item"><div class="ocp-inspect-item__head"><span class="ocp-inspect-item__title">Result Bundle</span></div><div class="ocp-toolbar">' + renderArtifactLink("Open Result Bundle", payload.result_bundle_ref) + '</div></div>' : ""
        ]),
        '<section class="ocp-inspect-section">' +
          '<h3 class="ocp-inspect-section__title">Raw JSON</h3>' +
          '<pre class="ocp-json-preview">' + escapeHtml(JSON.stringify(payload, null, 2)) + '</pre>' +
        '</section>'
      ].join("");
    }

    function renderJobInspect(payload) {
      const recovery = payload.recovery || {};
      const queue = payload.queue || {};
      const attempts = payload.attempts || [];
      const mission = payload.mission || {};
      return [
        '<section class="ocp-inspect-section">' +
          '<h3 class="ocp-inspect-section__title">Queue Job</h3>' +
          '<div class="ocp-toolbar">' +
            '<span class="ocp-pill ' + inspectStatusTone(payload.status) + '">' + escapeHtml(String(payload.status || "queued").toUpperCase()) + '</span>' +
            (queue.status ? '<span class="ocp-pill ' + inspectStatusTone(queue.status) + '">' + escapeHtml(String(queue.status).toUpperCase()) + '</span>' : '') +
            (mission.mission_id ? renderInspectActionButton("Mission Context", "mission", mission.mission_id, "/mesh/missions/" + encodeURIComponent(mission.mission_id)) : '') +
            '<a class="ocp-link-button ocp-mono-link" href="/mesh/jobs/' + escapeHtml(payload.id || "") + '" target="_blank" rel="noreferrer">Open JSON</a>' +
          '</div>' +
        '</section>',
        '<section class="ocp-inspect-section">' +
          '<h3 class="ocp-inspect-section__title">Execution + Recovery</h3>' +
          renderInspectStats([
            { label: "Kind", value: payload.kind || "job" },
            { label: "Origin", value: payload.origin || "" },
            { label: "Target", value: payload.target || "local" },
            { label: "Recovery State", value: recovery.state || payload.status || "" },
            { label: "Resumable", value: recovery.resumable ? "yes" : "no" },
            { label: "Queue Deliveries", value: String(queue.delivery_attempts || 0) }
          ]) +
          '<div class="ocp-inspect-copy" style="margin-top:12px;">' + escapeHtml(recovery.recovery_hint || queue.last_error || "Queue-backed execution state is available for operator inspection.") + '</div>' +
          '<div class="ocp-toolbar" style="margin-top:12px;">' +
            renderArtifactLink("Checkpoint Artifact", payload.latest_checkpoint_ref) +
            renderArtifactLink("Result Artifact", payload.result_ref) +
            renderArtifactLink("Result Bundle", payload.result_bundle_ref) +
          '</div>' +
        '</section>',
        renderInspectList("Attempts", attempts.map(function (attempt) {
          return '<div class="ocp-inspect-item">' +
            '<div class="ocp-inspect-item__head">' +
              '<span class="ocp-inspect-item__title">' + escapeHtml(compactText(attempt.id || "attempt", 28)) + '</span>' +
              '<span class="ocp-pill ' + inspectStatusTone(attempt.status) + '">' + escapeHtml(String(attempt.status || "unknown").toUpperCase()) + '</span>' +
            '</div>' +
            '<div class="ocp-inspect-item__meta">' +
              '<span>' + escapeHtml(attempt.executor || "runtime") + '</span>' +
              '<span>' + escapeHtml(relativeTime(attempt.updated_at || attempt.started_at || attempt.created_at)) + '</span>' +
            '</div>' +
          '</div>';
        })),
        '<section class="ocp-inspect-section">' +
          '<h3 class="ocp-inspect-section__title">Raw JSON</h3>' +
          '<pre class="ocp-json-preview">' + escapeHtml(JSON.stringify(payload, null, 2)) + '</pre>' +
        '</section>'
      ].join("");
    }

    function renderCooperativeInspect(payload) {
      const summary = payload.summary || {};
      const counts = summary.counts || {};
      const children = payload.children || [];
      return [
        '<section class="ocp-inspect-section">' +
          '<h3 class="ocp-inspect-section__title">Cooperative Task</h3>' +
          '<div class="ocp-toolbar">' +
            '<span class="ocp-pill ' + inspectStatusTone(payload.state || payload.status) + '">' + escapeHtml(String(payload.state || payload.status || "active").toUpperCase()) + '</span>' +
            '<a class="ocp-link-button ocp-mono-link" href="/mesh/cooperative-tasks/' + escapeHtml(payload.id || "") + '" target="_blank" rel="noreferrer">Open JSON</a>' +
          '</div>' +
          '<div class="ocp-inspect-copy" style="margin-top:12px;">Distributed shard orchestration across local and remote peers.</div>' +
        '</section>',
        '<section class="ocp-inspect-section">' +
          '<h3 class="ocp-inspect-section__title">Shard Progress</h3>' +
          renderInspectStats([
            { label: "Strategy", value: payload.strategy || "spread" },
            { label: "Shards", value: String(payload.shard_count || 0) },
            { label: "Completed", value: String(counts.completed || 0) },
            { label: "Active", value: String(counts.active || 0) },
            { label: "Pending", value: String(counts.pending || 0) },
            { label: "Failed", value: String(counts.attention || counts.failed || 0) }
          ]) +
        '</section>',
        renderInspectList("Child Placements", children.map(function (child) {
          const job = child.job || {};
          const placement = child.placement || {};
          return '<div class="ocp-inspect-item">' +
            '<div class="ocp-inspect-item__head">' +
              '<span class="ocp-inspect-item__title">' + escapeHtml(child.label || compactText(job.id || "child", 24)) + '</span>' +
              '<span class="ocp-pill ' + inspectStatusTone(job.status || child.state) + '">' + escapeHtml(String(job.status || child.state || "queued").toUpperCase()) + '</span>' +
            '</div>' +
            '<div class="ocp-inspect-item__meta">' +
              '<span>' + escapeHtml(placement.target_peer_id || job.target || "local") + '</span>' +
              '<span>' + escapeHtml(job.kind || "job") + '</span>' +
              '<span>' + escapeHtml(relativeTime(job.updated_at || payload.updated_at || payload.created_at)) + '</span>' +
            '</div>' +
            '<div class="ocp-toolbar">' +
              (job.id ? renderInspectActionButton("Inspect Job", "job", job.id, "/mesh/jobs/" + encodeURIComponent(job.id)) : "") +
            '</div>' +
          '</div>';
        })),
        '<section class="ocp-inspect-section">' +
          '<h3 class="ocp-inspect-section__title">Raw JSON</h3>' +
          '<pre class="ocp-json-preview">' + escapeHtml(JSON.stringify(payload, null, 2)) + '</pre>' +
        '</section>'
      ].join("");
    }

    function renderInspectBody(surface, payload) {
      if (surface === "mission") {
        return renderMissionInspect(payload);
      }
      if (surface === "job") {
        return renderJobInspect(payload);
      }
      if (surface === "cooperative-task") {
        return renderCooperativeInspect(payload);
      }
      return '<div class="ocp-empty">No drill-down renderer is available for this surface yet.</div>';
    }

    function closeInspectOverlay() {
      const overlay = document.getElementById("inspect-overlay");
      if (!overlay) {
        return;
      }
      overlay.classList.remove("is-open");
      overlay.setAttribute("aria-hidden", "true");
      app.inspect = { surface: "", id: "", href: "", title: "" };
    }

    async function openInspectOverlay(surface, identifier, title, href) {
      const endpoint = inspectEndpoint(surface, identifier);
      if (!endpoint) {
        return;
      }
      const overlay = document.getElementById("inspect-overlay");
      const body = document.getElementById("inspect-body");
      document.getElementById("inspect-title").textContent = title || (inspectLabel(surface) + " Inspect");
      document.getElementById("inspect-subtitle").textContent = inspectLabel(surface) + " drill-down stays inside the cockpit.";
      body.innerHTML = '<div class="ocp-empty">Loading live ' + escapeHtml(inspectLabel(surface).toLowerCase()) + ' data…</div>';
      overlay.classList.add("is-open");
      overlay.setAttribute("aria-hidden", "false");
      app.inspect = { surface: surface, id: identifier, href: href || endpoint, title: title || "" };
      try {
        const payload = await fetchJson(endpoint);
        document.getElementById("inspect-title").textContent = title || payload.title || payload.name || payload.kind || payload.id || inspectLabel(surface);
        document.getElementById("inspect-subtitle").textContent = "Live " + inspectLabel(surface).toLowerCase() + " view from " + endpoint + ".";
        body.innerHTML = renderInspectBody(surface, payload);
      } catch (error) {
        body.innerHTML = '<div class="ocp-empty">Inspect failed: ' + escapeHtml(error.message) + '</div>';
      }
    }

    function refreshInspectOverlay() {
      if (!app.inspect.surface || !app.inspect.id) {
        return;
      }
      openInspectOverlay(app.inspect.surface, app.inspect.id, app.inspect.title, app.inspect.href);
    }

    function renderOperations(state) {
      const queueMessages = (state.queue && state.queue.messages) || [];
      const jobs = state.jobs || {};
      const target = document.getElementById("operation-grid");
      if (!queueMessages.length) {
        target.innerHTML = '<div class="ocp-empty">No queue or recovery activity yet.</div>';
        clearError("operations");
        return;
      }
      target.innerHTML = queueMessages.map(function (message) {
        const job = jobs[message.job_id];
        if (!job) {
          return "";
        }
        const recovery = job.recovery || {};
        const actions = jobActionSpecs(message, job).map(function (spec) {
          return '<button class="ocp-button ' + escapeHtml(spec.tone) + '" type="button" data-action="' + escapeHtml(spec.action) + '" data-job-id="' + escapeHtml(job.id || "") + '" data-queue-message-id="' + escapeHtml(message.id || "") + '">' + escapeHtml(spec.label) + '</button>';
        }).join("");
        return '<div class="ocp-operation-row">' +
          '<div class="ocp-operation-main">' +
            '<div class="ocp-operation-head">' +
              '<span class="ocp-operation-id">' + escapeHtml(truncateId(job.id || "job", 22)) + '</span>' +
              '<span class="ocp-pill ' + (String(job.status || "").toLowerCase() === "failed" ? "ocp-pill--blocked" : "ocp-pill--violet") + '">' + escapeHtml(String(job.status || "queued").toUpperCase()) + '</span>' +
            '</div>' +
            '<div class="ocp-operation-meta">' +
              '<span>' + escapeHtml(job.kind || "job") + '</span>' +
              '<span>' + escapeHtml(String(message.status || "queued")) + '</span>' +
              '<span>' + escapeHtml(String(message.delivery_attempts || 0) + " deliveries") + '</span>' +
              '<span>' + escapeHtml(relativeTime(job.updated_at || message.updated_at)) + '</span>' +
            '</div>' +
            '<div class="ocp-autonomy-copy">' + escapeHtml(recovery.recovery_hint || message.last_error || "Queue-backed job ready for operator action.") + '</div>' +
            '<div class="ocp-toolbar">' + actions +
              renderInspectActionButton("Inspect Job", "job", job.id || "", "/mesh/jobs/" + encodeURIComponent(job.id || "")) +
              '<a class="ocp-link-button ocp-mono-link" href="/mesh/jobs/' + escapeHtml(job.id || "") + '">Inspect JSON</a>' +
            '</div>' +
          '</div>' +
        '</div>';
      }).join("") || '<div class="ocp-empty">No queue or recovery activity yet.</div>';
      clearError("operations");
    }

    function missionStatusTone(status) {
      const token = String(status || "planned").toLowerCase();
      if (token === "completed") {
        return "ocp-pill--eligible";
      }
      if (token === "failed" || token === "cancelled") {
        return "ocp-pill--blocked";
      }
      if (token === "checkpointed" || token === "waiting") {
        return "ocp-pill--warn";
      }
      return "ocp-pill--role";
    }

    async function operateMission(action, missionId) {
      let endpoint = "/mesh/missions/" + encodeURIComponent(missionId) + "/cancel";
      if (action === "restart") {
        endpoint = "/mesh/missions/" + encodeURIComponent(missionId) + "/restart";
      } else if (action === "resume") {
        endpoint = "/mesh/missions/" + encodeURIComponent(missionId) + "/resume";
      } else if (action === "resume-checkpoint") {
        endpoint = "/mesh/missions/" + encodeURIComponent(missionId) + "/resume-from-checkpoint";
      }
      await fetchJson(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          operator_id: app.state.node_id + ":ocp-mobile-ui",
          reason: "phone_control_mission_" + action
        })
      });
    }

    function renderMissions(state) {
      const missions = (state.missions && state.missions.missions) || [];
      const target = document.getElementById("mission-grid");
      if (!missions.length) {
        target.innerHTML = '<div class="ocp-empty">No active mission objects yet. Mission launches will accumulate durable intent here.</div>';
        clearError("missions");
        return;
      }
      target.innerHTML = missions.map(function (mission) {
        const summary = mission.summary || {};
        const continuity = mission.continuity || {};
        const launch = (mission.metadata || {}).launch || {};
        const lineage = mission.lineage || {};
        const lineageJobs = lineage.jobs || [];
        const lineageTasks = lineage.cooperative_tasks || [];
        const checkpointReady = continuity.checkpoint_ready ? "checkpoint ready" : "no checkpoint";
        const resultReady = mission.result_bundle_ref && Object.keys(mission.result_bundle_ref).length ? "bundle ready" : "result pending";
        const actionButtons = [];
        if (continuity.resumable && ["checkpointed", "failed", "waiting"].includes(String(mission.status || "").toLowerCase())) {
          actionButtons.push('<button class="ocp-button ocp-button--cyan" type="button" data-action="mission-resume" data-mission-id="' + escapeHtml(mission.id || "") + '">Resume Latest</button>');
        }
        if (continuity.checkpoint_ready && ["checkpointed", "failed", "waiting"].includes(String(mission.status || "").toLowerCase())) {
          actionButtons.push('<button class="ocp-button ocp-button--secondary" type="button" data-action="mission-resume-checkpoint" data-mission-id="' + escapeHtml(mission.id || "") + '">Resume Checkpoint</button>');
        }
        if (!["completed", "cancelled"].includes(String(mission.status || "").toLowerCase())) {
          actionButtons.push('<button class="ocp-button ocp-button--coral" type="button" data-action="mission-cancel" data-mission-id="' + escapeHtml(mission.id || "") + '">Cancel Mission</button>');
        }
        if (["failed", "checkpointed", "waiting", "cancelled"].includes(String(mission.status || "").toLowerCase())) {
          actionButtons.push('<button class="ocp-button ocp-button--amber" type="button" data-action="mission-restart" data-mission-id="' + escapeHtml(mission.id || "") + '">Restart Mission</button>');
        }
        const lineageLinks = [];
        if (lineageJobs.length) {
          lineageLinks.push('<a class="ocp-link-button ocp-mono-link" href="/mesh/jobs/' + escapeHtml(lineageJobs[0].id || "") + '">Primary Job</a>');
        }
        if (lineageTasks.length) {
          lineageLinks.push('<a class="ocp-link-button ocp-mono-link" href="/mesh/cooperative-tasks/' + escapeHtml(lineageTasks[0].id || "") + '">Coop Task</a>');
        }
        if (mission.latest_checkpoint_ref && Object.keys(mission.latest_checkpoint_ref).length) {
          lineageLinks.push('<a class="ocp-link-button ocp-mono-link" href="/mesh/artifacts/' + escapeHtml(mission.latest_checkpoint_ref.id || "") + '">Checkpoint</a>');
        }
        if (mission.result_bundle_ref && Object.keys(mission.result_bundle_ref).length) {
          lineageLinks.push('<a class="ocp-link-button ocp-mono-link" href="/mesh/artifacts/' + escapeHtml(mission.result_bundle_ref.id || "") + '">Result Bundle</a>');
        }
        return '<div class="ocp-task-row">' +
          taskGlyph() +
          '<div class="ocp-task-main">' +
            '<div class="ocp-task-head">' +
              '<span class="ocp-task-id">' + escapeHtml(truncateId(mission.title || mission.id || "mission", 28)) + '</span>' +
              '<span class="ocp-pill ' + missionStatusTone(mission.status) + '">' + escapeHtml(String(mission.status || "planned").toUpperCase()) + '</span>' +
            '</div>' +
            '<div class="ocp-autonomy-copy">' + escapeHtml(mission.intent || "Mission intent not provided.") + '</div>' +
            '<div class="ocp-task-meta">' +
              '<span>' + escapeHtml(String(summary.job_count || 0) + " jobs") + '</span>' +
              '<span>' + escapeHtml(String(summary.cooperative_task_count || 0) + " coop tasks") + '</span>' +
              '<span>' + escapeHtml(String(mission.priority || "normal")) + " priority</span>" +
              '<span>' + escapeHtml(String(mission.workload_class || "default")) + '</span>' +
            '</div>' +
            '<div class="ocp-task-meta">' +
              '<span>' + escapeHtml(String(launch.type || "job").replace(/_/g, " ")) + '</span>' +
              '<span>' + escapeHtml(String(mission.target_strategy || "local").replace(/_/g, " ")) + '</span>' +
              '<span>' + escapeHtml(checkpointReady) + '</span>' +
              '<span>' + escapeHtml(resultReady) + '</span>' +
            '</div>' +
            '<div class="ocp-toolbar">' + actionButtons.join("") +
              renderInspectActionButton("Inspect Mission", "mission", mission.id || "", "/mesh/missions/" + encodeURIComponent(mission.id || "")) +
              lineageLinks.join("") +
              '<a class="ocp-link-button ocp-mono-link" href="/mesh/missions/' + escapeHtml(mission.id || "") + '">Inspect JSON</a>' +
            '</div>' +
          '</div>' +
        '</div>';
      }).join("");
      clearError("missions");
    }

    function renderCooperativeTasks(state) {
      const tasks = (state.cooperative_tasks && state.cooperative_tasks.tasks) || [];
      const target = document.getElementById("coop-grid");
      if (!tasks.length) {
        target.innerHTML = '<div class="ocp-empty">No cooperative task shards are active right now.</div>';
        clearError("tasks");
        return;
      }
      target.innerHTML = tasks.map(function (task) {
        const summary = task.summary || {};
        const counts = summary.counts || {};
        const shards = Math.max(1, Number(task.shard_count || 0));
        const done = Math.max(0, Number(counts.completed || 0));
        const progress = Math.max(0, Math.min(100, Math.round((done / shards) * 100)));
        const status = taskStatus(task);
        const firstChild = (task.children || []).find(function (child) { return child && child.job; }) || {};
        const placement = firstChild.placement || {};
        const helperName = placement.target_peer_id || (firstChild.job && firstChild.job.target) || "local";
        return '<div class="ocp-task-row">' +
          taskGlyph() +
          '<div class="ocp-task-main">' +
            '<div class="ocp-task-head">' +
              '<span class="ocp-task-id">' + escapeHtml(truncateId(task.id || task.name || "task", 24)) + '</span>' +
              '<span class="ocp-pill ' + (status.tone === "success" ? "ocp-pill--eligible" : (status.tone === "warn" ? "ocp-pill--warn" : (status.tone === "danger" ? "ocp-pill--blocked" : "ocp-pill--role"))) + '">' + escapeHtml(status.label) + '</span>' +
            '</div>' +
            '<div class="ocp-progress"><span style="width:' + String(progress) + '%;"></span></div>' +
            '<div class="ocp-task-meta">' +
              '<span>' + escapeHtml(done + "/" + shards + " shards") + '</span>' +
              '<span>' + escapeHtml("helper " + truncateId(helperName || "local", 18)) + '</span>' +
              '<span>' + escapeHtml(relativeTime(task.created_at || task.updated_at)) + '</span>' +
            '</div>' +
            '<div class="ocp-toolbar">' +
              renderInspectActionButton("Inspect Task", "cooperative-task", task.id || "", "/mesh/cooperative-tasks/" + encodeURIComponent(task.id || "")) +
              '<a class="ocp-link-button ocp-mono-link" href="/mesh/cooperative-tasks/' + escapeHtml(task.id || "") + '">Inspect JSON</a>' +
            '</div>' +
          '</div>' +
        '</div>';
      }).join("");
      clearError("tasks");
    }

    function connectStatusTone(status) {
      const token = String(status || "").toLowerCase();
      if (["connected", "ready"].includes(token)) {
        return "safe";
      }
      if (["error", "failed", "degraded"].includes(token)) {
        return "danger";
      }
      if (["discovered", "scanned", "pending"].includes(token)) {
        return "warn";
      }
      return "violet";
    }

    function compactEndpoint(url) {
      return String(url || "").replace(/^https?:\\/\\//, "");
    }

    function renderConnectDevices(state) {
      const peers = (state.peers && state.peers.peers) || [];
      const candidates = (state.discovery_candidates && state.discovery_candidates.candidates) || [];
      const connectivity = state.connectivity || {};
      const grid = document.getElementById("connect-grid");
      const diagnostics = document.getElementById("connect-diagnostics");
      const errors = document.getElementById("connect-errors");
      const summary = document.getElementById("connect-summary");
      const pill = document.getElementById("connect-state-pill");
      const connectedPeerIds = new Set(peers.map(function (peer) { return String(peer.peer_id || ""); }).filter(Boolean));
      const cards = [];

      peers.forEach(function (peer) {
        const status = String(peer.status || "connected").toLowerCase();
        const endpoint = peer.endpoint_url || "";
        const profile = peer.device_profile || {};
        const details = [
          profile.device_class ? String(profile.device_class).toUpperCase() : "",
          profile.form_factor ? String(profile.form_factor).toUpperCase() : "",
          relativeTime(peer.last_seen_at || peer.updated_at)
        ].filter(Boolean);
        cards.push({
          key: "peer:" + String(peer.peer_id || endpoint),
          title: peer.display_name || peer.peer_id || "Connected peer",
          endpoint: endpoint,
          status: status || "connected",
          tone: connectStatusTone(status),
          meta: details,
          copy: status === "connected"
            ? "This node is already in the sovereign mesh. You can send a proof mission immediately."
            : "This peer exists in the registry but may need a sync or reconnect before remote execution.",
          peer_id: peer.peer_id || "",
          base_url: endpoint,
          connected: true
        });
      });

      candidates.forEach(function (candidate) {
        const candidatePeerId = String(candidate.peer_id || "");
        if (candidatePeerId && connectedPeerIds.has(candidatePeerId)) {
          return;
        }
        const endpoint = candidate.endpoint_url || candidate.base_url || "";
        const status = String(candidate.status || "discovered").toLowerCase();
        const profile = candidate.device_profile || {};
        cards.push({
          key: "candidate:" + String(endpoint),
          title: candidate.display_name || candidate.peer_id || compactEndpoint(endpoint) || "Discovered node",
          endpoint: endpoint,
          status: status,
          tone: connectStatusTone(status === "error" ? "error" : status),
          meta: [
            profile.device_class ? String(profile.device_class).toUpperCase() : "",
            profile.form_factor ? String(profile.form_factor).toUpperCase() : "",
            candidate.last_seen_at ? relativeTime(candidate.last_seen_at) : ""
          ].filter(Boolean),
          copy: candidate.last_error
            ? "Last reachability error: " + String(candidate.last_error)
            : "Discovered node ready for one-click trust + connect.",
          peer_id: candidatePeerId,
          base_url: endpoint,
          connected: false
        });
      });

      const connectedCount = peers.filter(function (peer) {
        return String(peer.status || "").toLowerCase() === "connected";
      }).length;
      const errorCount = candidates.filter(function (candidate) {
        return String(candidate.status || "").toLowerCase() === "error" || String(candidate.last_error || "").trim();
      }).length;
      const discoveredCount = candidates.filter(function (candidate) {
        return ["discovered", "connected", "self"].includes(String(candidate.status || "").toLowerCase());
      }).length;
      summary.innerHTML = [
        '<span>' + escapeHtml(String(connectedCount) + " connected") + '</span>',
        '<span>' + escapeHtml(String(discoveredCount) + " discovered") + '</span>',
        '<span>' + escapeHtml(String((connectivity.local_ipv4 || []).length) + " local IPs") + '</span>'
      ].join("");
      if (connectedCount > 0) {
        pill.textContent = "MESH LINKED";
        pill.className = "ocp-pill ocp-pill--eligible";
      } else if (discoveredCount > 0) {
        pill.textContent = "READY TO CONNECT";
        pill.className = "ocp-pill ocp-pill--warn";
      } else if (errorCount > 0) {
        pill.textContent = "CHECK REACHABILITY";
        pill.className = "ocp-pill ocp-pill--blocked";
      } else {
        pill.textContent = "DISCOVERY READY";
        pill.className = "ocp-pill ocp-pill--violet";
      }

      grid.innerHTML = cards.length ? cards.map(function (card) {
        const statusLabel = String(card.status || (card.connected ? "connected" : "discovered")).replace(/_/g, " ");
        const connectLabel = card.connected ? "Reconnect" : "Connect";
        return '<article class="ocp-connect-card">' +
          '<div class="ocp-connect-card__head">' +
            '<div>' +
              '<h3 class="ocp-connect-card__title">' + escapeHtml(card.title) + '</h3>' +
              '<div class="ocp-connect-card__endpoint ocp-mono">' + escapeHtml(compactEndpoint(card.endpoint)) + '</div>' +
            '</div>' +
            '<span class="ocp-pill ' + pillClass(card.tone) + '">' + escapeHtml(String(statusLabel).toUpperCase()) + '</span>' +
          '</div>' +
          '<div class="ocp-connect-card__meta">' + card.meta.map(function (item) {
            return '<span>' + escapeHtml(item) + '</span>';
          }).join("") + '</div>' +
          '<div class="ocp-connect-card__copy">' + escapeHtml(card.copy) + '</div>' +
          '<div class="ocp-connect-card__actions">' +
            '<button class="ocp-button ocp-button--cyan" type="button" data-action="connect-peer" data-peer-id="' + escapeHtml(card.peer_id || "") + '" data-base-url="' + escapeHtml(card.base_url || "") + '">' + escapeHtml(connectLabel) + '</button>' +
            '<button class="ocp-button ocp-button--amber" type="button" data-action="send-test-mission" data-peer-id="' + escapeHtml(card.peer_id || "") + '" data-base-url="' + escapeHtml(card.base_url || "") + '">Send Test Mission</button>' +
          '</div>' +
        '</article>';
      }).join("") : '<div class="ocp-empty">No nearby devices have been scanned yet. Start with <strong>Scan Nearby</strong>, or paste a device URL above and press Connect.</div>';

      diagnostics.innerHTML = [
        connectivity.base_url ? '<span class="ocp-mono">' + escapeHtml("Advertised " + compactEndpoint(connectivity.base_url)) + '</span>' : "",
        connectivity.port ? '<span>' + escapeHtml("Port " + String(connectivity.port)) + '</span>' : "",
        (connectivity.local_ipv4 || []).map(function (ip) {
          return '<span class="ocp-mono">' + escapeHtml(ip) + '</span>';
        }).join("")
      ].filter(Boolean).join("");

      const recentErrors = connectivity.recent_errors || [];
      errors.innerHTML = recentErrors.length ? recentErrors.map(function (item) {
        return '<div class="ocp-connect-error">' +
          '<strong>' + escapeHtml(item.display_name || compactEndpoint(item.base_url || "")) + '</strong><br>' +
          escapeHtml(item.error || "reachability error") +
        '</div>';
      }).join("") : '<div class="ocp-empty">No recent connect errors. If scan misses a node, paste its URL and press Connect.</div>';
      clearError("connect");
    }

    function renderAutonomy(state) {
      const autonomy = state.autonomy || {};
      const policy = autonomy.policy || {};
      const posture = autonomyPosture(policy);
      const descriptionBits = [];
      descriptionBits.push("Policy mode is " + String(policy.mode || "manual") + ".");
      descriptionBits.push("Threshold is " + String(policy.pressure_threshold || "elevated") + ".");
      descriptionBits.push("Decision is " + String(autonomy.decision || "noop") + ".");
      if ((autonomy.reasons || []).length) {
        descriptionBits.push("Signals: " + autonomy.reasons.join(", ").replace(/_/g, " ") + ".");
      }
      document.getElementById("autonomy-description").textContent = descriptionBits.join(" ");
      document.getElementById("autonomy-last-run").textContent = "Last autonomy run: " + relativeTime(autonomy.generated_at || "");
      ["manual", "assisted", "autonomous"].forEach(function (key) {
        const node = document.getElementById("autonomy-" + key);
        node.classList.toggle("is-active", key.toUpperCase() === posture.label);
      });
      const indicator = document.getElementById("autonomy-indicator");
      indicator.style.transform = "translateX(calc(" + String(posture.index) + " * (100% + 6px)))";
      indicator.style.background = posture.tone === "manual"
        ? "rgba(139, 127, 232, 0.2)"
        : (posture.tone === "assisted" ? "rgba(255, 149, 0, 0.18)" : "rgba(0, 212, 255, 0.18)");
      clearError("autonomy");
    }

    function renderOffloadMemory(state) {
      const preferences = (state.preferences && state.preferences.preferences) || [];
      const target = document.getElementById("offload-grid");
      if (!preferences.length) {
        target.innerHTML = '<div class="ocp-empty">No active offloaded memory is currently stored on this node.</div>';
        clearError("offload");
        return;
      }
      target.innerHTML = preferences.map(function (item) {
        const metadata = item.metadata || {};
        const statusTone = item.preference === "deny" ? "ocp-pill--blocked" : (item.preference === "approval" ? "ocp-pill--warn" : "ocp-pill--eligible");
        return '<div class="ocp-offload-row">' +
          '<span class="ocp-task-id">' + escapeHtml(item.workload_class || "default") + '</span>' +
          '<span class="ocp-mono">' + escapeHtml(String(metadata.size_bytes || "--")) + '</span>' +
          '<span>' + escapeHtml(item.peer_id || "peer") + '</span>' +
          '<span class="ocp-pill ' + statusTone + '">' + escapeHtml(String(item.preference || "allow").toUpperCase()) + '</span>' +
          '<a class="ocp-link-button ocp-mono-link" href="/mesh/helpers/preferences?peer_id=' + encodeURIComponent(item.peer_id || "") + '">View</a>' +
        '</div>';
      }).join("");
      clearError("offload");
    }

    function preferenceLookup(state, peerId, workloadClass) {
      const preferences = ((state.preferences && state.preferences.preferences) || []);
      const targetWorkload = String(workloadClass || "default");
      const peerToken = String(peerId || "");
      return preferences.find(function (item) {
        return String(item.peer_id || "") === peerToken && String(item.workload_class || "default") === targetWorkload;
      }) || preferences.find(function (item) {
        return String(item.peer_id || "") === peerToken && String(item.workload_class || "default") === "default";
      }) || null;
    }

    function renderHelperFleet(state) {
      const helpers = (state.helpers && state.helpers.helpers) || [];
      const peers = (state.peers && state.peers.peers) || [];
      const peerMap = Object.fromEntries(peers.map(function (peer) { return [peer.peer_id, peer]; }));
      const candidates = (state.autonomy && state.autonomy.candidates) || [];
      const activeWorkload = String((((state.autonomy || {}).placement || {}).workload_class) || "default");
      const candidateRanks = {};
      candidates.forEach(function (candidate, index) {
        candidateRanks[candidate.peer_id] = index + 1;
      });
      document.getElementById("helper-count-pill").textContent = helpers.length + " helpers";
      const target = document.getElementById("helper-grid");
      if (!helpers.length) {
        target.innerHTML = '<div class="ocp-empty ocp-empty--fleet">No helpers discovered on mesh. Expand peer search or wait for trusted nodes to appear.</div>';
        clearError("fleet");
        return;
      }
      target.innerHTML = helpers.map(function (helper) {
        const compute = helper.compute_profile || {};
        const peer = peerMap[helper.peer_id] || {};
        const load = peer.load || {};
        const status = String(peer.status || helper.state || "offline").toLowerCase();
        const helperState = String(helper.state || "unenlisted").toLowerCase();
        const latency = latencyValue(peer);
        const isOffline = !peer.peer_id || status === "disconnected" || status === "offline";
        const busy = !isOffline && (helperState === "draining" || load.pressure === "elevated" || load.pressure === "saturated");
        const dotClass = isOffline ? "ocp-status-dot ocp-status-dot--offline" : (busy ? "ocp-status-dot ocp-status-dot--busy" : "ocp-status-dot ocp-status-dot--live");
        const priority = candidateRanks[helper.peer_id] || "-";
        const preference = preferenceLookup(state, helper.peer_id, activeWorkload);
        const prefValue = String((preference && preference.preference) || "allow");
        const prefLabel = prefValue === "prefer" ? "Always use" : (prefValue === "approval" ? "Ask first" : (prefValue === "avoid" ? "Avoid" : (prefValue === "deny" ? "Never use" : "Allowed")));
        let actions = "";
        if (helperState === "unenlisted" || helperState === "idle" || !helperState) {
          actions += '<button class="ocp-button ocp-button--cyan" type="button" data-action="helper-enlist" data-peer-id="' + escapeHtml(helper.peer_id || "") + '">Enlist for Task</button>';
        }
        if (helperState === "enlisted") {
          actions += '<button class="ocp-button ocp-button--amber" type="button" data-action="helper-drain" data-peer-id="' + escapeHtml(helper.peer_id || "") + '">Drain</button>';
        }
        actions += '<button class="ocp-button ocp-button--coral" type="button" data-action="helper-retire" data-peer-id="' + escapeHtml(helper.peer_id || "") + '">Retire</button>';
        return '<article class="ocp-helper-card">' +
          '<div class="ocp-helper-card__top">' +
            '<div>' +
              '<h3 class="ocp-helper-card__name">' + escapeHtml(helper.display_name || helper.peer_id || "Helper") + '</h3>' +
              '<div class="ocp-helper-card__caps">' +
                '<span class="ocp-mini-tag ocp-mini-tag--trust">' + escapeHtml(String(helper.trust_tier || "trusted").toUpperCase()) + '</span>' +
                (compute.gpu_capable ? '<span class="ocp-mini-tag ocp-mini-tag--gpu">GPU</span>' : '') +
              '</div>' +
            '</div>' +
            '<span class="' + dotClass + '"></span>' +
          '</div>' +
          '<div class="ocp-helper-card__meta">' +
            '<span class="ocp-mono">' + escapeHtml(String(compute.memory_mb || 0) + " MB • " + String(compute.cpu_cores || 0) + " CPU") + '</span>' +
            '<span style="color:' + latencyColor(latency) + ';">' + escapeHtml(latency == null ? "latency n/a" : latency + " ms") + '</span>' +
            '<span>' + escapeHtml("queue " + String(load.queue_depth || 0) + " • " + helperState) + '</span>' +
          '</div>' +
          '<div class="ocp-helper-card__actions">' +
            '<span class="ocp-rank">Priority ' + escapeHtml(String(priority)) + '</span>' +
            '<span class="ocp-relative">' + escapeHtml(relativeTime(peer.last_seen_at || helper.last_action_at || helper.enlisted_at)) + '</span>' +
          '</div>' +
          '<div class="ocp-toolbar">' + actions + '</div>' +
          '<div class="ocp-helper-card__memory">' +
            '<div class="ocp-helper-card__memory-head">' +
              '<span class="ocp-helper-card__memory-title">Memory for ' + escapeHtml(activeWorkload) + '</span>' +
              '<span class="ocp-pill ' + (prefValue === "deny" ? "ocp-pill--blocked" : (prefValue === "approval" || prefValue === "avoid" ? "ocp-pill--warn" : "ocp-pill--eligible")) + '">' + escapeHtml(String(prefLabel).toUpperCase()) + '</span>' +
            '</div>' +
            '<div class="ocp-helper-pref-group">' +
              '<button class="ocp-chip-button ' + (prefValue === "prefer" ? 'is-active' : '') + '" type="button" data-action="set-helper-preference" data-peer-id="' + escapeHtml(helper.peer_id || "") + '" data-preference="prefer" data-workload-class="' + escapeHtml(activeWorkload) + '">Always</button>' +
              '<button class="ocp-chip-button ' + (prefValue === "approval" ? 'is-active' : '') + '" type="button" data-action="set-helper-preference" data-peer-id="' + escapeHtml(helper.peer_id || "") + '" data-preference="approval" data-workload-class="' + escapeHtml(activeWorkload) + '">Ask</button>' +
              '<button class="ocp-chip-button ' + (prefValue === "avoid" ? 'is-active' : '') + '" type="button" data-action="set-helper-preference" data-peer-id="' + escapeHtml(helper.peer_id || "") + '" data-preference="avoid" data-workload-class="' + escapeHtml(activeWorkload) + '">Avoid</button>' +
              '<button class="ocp-chip-button is-danger ' + (prefValue === "deny" ? 'is-active' : '') + '" type="button" data-action="set-helper-preference" data-peer-id="' + escapeHtml(helper.peer_id || "") + '" data-preference="deny" data-workload-class="' + escapeHtml(activeWorkload) + '">Never</button>' +
            '</div>' +
            '<div class="ocp-helper-card__memory-note">' + escapeHtml((preference && preference.metadata && preference.metadata.note) || "This memory will steer future autonomy decisions for this workload.") + '</div>' +
          '</div>' +
        '</article>';
      }).join("");
      clearError("fleet");
    }

    function renderPeers(state) {
      const peers = (state.peers && state.peers.peers) || [];
      const target = document.getElementById("peer-grid");
      if (!peers.length) {
        target.innerHTML = '<div class="ocp-empty">No peer links are active right now.</div>';
        clearError("peers");
        return;
      }
      target.innerHTML = peers.map(function (peer) {
        const profile = peer.device_profile || {};
        const dotClass = peer.status === "connected" ? "ocp-status-dot ocp-status-dot--live" : (peer.status === "degraded" ? "ocp-status-dot ocp-status-dot--busy" : "ocp-status-dot ocp-status-dot--offline");
        const tags = [profile.device_class, profile.execution_tier, profile.form_factor].filter(Boolean).slice(0, 3);
        return '<article class="ocp-peer-card">' +
          '<div class="ocp-peer-card__top">' +
            '<div>' +
              '<div class="ocp-peer-card__id">' + escapeHtml(truncateId(peer.peer_id || "peer", 12)) + '</div>' +
              '<div class="ocp-relative ocp-mono">' + escapeHtml((peer.endpoint_url || "").replace(/^https?:\\/\\//, "")) + '</div>' +
            '</div>' +
            '<span class="' + dotClass + '"></span>' +
          '</div>' +
          '<div class="ocp-peer-card__meta">' +
            '<span>' + escapeHtml("last seen " + relativeTime(peer.last_seen_at || peer.updated_at)) + '</span>' +
            '<span>' + escapeHtml(String(peer.status || "unknown")) + '</span>' +
          '</div>' +
          '<div class="ocp-peer-tags">' + tags.map(function (tag) {
            return '<span class="ocp-mini-tag">' + escapeHtml(String(tag).toUpperCase()) + '</span>';
          }).join("") + '</div>' +
        '</article>';
      }).join("");
      clearError("peers");
    }

    function renderApprovals(state) {
      const approvals = (state.approvals && state.approvals.approvals) || [];
      const pending = approvals.filter(function (item) { return item.status === "pending"; });
      const section = document.getElementById("approvals-section");
      const title = document.getElementById("approvals-title");
      document.getElementById("approval-count-pill").textContent = pending.length + " pending";
      section.classList.toggle("ocp-approvals--hot", pending.length > 0);
      title.innerHTML = pending.length > 0 ? 'Approvals Queue <span class="ocp-approvals__pulse"></span>' : "Approvals Queue";
      const target = document.getElementById("approval-grid");
      if (!approvals.length) {
        target.innerHTML = '<div class="ocp-empty">Approval queue is clear.</div>';
        clearError("approvals");
        return;
      }
      target.innerHTML = approvals.map(function (item) {
        const locked = item.status !== "pending";
        const risk = (item.metadata || {}).risk_level || item.severity || "normal";
        return '<article class="ocp-approval-card ' + (locked ? "is-dim" : "") + '">' +
          '<div class="ocp-approval-head">' +
            '<div>' +
              '<h4>' + escapeHtml(item.title || "Approval request") + '</h4>' +
              '<p class="ocp-approval-summary">' + escapeHtml(item.summary || item.compact_summary || "") + '</p>' +
            '</div>' +
            '<span class="ocp-pill ' + (item.status === "pending" ? "ocp-pill--warn" : "ocp-pill--violet") + '">' + escapeHtml(String(item.status || "pending").toUpperCase()) + '</span>' +
          '</div>' +
          '<div class="ocp-approval-meta">' +
            '<span>' + escapeHtml(item.action_type || "approval") + '</span>' +
            '<span>' + escapeHtml("requester " + (item.requested_by_peer_id || item.requested_by_agent_id || "unknown")) + '</span>' +
            '<span>' + escapeHtml(relativeTime(item.created_at)) + '</span>' +
            '<span>' + escapeHtml("risk " + String(risk)) + '</span>' +
          '</div>' +
          '<div class="ocp-approval-actions">' +
            '<button class="ocp-button ocp-button--cyan" type="button" data-action="resolve" data-decision="approved" data-approval-id="' + escapeHtml(item.id || "") + '"' + (locked ? ' disabled="disabled"' : "") + '>Approve</button>' +
            '<button class="ocp-button ocp-button--coral" type="button" data-action="resolve" data-decision="rejected" data-approval-id="' + escapeHtml(item.id || "") + '"' + (locked ? ' disabled="disabled"' : "") + '>Reject</button>' +
            '<button class="ocp-button ocp-button--secondary" type="button" data-action="resolve" data-decision="deferred" data-approval-id="' + escapeHtml(item.id || "") + '"' + (locked ? ' disabled="disabled"' : "") + '>Defer</button>' +
          '</div>' +
        '</article>';
      }).join("");
      clearError("approvals");
    }

    function renderNotifications(state) {
      const notifications = (state.notifications && state.notifications.notifications) || [];
      const target = document.getElementById("notification-grid");
      if (!notifications.length) {
        target.innerHTML = '<div class="ocp-empty">No notifications for this node.</div>';
        clearError("notifications");
        return;
      }
      target.innerHTML = notifications.map(function (item, index) {
        const locked = item.status !== "unread";
        return '<div class="ocp-notification-row is-' + escapeHtml(String(item.priority || "normal")) + ' ' + (index >= 20 ? 'is-dim' : '') + '">' +
          '<span class="ocp-notification-strip"></span>' +
          '<div class="ocp-notification-main">' +
            '<div class="ocp-notification-head">' +
              '<span class="ocp-notification-title">' + escapeHtml(item.title || item.compact_title || "Notification") + '</span>' +
              '<span class="ocp-mono ocp-relative">' + escapeHtml(relativeTime(item.created_at)) + '</span>' +
            '</div>' +
            '<div class="ocp-notification-meta">' +
              '<span>' + escapeHtml(item.notification_type || "notification") + '</span>' +
              '<span>' + escapeHtml(item.priority || "normal") + '</span>' +
              '<span>' + escapeHtml(item.status || "unread") + '</span>' +
            '</div>' +
            '<div class="ocp-autonomy-copy">' + escapeHtml(item.body || item.compact_body || "") + '</div>' +
          '</div>' +
          '<button class="ocp-button ocp-button--secondary" type="button" data-action="ack" data-notification-id="' + escapeHtml(item.id || "") + '"' + (locked ? ' disabled="disabled"' : "") + '>' + escapeHtml(locked ? "Acked" : "Acknowledge") + '</button>' +
        '</div>';
      }).join("");
      clearError("notifications");
    }

    function renderJsonSurfaces() {
      document.getElementById("json-surfaces").innerHTML = JSON_SURFACES.map(function (surface) {
        return '<a class="ocp-chip-button" href="' + escapeHtml(surface.href) + '" target="_blank" rel="noreferrer" data-copy-url="' + escapeHtml(surface.href) + '">' + escapeHtml(surface.label) + '</a>';
      }).join("");
    }

    function renderAll(state) {
      renderHero(state);
      renderPulse(state);
      renderConnectDevices(state);
      renderCenterpiece(state);
      renderOperations(state);
      renderMissions(state);
      renderCooperativeTasks(state);
      renderAutonomy(state);
      renderOffloadMemory(state);
      renderHelperFleet(state);
      renderPeers(state);
      renderApprovals(state);
      renderNotifications(state);
    }

    function resizeCanvas(canvas) {
      if (!canvas) {
        return;
      }
      const rect = canvas.getBoundingClientRect();
      const scale = window.devicePixelRatio || 1;
      const width = Math.max(1, Math.round(rect.width * scale));
      const height = Math.max(1, Math.round(rect.height * scale));
      if (canvas.width !== width || canvas.height !== height) {
        canvas.width = width;
        canvas.height = height;
      }
    }

    function drawGauge(canvas, percent, type) {
      if (!canvas) {
        return;
      }
      resizeCanvas(canvas);
      const ctx = canvas.getContext("2d");
      const width = canvas.width;
      const height = canvas.height;
      ctx.clearRect(0, 0, width, height);
      const scale = window.devicePixelRatio || 1;
      ctx.save();
      ctx.scale(scale, scale);
      const cssWidth = width / scale;
      const cssHeight = height / scale;
      ctx.lineCap = "round";
      if (type === "hero") {
        const cx = cssWidth / 2;
        const cy = cssHeight - 8;
        const radius = Math.min(cssWidth / 2 - 8, cssHeight - 12);
        ctx.strokeStyle = "rgba(255,255,255,0.08)";
        ctx.lineWidth = 5;
        ctx.beginPath();
        ctx.arc(cx, cy, radius, Math.PI, 0);
        ctx.stroke();
        ctx.strokeStyle = gaugeColor(percent);
        ctx.beginPath();
        ctx.arc(cx, cy, radius, Math.PI, Math.PI + (percent / 100) * Math.PI);
        ctx.stroke();
      } else {
        const cx = cssWidth / 2;
        const cy = cssHeight / 2;
        const radius = Math.min(cssWidth, cssHeight) / 2 - 18;
        const start = Math.PI * 0.75;
        const sweep = Math.PI * 1.5;
        ctx.strokeStyle = "rgba(255,255,255,0.05)";
        ctx.lineWidth = 16;
        ctx.beginPath();
        ctx.arc(cx, cy, radius, start, start + sweep);
        ctx.stroke();
        ctx.strokeStyle = gaugeColor(percent);
        ctx.beginPath();
        ctx.arc(cx, cy, radius, start, start + sweep * (percent / 100));
        ctx.stroke();
      }
      ctx.restore();
    }

    function animateGauge(name) {
      const gauge = app.gauges[name];
      if (!gauge || !gauge.canvas) {
        return;
      }
      const start = gauge.current;
      const target = gauge.target;
      const startedAt = performance.now();
      const duration = 1200;
      function tick(now) {
        const elapsed = Math.min(1, (now - startedAt) / duration);
        const eased = 1 - Math.pow(1 - elapsed, 3);
        gauge.current = start + (target - start) * eased;
        drawGauge(gauge.canvas, gauge.current, gauge.type);
        if (elapsed < 1) {
          requestAnimationFrame(tick);
        } else {
          gauge.current = target;
          drawGauge(gauge.canvas, gauge.current, gauge.type);
        }
      }
      requestAnimationFrame(tick);
    }

    function setGaugeTarget(name, value) {
      const gauge = app.gauges[name];
      if (!gauge) {
        return;
      }
      gauge.target = value;
      animateGauge(name);
    }

    function initGauges() {
      app.gauges.hero.canvas = document.getElementById("hero-pressure-canvas");
      app.gauges.main.canvas = document.getElementById("pressure-gauge-canvas");
      window.addEventListener("resize", function () {
        drawGauge(app.gauges.hero.canvas, app.gauges.hero.current, "hero");
        drawGauge(app.gauges.main.canvas, app.gauges.main.current, "main");
      });
    }

    function initMesh() {
      const canvas = document.getElementById("mesh-bg");
      const context = canvas.getContext("2d");
      const scene = {
        nodes: [],
        pulses: []
      };
      app.meshScene = scene;
      function reset() {
        canvas.width = window.innerWidth * (window.devicePixelRatio || 1);
        canvas.height = window.innerHeight * (window.devicePixelRatio || 1);
        const scale = window.devicePixelRatio || 1;
        context.setTransform(scale, 0, 0, scale, 0, 0);
        const width = window.innerWidth;
        const height = window.innerHeight;
        const count = Math.max(18, Math.min(34, Math.round((width * height) / 58000)));
        scene.nodes = Array.from({ length: count }).map(function () {
          return {
            x: Math.random() * width,
            y: Math.random() * height,
            vx: (Math.random() - 0.5) * 0.16,
            vy: (Math.random() - 0.5) * 0.16
          };
        });
        scene.pulses = [];
      }
      function maybePulse(edges) {
        if (edges.length && Math.random() < 0.018 && scene.pulses.length < 6) {
          const edge = edges[Math.floor(Math.random() * edges.length)];
          scene.pulses.push({
            edge: edge,
            progress: 0
          });
        }
      }
      function render() {
        const width = window.innerWidth;
        const height = window.innerHeight;
        context.clearRect(0, 0, width, height);
        const edges = [];
        scene.nodes.forEach(function (node) {
          node.x += node.vx;
          node.y += node.vy;
          if (node.x < 0 || node.x > width) {
            node.vx *= -1;
          }
          if (node.y < 0 || node.y > height) {
            node.vy *= -1;
          }
        });
        for (let index = 0; index < scene.nodes.length; index += 1) {
          for (let inner = index + 1; inner < scene.nodes.length; inner += 1) {
            const left = scene.nodes[index];
            const right = scene.nodes[inner];
            const dx = left.x - right.x;
            const dy = left.y - right.y;
            const distance = Math.hypot(dx, dy);
            if (distance < 180) {
              edges.push([left, right]);
              context.strokeStyle = "rgba(0, 212, 255, 0.03)";
              context.lineWidth = 1;
              context.beginPath();
              context.moveTo(left.x, left.y);
              context.lineTo(right.x, right.y);
              context.stroke();
            }
          }
        }
        maybePulse(edges);
        scene.pulses = scene.pulses.filter(function (pulse) {
          pulse.progress += 1 / 90;
          if (pulse.progress >= 1) {
            return false;
          }
          const left = pulse.edge[0];
          const right = pulse.edge[1];
          const x = left.x + (right.x - left.x) * pulse.progress;
          const y = left.y + (right.y - left.y) * pulse.progress;
          context.strokeStyle = "rgba(0, 212, 255, 0.26)";
          context.lineWidth = 2;
          context.beginPath();
          context.moveTo(x - (right.x - left.x) * 0.06, y - (right.y - left.y) * 0.06);
          context.lineTo(x + (right.x - left.x) * 0.06, y + (right.y - left.y) * 0.06);
          context.stroke();
          return true;
        });
        scene.nodes.forEach(function (node) {
          context.fillStyle = "rgba(0, 212, 255, 0.08)";
          context.beginPath();
          context.arc(node.x, node.y, 2, 0, Math.PI * 2);
          context.fill();
        });
        requestAnimationFrame(render);
      }
      window.addEventListener("resize", reset);
      reset();
      render();
    }

    function buttonLoading(button, loading) {
      if (!button) {
        return;
      }
      if (loading) {
        if (!button.dataset.originalLabel) {
          button.dataset.originalLabel = button.innerHTML;
        }
        button.disabled = true;
        button.innerHTML = '<span class="ocp-spinner"></span> ' + escapeHtml(button.textContent.trim() || "Working");
      } else {
        if (button.dataset.originalLabel) {
          button.innerHTML = button.dataset.originalLabel;
        }
        button.disabled = false;
      }
    }

    async function fetchState(options) {
      const config = options || {};
      const manifest = await fetchJson("/mesh/manifest");
      const nodeId = (manifest.organism_card && (manifest.organism_card.organism_id || manifest.organism_card.node_id)) || app.state.node_id;
      const requests = [
        { key: "peers", url: "/mesh/peers?limit=8", section: "peers" },
        { key: "notifications", url: "/mesh/notifications?limit=8&target_peer_id=" + encodeURIComponent(nodeId), section: "notifications" },
        { key: "approvals", url: "/mesh/approvals?limit=8&target_peer_id=" + encodeURIComponent(nodeId), section: "approvals" },
        { key: "discovery_candidates", url: "/mesh/discovery/candidates?limit=12", section: "connect" },
        { key: "connectivity", url: "/mesh/connectivity/diagnostics", section: "connect" },
        { key: "queue_metrics", url: "/mesh/queue/metrics", section: "centerpiece" },
        { key: "workers", url: "/mesh/workers?limit=8", section: "centerpiece" },
        { key: "queue", url: "/mesh/queue?limit=8", section: "operations" },
        { key: "missions", url: "/mesh/missions?limit=6", section: "missions" },
        { key: "pressure", url: "/mesh/pressure", section: "centerpiece" },
        { key: "helpers", url: "/mesh/helpers?limit=12", section: "fleet" },
        { key: "cooperative_tasks", url: "/mesh/cooperative-tasks?limit=6", section: "tasks" },
        { key: "autonomy", url: "/mesh/helpers/autonomy", section: "autonomy" },
        { key: "preferences", url: "/mesh/helpers/preferences?limit=6", section: "offload" }
      ];
      const results = await Promise.allSettled(requests.map(function (item) {
        return fetchJson(item.url);
      }));
      const nextState = Object.assign({}, app.state, {
        manifest: manifest,
        node_id: nodeId,
        display_name: (manifest.organism_card && (manifest.organism_card.display_name || manifest.organism_card.organism_id || manifest.organism_card.node_id)) || app.state.display_name,
        role_label: (manifest.organism_card && manifest.organism_card.role) || app.state.role_label || "Sovereign Node",
        version: [((manifest.implementation || {}).name || "OCP"), manifest.protocol_release || manifest.protocol_version || ""].filter(Boolean).join(" "),
        device_profile: manifest.device_profile || app.state.device_profile || {},
        sync_policy: manifest.sync_policy || app.state.sync_policy || {}
      });
      results.forEach(function (result, index) {
        const request = requests[index];
        if (result.status === "fulfilled") {
          nextState[request.key] = result.value;
          clearError(request.section);
          if (request.key === "peers") {
            clearError("hero");
          }
        } else {
          showError(request.section, "Live update failed: " + result.reason.message);
          if (request.key === "peers") {
            showError("hero", "Mesh update failed: " + result.reason.message);
          }
        }
      });
      const queueMessages = (nextState.queue && nextState.queue.messages) || [];
      const jobPairs = await Promise.all(queueMessages.map(async function (message) {
        try {
          const job = await fetchJson("/mesh/jobs/" + encodeURIComponent(message.job_id));
          return [message.job_id, job];
        } catch (error) {
          return [message.job_id, null];
        }
      }));
      nextState.jobs = Object.fromEntries(jobPairs.filter(function (pair) { return pair[1]; }));
      app.state = nextState;
      renderAll(app.state);
      if (app.inspect.surface && app.inspect.id) {
        refreshInspectOverlay();
      }
      if (!config.silent) {
        setStatus("Cockpit refreshed for " + nodeId + ".");
      }
      return nextState;
    }

    function closeControlStream() {
      if (app.stream.reconnectTimer) {
        clearTimeout(app.stream.reconnectTimer);
        app.stream.reconnectTimer = null;
      }
      if (app.stream.source) {
        app.stream.source.close();
        app.stream.source = null;
      }
    }

    function scheduleControlStreamReconnect() {
      if (app.stream.reconnectTimer) {
        return;
      }
      app.stream.reconnectTimer = setTimeout(function () {
        app.stream.reconnectTimer = null;
        initControlStream();
      }, 2500);
    }

    function applyControlStreamEnvelope(envelope) {
      if (!envelope || !envelope.state) {
        return;
      }
      const nextState = Object.assign({}, app.state, envelope.state);
      const controlStream = Object.assign({}, nextState.control_stream || {}, {
        cursor: Number(envelope.cursor || (((envelope.state || {}).control_stream || {}).cursor) || app.stream.cursor || 0),
        recent_event_count: Array.isArray(envelope.events) ? envelope.events.length : 0,
        generated_at: envelope.generated_at || ""
      });
      nextState.control_stream = controlStream;
      app.state = nextState;
      app.stream.cursor = Number(controlStream.cursor || 0);
      renderAll(app.state);
      if (app.inspect.surface && app.inspect.id) {
        refreshInspectOverlay();
      }
      if (Array.isArray(envelope.events) && envelope.events.length) {
        const latestEvent = envelope.events[envelope.events.length - 1] || {};
        setStatus("Live stream: " + String(envelope.events.length) + " mesh event(s) applied through " + String(latestEvent.event_type || "control update") + ".");
      }
    }

    function initControlStream() {
      closeControlStream();
      if (typeof window.EventSource !== "function") {
        setStatus("Live stream unavailable in this browser. Using periodic refresh.");
        return;
      }
      const route = ((((app.state || {}).control_stream || {}).route) || "/mesh/control/stream");
      const since = Number(app.stream.cursor || ((((app.state || {}).control_stream || {}).cursor) || 0));
      const source = new EventSource(route + "?since=" + encodeURIComponent(String(since)));
      app.stream.source = source;
      source.addEventListener("control-state", function (event) {
        try {
          const envelope = JSON.parse(event.data || "{}");
          applyControlStreamEnvelope(envelope);
        } catch (error) {
          setStatus("Live stream parse failed: " + error.message);
        }
      });
      source.addEventListener("stream-open", function (event) {
        try {
          const payload = JSON.parse(event.data || "{}");
          if (payload && payload.cursor != null) {
            app.stream.cursor = Number(payload.cursor || app.stream.cursor || 0);
          }
        } catch (error) {
        }
        setStatus("Live mesh stream connected.");
      });
      source.onerror = function () {
        closeControlStream();
        setStatus("Live mesh stream interrupted. Reconnecting...");
        scheduleControlStreamReconnect();
      };
    }

    function manualConnectUrl() {
      const field = document.getElementById("connect-device-url");
      return field ? String(field.value || "").trim() : "";
    }

    function normalizedManualConnectUrl() {
      let token = manualConnectUrl();
      if (!token) {
        return "";
      }
      if (token.indexOf("://") === -1) {
        token = "http://" + token;
      }
      return token.replace(/\\/+$/, "");
    }

    async function scanLocalPeers() {
      return fetchJson("/mesh/discovery/scan-local", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ trust_tier: "trusted", timeout: 0.8, limit: 24 })
      });
    }

    async function connectPeerDevice(options) {
      const payload = Object.assign({ trust_tier: "trusted", timeout: 3.0 }, options || {});
      return fetchJson("/mesh/peers/connect", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
    }

    async function connectAllDevices(options) {
      const payload = Object.assign({ trust_tier: "trusted", timeout: 3.0, scan_timeout: 0.8, limit: 24 }, options || {});
      return fetchJson("/mesh/peers/connect-all", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
    }

    async function launchTestMission(options) {
      const payload = Object.assign({ trust_tier: "trusted", timeout: 3.0 }, options || {});
      return fetchJson("/mesh/missions/test-launch", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
    }

    async function acknowledgeNotification(notificationId) {
      await fetchJson("/mesh/notifications/" + encodeURIComponent(notificationId) + "/ack", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          status: "acked",
          actor_peer_id: app.state.node_id,
          actor_agent_id: "ocp-mobile-ui",
          reason: "phone_control_ack"
        })
      });
    }

    async function resolveApproval(approvalId, decision) {
      await fetchJson("/mesh/approvals/" + encodeURIComponent(approvalId) + "/resolve", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          decision: decision,
          operator_peer_id: app.state.node_id,
          operator_agent_id: "ocp-mobile-ui",
          reason: "phone_control_" + decision
        })
      });
    }

    async function operateJob(action, jobId, queueMessageId) {
      if (action === "resume") {
        await fetchJson("/mesh/jobs/" + encodeURIComponent(jobId) + "/resume", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            operator_id: app.state.node_id + ":ocp-mobile-ui",
            reason: "phone_control_resume_latest"
          })
        });
        return;
      }
      if (action === "restart") {
        await fetchJson("/mesh/jobs/" + encodeURIComponent(jobId) + "/restart", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            operator_id: app.state.node_id + ":ocp-mobile-ui",
            reason: "phone_control_restart"
          })
        });
        return;
      }
      if (action === "replay") {
        await fetchJson("/mesh/queue/replay", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            queue_message_id: queueMessageId,
            reason: "phone_control_replay"
          })
        });
        return;
      }
      if (action === "cancel") {
        await fetchJson("/mesh/jobs/" + encodeURIComponent(jobId) + "/cancel", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            reason: "phone_control_cancel"
          })
        });
      }
    }

    function initActions() {
      document.getElementById("refresh-button").addEventListener("click", function () {
        setStatus("Refreshing cockpit...");
        fetchState().catch(function (error) {
          setStatus("Refresh failed: " + error.message);
        });
      });
      document.getElementById("inspect-close").addEventListener("click", closeInspectOverlay);
      document.getElementById("inspect-backdrop").addEventListener("click", closeInspectOverlay);
      document.addEventListener("keydown", function (event) {
        if (event.key === "Escape") {
          closeInspectOverlay();
        }
      });
      document.addEventListener("click", function (event) {
        const actionButton = event.target.closest("button[data-action]");
        if (actionButton && !actionButton.disabled) {
          const action = actionButton.getAttribute("data-action");
          buttonLoading(actionButton, true);
          if (action === "ack") {
            setStatus("Acknowledging notification...");
            acknowledgeNotification(actionButton.getAttribute("data-notification-id")).then(function () {
              setStatus("Notification acknowledged.");
              return fetchState({ silent: true });
            }).catch(function (error) {
              showError("notifications", "Notification action failed: " + error.message);
              setStatus("Ack failed: " + error.message);
            }).finally(function () {
              buttonLoading(actionButton, false);
            });
            return;
          }
          if (action === "resolve") {
            setStatus("Resolving approval...");
            resolveApproval(actionButton.getAttribute("data-approval-id"), actionButton.getAttribute("data-decision")).then(function () {
              setStatus("Approval updated.");
              return fetchState({ silent: true });
            }).catch(function (error) {
              showError("approvals", "Approval action failed: " + error.message);
              setStatus("Approval failed: " + error.message);
            }).finally(function () {
              buttonLoading(actionButton, false);
            });
            return;
          }
          if (action === "scan-local-peers") {
            setStatus("Scanning nearby nodes...");
            scanLocalPeers().then(function (result) {
              setStatus("Scan complete: " + String(result.connected || 0) + " connected, " + String(result.discovered || 0) + " discovered, " + String(result.errors || 0) + " errors.");
              return fetchState({ silent: true });
            }).catch(function (error) {
              showError("connect", "Nearby scan failed: " + error.message);
              setStatus("Nearby scan failed: " + error.message);
            }).finally(function () {
              buttonLoading(actionButton, false);
            });
            return;
          }
          if (action === "connect-all-peers") {
            setStatus("Connecting everything nearby...");
            connectAllDevices().then(function (result) {
              setStatus(
                "Mesh connect complete: " +
                String(result.connected || 0) + " new, " +
                String(result.already_connected || 0) + " already ready, " +
                String(result.errors || 0) + " problem(s)."
              );
              return fetchState({ silent: true });
            }).catch(function (error) {
              showError("connect", "Connect everything failed: " + error.message);
              setStatus("Connect everything failed: " + error.message);
            }).finally(function () {
              buttonLoading(actionButton, false);
            });
            return;
          }
          if (action === "connect-peer" || action === "connect-peer-manual") {
            const manualUrl = normalizedManualConnectUrl();
            const baseUrl = action === "connect-peer-manual"
              ? manualUrl
              : (actionButton.getAttribute("data-base-url") || manualUrl);
            const peerId = action === "connect-peer-manual"
              ? ""
              : (actionButton.getAttribute("data-peer-id") || "");
            if (!baseUrl && !peerId) {
              buttonLoading(actionButton, false);
              showError("connect", "A device URL is required before connect can run.");
              setStatus("Connect failed: device URL is missing.");
              return;
            }
            setStatus("Connecting device...");
            connectPeerDevice({ base_url: baseUrl, peer_id: peerId }).then(function (result) {
              const peer = result.peer || {};
              setStatus("Connected " + String(peer.display_name || peer.peer_id || baseUrl) + ".");
              return fetchState({ silent: true });
            }).catch(function (error) {
              showError("connect", "Connect failed: " + error.message);
              setStatus("Connect failed: " + error.message);
            }).finally(function () {
              buttonLoading(actionButton, false);
            });
            return;
          }
          if (action === "send-test-mission" || action === "send-test-mission-manual") {
            const manualUrl = normalizedManualConnectUrl();
            const baseUrl = action === "send-test-mission-manual"
              ? manualUrl
              : (actionButton.getAttribute("data-base-url") || manualUrl);
            const peerId = action === "send-test-mission-manual"
              ? ""
              : (actionButton.getAttribute("data-peer-id") || "");
            if (!baseUrl && !peerId) {
              buttonLoading(actionButton, false);
              showError("connect", "A device URL is required before a test mission can run.");
              setStatus("Test mission failed: device URL is missing.");
              return;
            }
            setStatus("Launching test mission...");
            launchTestMission({ base_url: baseUrl, peer_id: peerId }).then(function (result) {
              const mission = result.mission || {};
              setStatus("Test mission launched: " + String(mission.title || mission.id || "mission") + ".");
              return fetchState({ silent: true });
            }).catch(function (error) {
              showError("connect", "Test mission failed: " + error.message);
              setStatus("Test mission failed: " + error.message);
            }).finally(function () {
              buttonLoading(actionButton, false);
            });
            return;
          }
          if (["resume", "restart", "replay", "cancel"].includes(action)) {
            setStatus("Running job control...");
            operateJob(action, actionButton.getAttribute("data-job-id"), actionButton.getAttribute("data-queue-message-id")).then(function () {
              setStatus("Job action complete.");
              return fetchState({ silent: true });
            }).catch(function (error) {
              showError("operations", "Job control failed: " + error.message);
              setStatus("Job action failed: " + error.message);
            }).finally(function () {
              buttonLoading(actionButton, false);
            });
            return;
          }
          if (["mission-cancel", "mission-restart", "mission-resume", "mission-resume-checkpoint"].includes(action)) {
            const missionAction = action === "mission-restart"
              ? "restart"
              : (action === "mission-resume-checkpoint" ? "resume-checkpoint" : (action === "mission-resume" ? "resume" : "cancel"));
            setStatus("Running mission control...");
            operateMission(missionAction, actionButton.getAttribute("data-mission-id")).then(function () {
              setStatus("Mission action complete.");
              return fetchState({ silent: true });
            }).catch(function (error) {
              showError("missions", "Mission control failed: " + error.message);
              setStatus("Mission action failed: " + error.message);
            }).finally(function () {
              buttonLoading(actionButton, false);
            });
            return;
          }
          if (action === "auto-seek-help") {
            setStatus("Seeking helper capacity...");
            fetchJson("/mesh/helpers/auto-seek", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ reason: "phone_control_auto_seek", max_enlist: 2 })
            }).then(function (result) {
              const enlisted = (result && result.enlisted) || [];
              setStatus("Auto-seek complete: " + enlisted.length + " enlisted.");
              return fetchState({ silent: true });
            }).catch(function (error) {
              showError("centerpiece", "Helper request failed: " + error.message);
              setStatus("Auto-seek failed: " + error.message);
            }).finally(function () {
              buttonLoading(actionButton, false);
            });
            return;
          }
          if (action === "run-autonomy") {
            setStatus("Running autonomy pass...");
            fetchJson("/mesh/helpers/autonomy/run", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ actor_agent_id: "ocp-mobile-ui" })
            }).then(function (result) {
              setStatus("Autonomy run: " + String((result && result.status) || "done") + ".");
              return fetchState({ silent: true });
            }).catch(function (error) {
              showError("autonomy", "Autonomy run failed: " + error.message);
              setStatus("Autonomy run failed: " + error.message);
            }).finally(function () {
              buttonLoading(actionButton, false);
            });
            return;
          }
          if (["helper-enlist", "helper-drain", "helper-retire"].includes(action)) {
            const peerId = actionButton.getAttribute("data-peer-id");
            const endpoint = action === "helper-enlist"
              ? "/mesh/helpers/enlist"
              : (action === "helper-drain" ? "/mesh/helpers/drain" : "/mesh/helpers/retire");
            setStatus("Updating helper state...");
            fetchJson(endpoint, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ peer_id: peerId, source: "phone_control" })
            }).then(function () {
              setStatus("Helper action complete for " + peerId + ".");
              return fetchState({ silent: true });
            }).catch(function (error) {
              showError("fleet", "Helper action failed: " + error.message);
              setStatus("Helper action failed: " + error.message);
            }).finally(function () {
              buttonLoading(actionButton, false);
            });
            return;
          }
          if (action === "set-helper-preference") {
            const peerId = actionButton.getAttribute("data-peer-id");
            const preference = actionButton.getAttribute("data-preference");
            const workloadClass = actionButton.getAttribute("data-workload-class") || "default";
            setStatus("Saving helper memory...");
            fetchJson("/mesh/helpers/preferences/set", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                peer_id: peerId,
                workload_class: workloadClass,
                preference: preference,
                source: "control_ui",
                metadata: {
                  note: "Saved from OCP control deck"
                }
              })
            }).then(function () {
              setStatus("Helper memory saved for " + peerId + ".");
              return fetchState({ silent: true });
            }).catch(function (error) {
              showError("fleet", "Saving helper memory failed: " + error.message);
              setStatus("Saving helper memory failed: " + error.message);
            }).finally(function () {
              buttonLoading(actionButton, false);
            });
          }
          return;
        }
        const inspectTarget = event.target.closest("[data-inspect-surface]");
        if (inspectTarget) {
          event.preventDefault();
          openInspectOverlay(
            inspectTarget.getAttribute("data-inspect-surface") || "",
            inspectTarget.getAttribute("data-inspect-id") || "",
            inspectTarget.getAttribute("data-inspect-title") || "",
            inspectTarget.getAttribute("data-inspect-href") || ""
          );
          return;
        }
        const copyLink = event.target.closest("[data-copy-url]");
        if (copyLink) {
          const href = copyLink.getAttribute("data-copy-url");
          if (navigator.clipboard && window.isSecureContext) {
            navigator.clipboard.writeText(window.location.origin + href).then(function () {
              setStatus("Copied " + href + " to clipboard.");
            }).catch(function () {
            });
          }
        }
      });
    }

    function initPolling() {
      if (app.refreshTimer) {
        clearInterval(app.refreshTimer);
      }
      const fallbackSeconds = Number((((app.state || {}).control_stream || {}).fallback_refresh_seconds) || 60);
      app.refreshTimer = setInterval(function () {
        if (document.visibilityState === "visible") {
          fetchState({ silent: true }).catch(function (error) {
            setStatus("Refresh failed: " + error.message);
          });
        }
      }, Math.max(15000, fallbackSeconds * 1000));
    }

    function init() {
      renderJsonSurfaces();
      initMesh();
      initGauges();
      initActions();
      initPolling();
      initControlStream();
      updateSectionReveals();
      renderAll(app.state);
      setStatus("Sovereign mesh cockpit online with live pulse tracking.");
      fetchState({ silent: true }).catch(function (error) {
        setStatus("Refresh failed: " + error.message);
      });
    }

    document.addEventListener("DOMContentLoaded", init);
  </script>
</body>
</html>"""
    return control_html.replace("__OCP_CONTROL_BOOTSTRAP__", bootstrap)


def build_easy_page(mesh: SovereignMesh) -> str:
    initial_state = build_control_state(mesh)
    bootstrap = json.dumps(initial_state).replace("</", "<\\/")
    easy_html = """<!doctype html>
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
      <p class="easy-lead">Open this page on both computers. On one computer, press <strong>Connect Everything</strong> to scan and join every reachable trusted device in one go. Then press <strong>Send Test Mission</strong> to prove real remote execution.</p>
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
            <strong>Press Connect, then Send Test Mission</strong>
            <span>That creates trust, syncs peer state, and launches a proof mission so you can see the mesh actually do work.</span>
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
    const easyApp = {
      state: OCP_EASY_BOOTSTRAP,
      refreshTimer: null
    };

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

    async function fetchJson(url, options) {
      const response = await fetch(url, options);
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
        cards.push({
          key: "peer:" + String(peer.peer_id || ""),
          title: peer.display_name || peer.peer_id || "Connected computer",
          baseUrl: peer.endpoint_url || "",
          peerId: peer.peer_id || "",
          status: peer.status || "connected",
          copy: "This computer is already part of your mesh. You can send a proof mission right now.",
          meta: [
            profile.device_class ? String(profile.device_class).toUpperCase() : "",
            profile.form_factor ? String(profile.form_factor).toUpperCase() : "",
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
        cards.push({
          key: "candidate:" + String(candidate.base_url || candidate.endpoint_url || ""),
          title: candidate.display_name || candidate.peer_id || compactUrl(candidate.endpoint_url || candidate.base_url || "") || "Discovered computer",
          baseUrl: candidate.endpoint_url || candidate.base_url || "",
          peerId: candidate.peer_id || "",
          status: candidate.status || "discovered",
          copy: candidate.last_error
            ? "Last problem: " + String(candidate.last_error)
            : "Discovered and ready for one-click connect.",
          meta: [
            profile.device_class ? String(profile.device_class).toUpperCase() : "",
            profile.form_factor ? String(profile.form_factor).toUpperCase() : "",
            candidate.last_seen_at ? "seen " + relativeTime(candidate.last_seen_at) : ""
          ].filter(Boolean),
          connected: false
        });
      });

      localSummary.innerHTML = [
        connectivity.base_url ? '<span class="easy-pill">' + escapeHtml("Share " + compactUrl(connectivity.base_url)) + '</span>' : "",
        '<span class="easy-pill">' + escapeHtml(String(peers.length) + " connected computer(s)") + '</span>',
        '<span class="easy-pill">' + escapeHtml(String(candidates.length) + " discovered candidate(s)") + '</span>'
      ].filter(Boolean).join("");

      if (shareUrl) {
        shareUrl.textContent = easyRootUrl(connectivity.base_url);
      }
      renderEasyQr(easyRootUrl(connectivity.base_url));

      localAddresses.innerHTML = [
        connectivity.base_url ? '<span class="easy-pill">' + escapeHtml("Advertised " + compactUrl(connectivity.base_url)) + '</span>' : "",
        (connectivity.local_ipv4 || []).map(function (item) {
          return '<span class="easy-pill">' + escapeHtml(item) + '</span>';
        }).join("")
      ].filter(Boolean).join("");

      const recentErrors = connectivity.recent_errors || [];
      const checklistItems = [
        "Open this page on both computers and keep both of them on the same Wi-Fi.",
        "Press Scan Nearby first. If the other computer does not appear, copy your Easy Link and paste it into the other computer's manual connect box.",
        recentErrors.length
          ? "A recent connect attempt failed. The most common fix is allowing Python through the firewall on the other computer."
          : "If nothing shows up yet, the other computer may still be starting up or blocked by a firewall."
      ];
      if (!(connectivity.local_ipv4 || []).length) {
        checklistItems.push("This computer does not currently report a local IPv4 address. Check that it is connected to a real local network.");
      }
      checklist.innerHTML = checklistItems.map(function (item) {
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
      setStatus("Connected " + String(peer.display_name || peer.peer_id || "computer") + ".");
      await refreshEasy({ silent: true });
    }

    async function connectEverything(payload) {
      const result = await fetchJson("/mesh/peers/connect-all", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(Object.assign({ trust_tier: "trusted", timeout: 3.0, scan_timeout: 0.8, limit: 24 }, payload || {}))
      });
      setStatus(
        "Mesh connect complete: " +
        String(result.connected || 0) + " new, " +
        String(result.already_connected || 0) + " already ready, " +
        String(result.errors || 0) + " problem(s)."
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
      document.getElementById("copy-share-url").addEventListener("click", function () {
        const connectivity = (easyApp.state || {}).connectivity || {};
        copyText(easyRootUrl(connectivity.base_url)).then(function () {
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
    return easy_html.replace("__OCP_EASY_BOOTSTRAP__", bootstrap)


class OCPHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def handle(self):
        try:
            super().handle()
        except Exception as exc:
            if _is_client_disconnect(exc):
                return
            raise

    def _mesh(self):
        server_obj = getattr(self, "server", None)
        mesh = getattr(server_obj, "mesh", None) or server_context.get("mesh")
        if mesh is None:
            raise RuntimeError("mesh runtime is not configured")
        return mesh

    def _read_json(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length)
        return json.loads(raw.decode("utf-8")) if raw else {}

    def _send_json(self, payload, code: int = 200):
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_html(self, markup: str, code: int = 200):
        raw = str(markup or "").encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _begin_sse(self, *, close_connection: bool = False):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close" if close_connection else "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

    def _write_sse_event(self, event_name: str, payload: dict[str, Any], *, event_id: str = ""):
        if event_id:
            self.wfile.write(f"id: {event_id}\n".encode("utf-8"))
        self.wfile.write(f"event: {event_name}\n".encode("utf-8"))
        for line in json.dumps(payload).splitlines():
            self.wfile.write(f"data: {line}\n".encode("utf-8"))
        self.wfile.write(b"\n")
        self.wfile.flush()

    def _write_sse_comment(self, text: str = "keepalive"):
        self.wfile.write(f": {text}\n\n".encode("utf-8"))
        self.wfile.flush()

    def _handle_control_page(self):
        self._send_html(build_control_page(self._mesh()))

    def _handle_easy_page(self):
        self._send_html(build_easy_page(self._mesh()))

    def _handle_control_stream(self, params):
        mesh = self._mesh()
        header_cursor = 0
        try:
            header_cursor = int(self.headers.get("Last-Event-ID", "0") or 0)
        except Exception:
            header_cursor = 0
        query_cursor = int(params.get("since", ["0"])[0] or 0)
        if query_cursor <= 0 and header_cursor <= 0:
            cursor = _latest_event_cursor(mesh)
        else:
            cursor = max(query_cursor, header_cursor, 0)
        limit = max(1, int(params.get("limit", ["50"])[0] or 50))
        once = params.get("once", ["0"])[0] in {"1", "true", "yes"}
        heartbeat_seconds = max(2.0, float(params.get("heartbeat", ["10"])[0] or 10.0))
        try:
            self._begin_sse(close_connection=once)
            opened = {"status": "ok", "cursor": cursor, "route": "/mesh/control/stream"}
            self._write_sse_event("stream-open", opened, event_id=str(cursor))
            snapshot = mesh.stream_snapshot(since_seq=cursor, limit=limit)
            envelope = build_control_stream_payload(mesh, since_seq=cursor, limit=limit, snapshot=snapshot)
            cursor = int(envelope.get("cursor") or cursor)
            self._write_sse_event("control-state", envelope, event_id=str(cursor))
            if once:
                self.close_connection = True
                return
            last_keepalive = time.monotonic()
            while True:
                time.sleep(1.0)
                snapshot = mesh.stream_snapshot(since_seq=cursor, limit=limit)
                events = list(snapshot.get("events") or [])
                if events:
                    envelope = build_control_stream_payload(mesh, since_seq=cursor, limit=limit, snapshot=snapshot)
                    next_cursor = int(envelope.get("cursor") or cursor)
                    cursor = next_cursor
                    self._write_sse_event("control-state", envelope, event_id=str(cursor))
                    last_keepalive = time.monotonic()
                    continue
                if time.monotonic() - last_keepalive >= heartbeat_seconds:
                    self._write_sse_comment()
                    last_keepalive = time.monotonic()
        except (BrokenPipeError, ConnectionResetError):
            return

    def _handle_mesh_manifest(self):
        self._send_json(self._mesh().get_manifest())

    def _handle_mesh_device_profile(self):
        self._send_json({"status": "ok", "device_profile": dict(self._mesh().device_profile)})

    def _handle_mesh_device_profile_update(self, data):
        self._send_json(self._mesh().update_device_profile(dict(data.get("device_profile") or {})))

    def _handle_mesh_peers(self, params):
        self._send_json(self._mesh().list_peers(limit=int(params.get("limit", ["25"])[0])))

    def _handle_mesh_peers_sync(self, data):
        self._send_json(
            self._mesh().sync_peer(
                (data.get("peer_id") or "").strip(),
                limit=int(data.get("limit") or 50),
            )
        )

    def _handle_mesh_discovery_candidates(self, params):
        self._send_json(
            self._mesh().list_discovery_candidates(
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
            )
        )

    def _handle_mesh_discovery_seek(self, data):
        self._send_json(
            self._mesh().seek_peers(
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
        )

    def _handle_mesh_discovery_scan_local(self, data):
        self._send_json(
            self._mesh().scan_local_peers(
                trust_tier=(data.get("trust_tier") or "trusted").strip(),
                timeout=float(data.get("timeout") or 0.8),
                limit=int(data.get("limit") or 24),
                port=int(data.get("port") or 0),
            )
        )

    def _handle_mesh_connectivity_diagnostics(self):
        self._send_json(self._mesh().connectivity_diagnostics(limit=24))

    def _handle_mesh_peers_connect(self, data):
        self._send_json(
            self._mesh().connect_device(
                base_url=(data.get("base_url") or "").strip(),
                peer_id=(data.get("peer_id") or "").strip(),
                trust_tier=(data.get("trust_tier") or "trusted").strip(),
                timeout=float(data.get("timeout") or 3.0),
                refresh_manifest=bool(data.get("refresh_manifest", True)),
            )
        )

    def _handle_mesh_peers_connect_all(self, data):
        self._send_json(
            self._mesh().connect_all_devices(
                trust_tier=(data.get("trust_tier") or "trusted").strip(),
                timeout=float(data.get("timeout") or 3.0),
                scan_timeout=float(data.get("scan_timeout") or 0.8),
                limit=int(data.get("limit") or 24),
                port=int(data.get("port") or 0),
                refresh_manifest=bool(data.get("refresh_manifest", True)),
            )
        )

    def _handle_mesh_stream(self, params):
        self._send_json(
            self._mesh().stream_snapshot(
                since_seq=int(params.get("since", ["0"])[0]),
                limit=int(params.get("limit", ["50"])[0]),
            )
        )

    def _handle_mesh_handshake(self, data):
        self._send_json(self._mesh().accept_handshake(data))

    def _handle_mesh_lease_acquire(self, data):
        self._send_json(self._mesh().acquire_lease(**dict(data or {})))

    def _handle_mesh_lease_heartbeat(self, data):
        self._send_json(self._mesh().heartbeat_lease(**dict(data or {})))

    def _handle_mesh_lease_release(self, data):
        self._send_json(self._mesh().release_lease(**dict(data or {})))

    def _handle_mesh_job_submit(self, data):
        self._send_json(self._mesh().accept_job_submission(data))

    def _handle_mesh_job_schedule(self, data):
        self._send_json(
            self._mesh().schedule_job(
                dict(data.get("job") or {}),
                request_id=(data.get("request_id") or "").strip() or None,
                preferred_peer_id=(data.get("preferred_peer_id") or "").strip(),
                allow_local=bool(data.get("allow_local", True)),
                allow_remote=bool(data.get("allow_remote", True)),
            )
        )

    def _handle_mesh_job_get(self, path: str):
        self._send_json(self._mesh().get_job(path.split("/mesh/jobs/", 1)[1]))

    def _handle_mesh_missions(self, params):
        self._send_json(
            self._mesh().list_missions(
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
            )
        )

    def _handle_mesh_mission_get(self, path: str):
        self._send_json(self._mesh().get_mission(path.split("/mesh/missions/", 1)[1]))

    def _handle_mesh_mission_launch(self, data):
        self._send_json(
            self._mesh().launch_mission(
                title=(data.get("title") or "").strip(),
                intent=(data.get("intent") or "").strip(),
                request_id=(data.get("request_id") or "").strip() or None,
                priority=(data.get("priority") or "normal").strip(),
                workload_class=(data.get("workload_class") or "").strip(),
                target_strategy=(data.get("target_strategy") or "").strip(),
                policy=dict(data.get("policy") or {}),
                continuity=dict(data.get("continuity") or {}),
                metadata=dict(data.get("metadata") or {}),
                job=dict(data.get("job") or {}),
                cooperative_task=dict(data.get("cooperative_task") or {}),
            )
        )

    def _handle_mesh_mission_test_launch(self, data):
        self._send_json(
            self._mesh().launch_test_mission(
                peer_id=(data.get("peer_id") or "").strip(),
                base_url=(data.get("base_url") or "").strip(),
                trust_tier=(data.get("trust_tier") or "trusted").strip(),
                timeout=float(data.get("timeout") or 3.0),
                request_id=(data.get("request_id") or "").strip() or None,
            )
        )

    def _handle_mesh_mission_cancel(self, path: str, data):
        mission_id = path[len("/mesh/missions/"):-len("/cancel")].strip("/")
        self._send_json(
            self._mesh().cancel_mission(
                mission_id,
                operator_id=(data.get("operator_id") or "").strip(),
                reason=(data.get("reason") or "mission_cancelled").strip(),
            )
        )

    def _handle_mesh_mission_resume(self, path: str, data):
        mission_id = path[len("/mesh/missions/"):-len("/resume")].strip("/")
        self._send_json(
            self._mesh().resume_mission(
                mission_id,
                operator_id=(data.get("operator_id") or "").strip(),
                reason=(data.get("reason") or "mission_resume_latest").strip(),
            )
        )

    def _handle_mesh_mission_resume_from_checkpoint(self, path: str, data):
        mission_id = path[len("/mesh/missions/"):-len("/resume-from-checkpoint")].strip("/")
        self._send_json(
            self._mesh().resume_mission_from_checkpoint(
                mission_id,
                operator_id=(data.get("operator_id") or "").strip(),
                reason=(data.get("reason") or "mission_resume_checkpoint").strip(),
                checkpoint_artifact_id=(data.get("checkpoint_artifact_id") or "").strip(),
            )
        )

    def _handle_mesh_mission_restart(self, path: str, data):
        mission_id = path[len("/mesh/missions/"):-len("/restart")].strip("/")
        self._send_json(
            self._mesh().restart_mission(
                mission_id,
                operator_id=(data.get("operator_id") or "").strip(),
                reason=(data.get("reason") or "mission_restart").strip(),
            )
        )

    def _handle_mesh_cooperative_tasks(self, params):
        self._send_json(
            self._mesh().list_cooperative_tasks(
                limit=int(params.get("limit", ["25"])[0]),
                state=params.get("state", [""])[0],
            )
        )

    def _handle_mesh_cooperative_task_get(self, path: str):
        self._send_json(self._mesh().get_cooperative_task(path.split("/mesh/cooperative-tasks/", 1)[1]))

    def _handle_mesh_cooperative_task_launch(self, data):
        self._send_json(
            self._mesh().launch_cooperative_task(
                name=(data.get("name") or "").strip(),
                request_id=(data.get("request_id") or "").strip() or None,
                strategy=(data.get("strategy") or "spread").strip(),
                allow_local=bool(data.get("allow_local", True)),
                allow_remote=bool(data.get("allow_remote", True)),
                target_peer_ids=list(data.get("target_peer_ids") or []),
                base_job=dict(data.get("base_job") or {}),
                shards=list(data.get("shards") or []),
                auto_enlist=bool(data.get("auto_enlist", False)),
            )
        )

    def _handle_mesh_pressure(self):
        self._send_json(self._mesh().mesh_pressure())

    def _handle_mesh_helpers(self, params):
        self._send_json(self._mesh().list_helpers(limit=int(params.get("limit", ["100"])[0])))

    def _handle_mesh_helpers_plan(self, data):
        self._send_json(
            self._mesh().plan_helper_enlistment(
                job=dict(data.get("job") or {}),
                limit=int(data.get("limit") or 6),
            )
        )

    def _handle_mesh_helpers_enlist(self, data):
        self._send_json(
            self._mesh().enlist_helper(
                (data.get("peer_id") or "").strip(),
                mode=(data.get("mode") or "on_demand").strip(),
                role=(data.get("role") or "helper").strip(),
                reason=(data.get("reason") or "operator_enlist").strip(),
                source=(data.get("source") or "operator").strip(),
            )
        )

    def _handle_mesh_helpers_drain(self, data):
        self._send_json(
            self._mesh().drain_helper(
                (data.get("peer_id") or "").strip(),
                drain_reason=(data.get("drain_reason") or data.get("reason") or "operator_drain").strip(),
                source=(data.get("source") or "operator").strip(),
            )
        )

    def _handle_mesh_helpers_retire(self, data):
        self._send_json(
            self._mesh().retire_helper(
                (data.get("peer_id") or "").strip(),
                reason=(data.get("reason") or "operator_retire").strip(),
                source=(data.get("source") or "operator").strip(),
            )
        )

    def _handle_mesh_helpers_auto_seek(self, data):
        self._send_json(
            self._mesh().auto_seek_help(
                job=dict(data.get("job") or {}),
                max_enlist=int(data.get("max_enlist") or 2),
                mode=(data.get("mode") or "on_demand").strip(),
                reason=(data.get("reason") or "auto_pressure").strip(),
                allow_remote_seek=bool(data.get("allow_remote_seek") or False),
                seek_hosts=list(data.get("seek_hosts") or []) or None,
            )
        )

    def _handle_mesh_helpers_preferences(self, params):
        self._send_json(
            self._mesh().list_offload_preferences(
                limit=int(params.get("limit", ["100"])[0]),
                peer_id=params.get("peer_id", [""])[0],
                workload_class=params.get("workload_class", [""])[0],
            )
        )

    def _handle_mesh_helpers_preferences_set(self, data):
        self._send_json(
            self._mesh().set_offload_preference(
                (data.get("peer_id") or "").strip(),
                workload_class=(data.get("workload_class") or "default").strip(),
                preference=(data.get("preference") or "allow").strip(),
                source=(data.get("source") or "operator").strip(),
                metadata=dict(data.get("metadata") or {}),
            )
        )

    def _handle_mesh_helpers_autonomy(self):
        self._send_json(self._mesh().evaluate_autonomous_offload())

    def _handle_mesh_helpers_autonomy_run(self, data):
        self._send_json(
            self._mesh().run_autonomous_offload(
                job=dict(data.get("job") or {}),
                actor_agent_id=(data.get("actor_agent_id") or "ocp-control-ui").strip(),
            )
        )

    def _handle_mesh_job_resume(self, path: str, data):
        job_id = path[len("/mesh/jobs/"):-len("/resume")].strip("/")
        self._send_json(
            self._mesh().resume_job(
                job_id,
                operator_id=(data.get("operator_id") or "").strip(),
                reason=(data.get("reason") or "operator_resume_latest").strip(),
            )
        )

    def _handle_mesh_job_resume_from_checkpoint(self, path: str, data):
        job_id = path[len("/mesh/jobs/"):-len("/resume-from-checkpoint")].strip("/")
        self._send_json(
            self._mesh().resume_job_from_checkpoint(
                job_id,
                checkpoint_artifact_id=(data.get("checkpoint_artifact_id") or "").strip(),
                operator_id=(data.get("operator_id") or "").strip(),
                reason=(data.get("reason") or "operator_resume_checkpoint").strip(),
            )
        )

    def _handle_mesh_job_restart(self, path: str, data):
        job_id = path[len("/mesh/jobs/"):-len("/restart")].strip("/")
        self._send_json(
            self._mesh().restart_job(
                job_id,
                operator_id=(data.get("operator_id") or "").strip(),
                reason=(data.get("reason") or "operator_restart").strip(),
            )
        )

    def _handle_mesh_workers(self, params):
        self._send_json(self._mesh().list_workers(limit=int(params.get("limit", ["25"])[0])))

    def _handle_mesh_notifications(self, params):
        self._send_json(
            self._mesh().list_notifications(
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
                target_peer_id=params.get("target_peer_id", [""])[0],
                target_agent_id=params.get("target_agent_id", [""])[0],
            )
        )

    def _handle_mesh_notification_publish(self, data):
        self._send_json(
            {
                "status": "ok",
                "notification": self._mesh().publish_notification(
                    notification_type=(data.get("notification_type") or "info").strip(),
                    priority=(data.get("priority") or "normal").strip(),
                    title=(data.get("title") or "").strip(),
                    body=(data.get("body") or "").strip(),
                    compact_title=(data.get("compact_title") or "").strip(),
                    compact_body=(data.get("compact_body") or "").strip(),
                    target_peer_id=(data.get("target_peer_id") or "").strip(),
                    target_agent_id=(data.get("target_agent_id") or "").strip(),
                    target_device_classes=list(data.get("target_device_classes") or []),
                    related_job_id=(data.get("related_job_id") or "").strip(),
                    related_approval_id=(data.get("related_approval_id") or "").strip(),
                    metadata=dict(data.get("metadata") or {}),
                ),
            }
        )

    def _handle_mesh_notification_ack(self, path: str, data):
        notification_id = path[len("/mesh/notifications/"):-len("/ack")].strip("/")
        self._send_json(
            {
                "status": "ok",
                "notification": self._mesh().ack_notification(
                    notification_id,
                    status=(data.get("status") or "acked").strip(),
                    actor_peer_id=(data.get("actor_peer_id") or "").strip(),
                    actor_agent_id=(data.get("actor_agent_id") or "").strip(),
                    reason=(data.get("reason") or "").strip(),
                ),
            }
        )

    def _handle_mesh_approvals(self, params):
        self._send_json(
            self._mesh().list_approvals(
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
                target_peer_id=params.get("target_peer_id", [""])[0],
                target_agent_id=params.get("target_agent_id", [""])[0],
            )
        )

    def _handle_mesh_approval_request(self, data):
        self._send_json(
            self._mesh().create_approval_request(
                title=(data.get("title") or "").strip(),
                summary=(data.get("summary") or "").strip(),
                action_type=(data.get("action_type") or "operator_action").strip(),
                severity=(data.get("severity") or "normal").strip(),
                request_id=(data.get("request_id") or "").strip(),
                requested_by_peer_id=(data.get("requested_by_peer_id") or "").strip(),
                requested_by_agent_id=(data.get("requested_by_agent_id") or "").strip(),
                target_peer_id=(data.get("target_peer_id") or "").strip(),
                target_agent_id=(data.get("target_agent_id") or "").strip(),
                target_device_classes=list(data.get("target_device_classes") or []),
                related_job_id=(data.get("related_job_id") or "").strip(),
                expires_at=(data.get("expires_at") or "").strip(),
                metadata=dict(data.get("metadata") or {}),
            )
        )

    def _handle_mesh_approval_resolve(self, path: str, data):
        approval_id = path[len("/mesh/approvals/"):-len("/resolve")].strip("/")
        self._send_json(
            self._mesh().resolve_approval(
                approval_id,
                decision=(data.get("decision") or "").strip(),
                operator_peer_id=(data.get("operator_peer_id") or "").strip(),
                operator_agent_id=(data.get("operator_agent_id") or "").strip(),
                reason=(data.get("reason") or "").strip(),
                metadata=dict(data.get("metadata") or {}),
            )
        )

    def _handle_mesh_secrets(self, params):
        self._send_json(
            self._mesh().list_secrets(
                limit=int(params.get("limit", ["25"])[0]),
                scope=params.get("scope", [""])[0],
            )
        )

    def _handle_mesh_secret_put(self, data):
        self._send_json(
            {
                "status": "ok",
                "secret": self._mesh().put_secret(
                    (data.get("name") or "").strip(),
                    data.get("value"),
                    scope=(data.get("scope") or "").strip(),
                    metadata=dict(data.get("metadata") or {}),
                ),
            }
        )

    def _handle_mesh_queue(self, params):
        self._send_json(
            self._mesh().list_queue_messages(
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
            )
        )

    def _handle_mesh_queue_events(self, params):
        self._send_json(
            self._mesh().list_queue_events(
                since_seq=int(params.get("since", ["0"])[0]),
                limit=int(params.get("limit", ["50"])[0]),
                queue_message_id=params.get("queue_message_id", [""])[0],
                job_id=params.get("job_id", [""])[0],
            )
        )

    def _handle_mesh_queue_metrics(self):
        self._send_json(self._mesh().queue_metrics())

    def _handle_mesh_queue_replay(self, data):
        self._send_json(
            self._mesh().replay_queue_message(
                queue_message_id=(data.get("queue_message_id") or "").strip(),
                job_id=(data.get("job_id") or "").strip(),
                reason=(data.get("reason") or "operator_replay").strip(),
            )
        )

    def _handle_mesh_queue_ack_deadline(self, data):
        self._send_json(
            {
                "status": "ok",
                "queue_message": self._mesh().set_queue_ack_deadline(
                    queue_message_id=(data.get("queue_message_id") or "").strip(),
                    attempt_id=(data.get("attempt_id") or "").strip(),
                    ttl_seconds=int(data.get("ttl_seconds") or 0),
                    reason=(data.get("reason") or "operator_ack_deadline_update").strip(),
                ),
            }
        )

    def _handle_mesh_scheduler_decisions(self, params):
        self._send_json(
            self._mesh().list_scheduler_decisions(
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
                target_type=params.get("target_type", [""])[0],
            )
        )

    def _handle_mesh_worker_register(self, data):
        self._send_json(
            {
                "status": "ok",
                "worker": self._mesh().register_worker(
                    worker_id=(data.get("worker_id") or "").strip(),
                    agent_id=(data.get("agent_id") or "").strip(),
                    capabilities=list(data.get("capabilities") or []),
                    resources=dict(data.get("resources") or {}),
                    labels=list(data.get("labels") or []),
                    max_concurrent_jobs=int(data.get("max_concurrent_jobs") or 1),
                    metadata=dict(data.get("metadata") or {}),
                    status=(data.get("status") or "active").strip().lower(),
                ),
            }
        )

    def _handle_mesh_worker_heartbeat(self, path: str, data):
        worker_id = path[len("/mesh/workers/"):-len("/heartbeat")].strip("/")
        self._send_json(
            {
                "status": "ok",
                "worker": self._mesh().heartbeat_worker(
                    worker_id,
                    status=(data.get("status") or "").strip(),
                    metadata=dict(data.get("metadata") or {}),
                ),
            }
        )

    def _handle_mesh_worker_poll(self, path: str, data):
        worker_id = path[len("/mesh/workers/"):-len("/poll")].strip("/")
        self._send_json(self._mesh().poll_jobs(worker_id, limit=int(data.get("limit") or 10)))

    def _handle_mesh_worker_claim(self, path: str, data):
        worker_id = path[len("/mesh/workers/"):-len("/claim")].strip("/")
        self._send_json(
            self._mesh().claim_next_job(
                worker_id,
                job_id=(data.get("job_id") or "").strip(),
                ttl_seconds=int(data.get("ttl_seconds") or 0),
            )
        )

    def _handle_mesh_attempt_heartbeat(self, path: str, data):
        attempt_id = path[len("/mesh/jobs/attempts/"):-len("/heartbeat")].strip("/")
        self._send_json(
            {
                "status": "ok",
                "attempt": self._mesh().heartbeat_job_attempt(
                    attempt_id,
                    ttl_seconds=int(data.get("ttl_seconds") or 300),
                    metadata=dict(data.get("metadata") or {}),
                ),
            }
        )

    def _handle_mesh_attempt_complete(self, path: str, data):
        attempt_id = path[len("/mesh/jobs/attempts/"):-len("/complete")].strip("/")
        self._send_json(
            self._mesh().complete_job_attempt(
                attempt_id,
                data.get("result"),
                media_type=(data.get("media_type") or "application/json").strip(),
                executor=(data.get("executor") or "").strip(),
                metadata=dict(data.get("metadata") or {}),
            )
        )

    def _handle_mesh_attempt_fail(self, path: str, data):
        attempt_id = path[len("/mesh/jobs/attempts/"):-len("/fail")].strip("/")
        self._send_json(
            self._mesh().fail_job_attempt(
                attempt_id,
                error=(data.get("error") or "job attempt failed").strip(),
                retryable=bool(data.get("retryable", True)),
                metadata=dict(data.get("metadata") or {}),
            )
        )

    def _handle_mesh_job_cancel(self, path: str, data):
        job_id = path[len("/mesh/jobs/"):-len("/cancel")].strip("/")
        self._send_json({"status": "cancelled", "job": self._mesh().cancel_job(job_id, reason=(data.get("reason") or "").strip())})

    def _handle_mesh_artifact_publish(self, data):
        self._send_json(self._mesh().accept_artifact_publish(data))

    def _handle_mesh_artifact_list(self, params):
        self._send_json(
            self._mesh().list_artifacts(
                limit=int(params.get("limit", ["25"])[0]),
                artifact_kind=params.get("artifact_kind", [""])[0],
                digest=params.get("digest", [""])[0],
                job_id=params.get("job_id", [""])[0],
                attempt_id=params.get("attempt_id", [""])[0],
                parent_artifact_id=params.get("parent_artifact_id", [""])[0],
                owner_peer_id=params.get("owner_peer_id", [""])[0],
                media_type=params.get("media_type", [""])[0],
                retention_class=params.get("retention_class", [""])[0],
            )
        )

    def _handle_mesh_artifact_get(self, path: str, params):
        artifact_id = path.split("/mesh/artifacts/", 1)[1]
        self._send_json(
            self._mesh().get_artifact(
                artifact_id,
                requester_peer_id=params.get("peer_id", [""])[0],
                include_content=params.get("include_content", ["1"])[0] != "0",
            )
        )

    def _handle_mesh_artifact_purge(self, data):
        self._send_json(self._mesh().purge_expired_artifacts(limit=int(data.get("limit") or 100)))

    def _handle_mesh_artifact_replicate(self, data):
        self._send_json(
            self._mesh().replicate_artifact_from_peer(
                (data.get("peer_id") or "").strip(),
                artifact_id=(data.get("artifact_id") or "").strip(),
                digest=(data.get("digest") or "").strip(),
                pin=bool(data.get("pin", False)),
            )
        )

    def _handle_mesh_artifact_replicate_graph(self, data):
        self._send_json(
            self._mesh().replicate_artifact_graph_from_peer(
                (data.get("peer_id") or "").strip(),
                artifact_id=(data.get("artifact_id") or "").strip(),
                digest=(data.get("digest") or "").strip(),
                pin=bool(data.get("pin", False)),
            )
        )

    def _handle_mesh_artifact_pin(self, data):
        self._send_json(
            {
                "status": "ok",
                "artifact": self._mesh().set_artifact_pin(
                    (data.get("artifact_id") or "").strip(),
                    pinned=bool(data.get("pinned", True)),
                    reason=(data.get("reason") or "operator_pin").strip(),
                ),
            }
        )

    def _handle_mesh_artifact_verify_mirror(self, data):
        self._send_json(
            self._mesh().verify_artifact_mirror(
                (data.get("artifact_id") or "").strip(),
                peer_id=(data.get("peer_id") or "").strip(),
                source_artifact_id=(data.get("source_artifact_id") or "").strip(),
                digest=(data.get("digest") or "").strip(),
            )
        )

    def _handle_mesh_handoff(self, data):
        self._send_json(self._mesh().accept_handoff(data))

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)
        try:
            if path in {"/", "/easy"}:
                return self._handle_easy_page()
            if path in {"/control", "/control/mobile"}:
                return self._handle_control_page()
            if path == "/mesh/control/stream":
                return self._handle_control_stream(params)
            if path == "/mesh/manifest":
                return self._handle_mesh_manifest()
            if path == "/mesh/device-profile":
                return self._handle_mesh_device_profile()
            if path == "/mesh/connectivity/diagnostics":
                return self._handle_mesh_connectivity_diagnostics()
            if path == "/mesh/discovery/candidates":
                return self._handle_mesh_discovery_candidates(params)
            if path == "/mesh/peers":
                return self._handle_mesh_peers(params)
            if path == "/mesh/stream":
                return self._handle_mesh_stream(params)
            if path == "/mesh/missions":
                return self._handle_mesh_missions(params)
            if path.startswith("/mesh/missions/"):
                return self._handle_mesh_mission_get(path)
            if path == "/mesh/cooperative-tasks":
                return self._handle_mesh_cooperative_tasks(params)
            if path.startswith("/mesh/cooperative-tasks/"):
                return self._handle_mesh_cooperative_task_get(path)
            if path == "/mesh/pressure":
                return self._handle_mesh_pressure()
            if path == "/mesh/helpers":
                return self._handle_mesh_helpers(params)
            if path == "/mesh/helpers/preferences":
                return self._handle_mesh_helpers_preferences(params)
            if path == "/mesh/helpers/autonomy":
                return self._handle_mesh_helpers_autonomy()
            if path == "/mesh/workers":
                return self._handle_mesh_workers(params)
            if path == "/mesh/notifications":
                return self._handle_mesh_notifications(params)
            if path == "/mesh/approvals":
                return self._handle_mesh_approvals(params)
            if path == "/mesh/secrets":
                return self._handle_mesh_secrets(params)
            if path == "/mesh/queue":
                return self._handle_mesh_queue(params)
            if path == "/mesh/queue/events":
                return self._handle_mesh_queue_events(params)
            if path == "/mesh/queue/metrics":
                return self._handle_mesh_queue_metrics()
            if path == "/mesh/scheduler/decisions":
                return self._handle_mesh_scheduler_decisions(params)
            if path == "/mesh/artifacts":
                return self._handle_mesh_artifact_list(params)
            if path.startswith("/mesh/artifacts/"):
                return self._handle_mesh_artifact_get(path, params)
            if path.startswith("/mesh/jobs/"):
                return self._handle_mesh_job_get(path)
            self._send_json({"error": "unknown endpoint"}, 404)
        except Exception as exc:
            self._send_json({"error": str(exc)}, 400)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            data = self._read_json()
            if path == "/mesh/handshake":
                return self._handle_mesh_handshake(data)
            if path == "/mesh/device-profile":
                return self._handle_mesh_device_profile_update(data)
            if path == "/mesh/discovery/seek":
                return self._handle_mesh_discovery_seek(data)
            if path == "/mesh/discovery/scan-local":
                return self._handle_mesh_discovery_scan_local(data)
            if path == "/mesh/peers/connect":
                return self._handle_mesh_peers_connect(data)
            if path == "/mesh/peers/connect-all":
                return self._handle_mesh_peers_connect_all(data)
            if path == "/mesh/peers/sync":
                return self._handle_mesh_peers_sync(data)
            if path == "/mesh/cooperative-tasks/launch":
                return self._handle_mesh_cooperative_task_launch(data)
            if path == "/mesh/helpers/plan":
                return self._handle_mesh_helpers_plan(data)
            if path == "/mesh/helpers/enlist":
                return self._handle_mesh_helpers_enlist(data)
            if path == "/mesh/helpers/drain":
                return self._handle_mesh_helpers_drain(data)
            if path == "/mesh/helpers/retire":
                return self._handle_mesh_helpers_retire(data)
            if path == "/mesh/helpers/auto-seek":
                return self._handle_mesh_helpers_auto_seek(data)
            if path == "/mesh/helpers/preferences/set":
                return self._handle_mesh_helpers_preferences_set(data)
            if path == "/mesh/helpers/autonomy/run":
                return self._handle_mesh_helpers_autonomy_run(data)
            if path == "/mesh/lease/acquire":
                return self._handle_mesh_lease_acquire(data)
            if path == "/mesh/lease/heartbeat":
                return self._handle_mesh_lease_heartbeat(data)
            if path == "/mesh/lease/release":
                return self._handle_mesh_lease_release(data)
            if path == "/mesh/jobs/submit":
                return self._handle_mesh_job_submit(data)
            if path == "/mesh/jobs/schedule":
                return self._handle_mesh_job_schedule(data)
            if path == "/mesh/missions/launch":
                return self._handle_mesh_mission_launch(data)
            if path == "/mesh/missions/test-launch":
                return self._handle_mesh_mission_test_launch(data)
            if path.startswith("/mesh/missions/") and path.endswith("/cancel"):
                return self._handle_mesh_mission_cancel(path, data)
            if path.startswith("/mesh/missions/") and path.endswith("/resume-from-checkpoint"):
                return self._handle_mesh_mission_resume_from_checkpoint(path, data)
            if path.startswith("/mesh/missions/") and path.endswith("/resume"):
                return self._handle_mesh_mission_resume(path, data)
            if path.startswith("/mesh/missions/") and path.endswith("/restart"):
                return self._handle_mesh_mission_restart(path, data)
            if path.startswith("/mesh/jobs/") and path.endswith("/resume-from-checkpoint"):
                return self._handle_mesh_job_resume_from_checkpoint(path, data)
            if path.startswith("/mesh/jobs/") and path.endswith("/resume"):
                return self._handle_mesh_job_resume(path, data)
            if path.startswith("/mesh/jobs/") and path.endswith("/restart"):
                return self._handle_mesh_job_restart(path, data)
            if path.startswith("/mesh/jobs/") and path.endswith("/cancel"):
                return self._handle_mesh_job_cancel(path, data)
            if path == "/mesh/workers/register":
                return self._handle_mesh_worker_register(data)
            if path == "/mesh/notifications/publish":
                return self._handle_mesh_notification_publish(data)
            if path.startswith("/mesh/notifications/") and path.endswith("/ack"):
                return self._handle_mesh_notification_ack(path, data)
            if path == "/mesh/approvals/request":
                return self._handle_mesh_approval_request(data)
            if path.startswith("/mesh/approvals/") and path.endswith("/resolve"):
                return self._handle_mesh_approval_resolve(path, data)
            if path == "/mesh/secrets/put":
                return self._handle_mesh_secret_put(data)
            if path.startswith("/mesh/workers/") and path.endswith("/heartbeat"):
                return self._handle_mesh_worker_heartbeat(path, data)
            if path.startswith("/mesh/workers/") and path.endswith("/poll"):
                return self._handle_mesh_worker_poll(path, data)
            if path.startswith("/mesh/workers/") and path.endswith("/claim"):
                return self._handle_mesh_worker_claim(path, data)
            if path == "/mesh/queue/replay":
                return self._handle_mesh_queue_replay(data)
            if path == "/mesh/queue/ack-deadline":
                return self._handle_mesh_queue_ack_deadline(data)
            if path.startswith("/mesh/jobs/attempts/") and path.endswith("/heartbeat"):
                return self._handle_mesh_attempt_heartbeat(path, data)
            if path.startswith("/mesh/jobs/attempts/") and path.endswith("/complete"):
                return self._handle_mesh_attempt_complete(path, data)
            if path.startswith("/mesh/jobs/attempts/") and path.endswith("/fail"):
                return self._handle_mesh_attempt_fail(path, data)
            if path == "/mesh/artifacts/publish":
                return self._handle_mesh_artifact_publish(data)
            if path == "/mesh/artifacts/replicate":
                return self._handle_mesh_artifact_replicate(data)
            if path == "/mesh/artifacts/replicate-graph":
                return self._handle_mesh_artifact_replicate_graph(data)
            if path == "/mesh/artifacts/pin":
                return self._handle_mesh_artifact_pin(data)
            if path == "/mesh/artifacts/verify-mirror":
                return self._handle_mesh_artifact_verify_mirror(data)
            if path == "/mesh/artifacts/purge":
                return self._handle_mesh_artifact_purge(data)
            if path == "/mesh/agents/handoff":
                return self._handle_mesh_handoff(data)
            self._send_json({"error": "unknown endpoint"}, 404)
        except Exception as exc:
            self._send_json({"error": str(exc)}, 400)


def build_http_server(mesh: SovereignMesh, *, host: str = "127.0.0.1", port: int = 8421) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), OCPHandler)
    httpd.mesh = mesh
    return httpd


def _bootstrap_mesh(args) -> SovereignMesh:
    lattice = OCPStore(db_path=args.db_path)
    registry = OCPRegistry(lattice)
    if args.agent_id:
        lattice.register_agent(
            agent_id=args.agent_id,
            agent_name=args.agent_name or args.agent_id,
            capabilities=["mesh", "worker-runtime"],
            metadata={
                "runtime": "ocp-standalone",
                "role": "controller",
                "scope": "standalone OCP control plane",
                "interface": "http",
            },
        )
        lattice.heartbeat_agent_session(
            args.session_id,
            agent_id=args.agent_id,
            runtime="ocp-standalone",
            current_task="serving /mesh routes",
            status="active",
        )
    mesh = SovereignMesh(
        lattice,
        registry=registry,
        workspace_root=args.workspace_root,
        identity_dir=args.identity_dir,
        display_name=args.display_name,
        node_id=args.node_id,
        base_url=(
            _normalize_base_url(args.base_url.rstrip("/"))
            if args.base_url
            else _preferred_local_base_url(bind_host=args.host, port=args.port)
        ),
        device_profile={
            key: value
            for key, value in {
                "device_class": args.device_class,
                "execution_tier": args.execution_tier,
                "power_profile": args.power_profile,
                "network_profile": args.network_profile,
                "mobility": args.mobility,
                "form_factor": args.form_factor,
            }.items()
            if value is not None
        }
        or None,
    )
    server_context["mesh"] = mesh
    server_context["runtime"] = {"lattice": lattice, "registry": registry}
    server_context["ready"] = True
    return mesh


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the standalone Sovereign Mesh OCP server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8421)
    parser.add_argument("--db-path", default="./ocp.db")
    parser.add_argument("--workspace-root", default=".")
    parser.add_argument("--identity-dir", default="./.mesh")
    parser.add_argument("--node-id", default=None)
    parser.add_argument("--display-name", default="Standalone OCP Organism")
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--device-class", choices=["full", "light", "micro", "relay"], default=None)
    parser.add_argument("--execution-tier", choices=["heavy", "standard", "light", "control", "sensor"], default=None)
    parser.add_argument("--power-profile", choices=["line_powered", "battery", "mixed"], default=None)
    parser.add_argument("--network-profile", choices=["wired", "broadband", "wifi", "metered", "intermittent"], default=None)
    parser.add_argument("--mobility", choices=["fixed", "portable", "mobile", "wearable"], default=None)
    parser.add_argument("--form-factor", choices=["server", "workstation", "laptop", "tablet", "phone", "watch", "relay", "edge"], default=None)
    parser.add_argument("--agent-id", default="ocp-control")
    parser.add_argument("--agent-name", default="OCP Control Plane")
    parser.add_argument("--session-id", default="ocp-control-session")
    args = parser.parse_args(argv)

    mesh = _bootstrap_mesh(args)
    httpd = build_http_server(mesh, host=args.host, port=args.port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
