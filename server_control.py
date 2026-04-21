from __future__ import annotations

import json
import time
from typing import Any

from mesh import SovereignMesh


def latest_event_cursor(mesh: SovereignMesh) -> int:
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
        pressure = {
            "pressure": "idle",
            "queued": 0,
            "inflight": 0,
            "total_slots": 0,
            "available_slots": 0,
            "reasons": [],
            "needs_help": False,
        }
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
            "cursor": latest_event_cursor(mesh),
            "transport": "sse",
            "fallback_refresh_seconds": 60,
        },
    }


def control_peer_advisories(state: dict[str, Any]) -> dict[str, Any]:
    peers = list(((state.get("peers") or {}).get("peers") or []))
    candidates = list(((state.get("discovery_candidates") or {}).get("candidates") or []))

    def project(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        projected = []
        for item in items:
            treaty = dict(item.get("treaty_compatibility") or {})
            advisory_state = str(treaty.get("advisory_state") or "limited").strip().lower()
            recommended_action = {
                "full": "Use this peer for treaty-aware continuity and custody review.",
                "advisory": "Use this peer for treaty-aware visibility, but choose a custody-capable peer for protected restores.",
                "limited": "Keep this peer on normal sync until it advertises treaty validation.",
            }.get(advisory_state, "Keep this peer on normal sync until it advertises treaty validation.")
            missing_capabilities = []
            if not treaty.get("shared_treaty_validation"):
                missing_capabilities.append("treaty_validation")
            if not treaty.get("remote_custody_review"):
                missing_capabilities.append("remote_custody_review")
            if treaty.get("remote_custody_review") and not treaty.get("custody_pairing_ready"):
                missing_capabilities.append("local_custody_review")
            display_name = item.get("display_name") or item.get("peer_id") or ""
            operator_summary = (
                f"{display_name or 'Peer'} treaty posture is {advisory_state}. "
                f"{treaty.get('summary') or 'Treaty-aware continuity remains advisory.'}"
            )
            projected.append(
                {
                    "peer_id": str(item.get("peer_id") or "").strip(),
                    "display_name": display_name,
                    "advisory_state": advisory_state,
                    "summary": treaty.get("summary") or "",
                    "operator_summary": item.get("operator_summary") or operator_summary,
                    "recommended_action": item.get("recommended_action") or recommended_action,
                    "missing_capabilities": list(item.get("missing_capabilities") or missing_capabilities),
                    "shared_treaty_validation": bool(treaty.get("shared_treaty_validation")),
                    "remote_custody_review": bool(treaty.get("remote_custody_review")),
                    "custody_pairing_ready": bool(treaty.get("custody_pairing_ready")),
                }
            )
        return projected

    connected = project(peers)
    discovered = project(candidates)
    return {
        "connected": connected,
        "discovered": discovered,
        "counts": {
            "connected": len(connected),
            "discovered": len(discovered),
            "connected_full": sum(1 for item in connected if item["advisory_state"] == "full"),
            "discovered_full": sum(1 for item in discovered if item["advisory_state"] == "full"),
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
        "peer_advisories": control_peer_advisories(state),
        "state": state,
        "generated_at": snapshot.get("generated_at") or "",
    }


def build_control_bootstrap(mesh: SovereignMesh) -> str:
    return json.dumps(build_control_state(mesh)).replace("</", "<\\/")


def handle_control_stream(handler, mesh: SovereignMesh, params: dict[str, list[str]]) -> None:
    header_cursor = 0
    try:
        header_cursor = int(handler.headers.get("Last-Event-ID", "0") or 0)
    except Exception:
        header_cursor = 0
    query_cursor = int(params.get("since", ["0"])[0] or 0)
    if query_cursor <= 0 and header_cursor <= 0:
        cursor = latest_event_cursor(mesh)
    else:
        cursor = max(query_cursor, header_cursor, 0)
    limit = max(1, int(params.get("limit", ["50"])[0] or 50))
    once = params.get("once", ["0"])[0] in {"1", "true", "yes"}
    heartbeat_seconds = max(2.0, float(params.get("heartbeat", ["10"])[0] or 10.0))
    try:
        handler._begin_sse(close_connection=once)
        opened = {"status": "ok", "cursor": cursor, "route": "/mesh/control/stream"}
        handler._write_sse_event("stream-open", opened, event_id=str(cursor))
        snapshot = mesh.stream_snapshot(since_seq=cursor, limit=limit)
        envelope = build_control_stream_payload(mesh, since_seq=cursor, limit=limit, snapshot=snapshot)
        cursor = int(envelope.get("cursor") or cursor)
        handler._write_sse_event("control-state", envelope, event_id=str(cursor))
        if once:
            handler.close_connection = True
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
                handler._write_sse_event("control-state", envelope, event_id=str(cursor))
                last_keepalive = time.monotonic()
                continue
            if time.monotonic() - last_keepalive >= heartbeat_seconds:
                handler._write_sse_comment()
                last_keepalive = time.monotonic()
    except (BrokenPipeError, ConnectionResetError):
        return


__all__ = [
    "build_control_bootstrap",
    "build_control_state",
    "build_control_stream_payload",
    "control_peer_advisories",
    "handle_control_stream",
    "latest_event_cursor",
]
