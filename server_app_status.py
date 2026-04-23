from __future__ import annotations

import datetime as dt
from typing import Any

from mesh import SovereignMesh


def _utcnow() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe(default: Any, func, *args, **kwargs) -> Any:
    try:
        return func(*args, **kwargs)
    except Exception:
        return default


def _node_summary(mesh: SovereignMesh, manifest: dict[str, Any]) -> dict[str, Any]:
    card = dict(manifest.get("organism_card") or {})
    profile = dict(manifest.get("device_profile") or card.get("device_profile") or getattr(mesh, "device_profile", {}) or {})
    return {
        "node_id": card.get("node_id") or card.get("organism_id") or getattr(mesh, "node_id", "ocp-node"),
        "display_name": card.get("display_name") or getattr(mesh, "display_name", "OCP Node"),
        "device_class": profile.get("device_class") or "unknown",
        "form_factor": profile.get("form_factor") or "device",
        "protocol_release": manifest.get("protocol_release") or "0.1",
        "protocol_version": manifest.get("protocol_version") or "",
    }


def _app_urls(mesh: SovereignMesh, connectivity: dict[str, Any]) -> dict[str, Any]:
    base_url = str(connectivity.get("share_url") or getattr(mesh, "base_url", "") or "").rstrip("/")
    lan_urls = [str(url).rstrip("/") for url in list(connectivity.get("lan_urls") or []) if str(url or "").strip()]
    phone_url = (lan_urls or [base_url])[0] if (lan_urls or [base_url]) else ""
    return {
        "base_url": base_url,
        "app_url": f"{base_url}/app" if base_url else "/app",
        "setup_url": f"{base_url}/easy" if base_url else "/easy",
        "control_url": f"{base_url}/control" if base_url else "/control",
        "phone_url": f"{phone_url}/app" if phone_url else "",
        "lan_urls": [f"{url}/app" for url in lan_urls],
        "sharing_mode": connectivity.get("sharing_mode") or "",
        "share_advice": connectivity.get("share_advice") or "",
    }


def _mesh_quality(autonomic: dict[str, Any], peers: dict[str, Any]) -> dict[str, Any]:
    routes = dict(autonomic.get("routes") or {})
    route_count = int(routes.get("count") or 0)
    healthy = int(routes.get("healthy") or 0)
    peer_count = int(peers.get("count") or len(list(peers.get("peers") or [])) or 0)
    if route_count and healthy == route_count:
        status = "strong"
        label = "Mesh strong"
    elif healthy:
        status = "usable"
        label = "Mesh usable"
    elif peer_count:
        status = "attention"
        label = "Mesh needs route repair"
    else:
        status = "isolated"
        label = "Local node ready"
    return {
        "status": status,
        "label": label,
        "peer_count": peer_count,
        "route_count": route_count,
        "healthy_routes": healthy,
        "operator_summary": autonomic.get("operator_summary") or "",
    }


def _latest_proof(missions: dict[str, Any]) -> dict[str, Any]:
    for mission in list(missions.get("missions") or []):
        metadata = dict(mission.get("metadata") or {})
        title = str(mission.get("title") or "")
        if metadata.get("test_mission") or metadata.get("mesh_wide_test") or "mesh test" in title.lower():
            return {
                "status": mission.get("status") or "unknown",
                "mission_id": mission.get("id") or "",
                "request_id": mission.get("request_id") or "",
                "title": title or "Mesh proof",
                "updated_at": mission.get("updated_at") or mission.get("created_at") or "",
                "summary": f"{title or 'Mesh proof'} is {mission.get('status') or 'unknown'}.",
            }
    return {
        "status": "none",
        "mission_id": "",
        "request_id": "",
        "title": "No proof yet",
        "updated_at": "",
        "summary": "Press Activate Autonomic Mesh to run a whole-mesh proof.",
    }


def _pending_approvals(approvals: dict[str, Any]) -> dict[str, Any]:
    items = list(approvals.get("approvals") or [])
    pending = [
        item
        for item in items
        if str(item.get("status") or "pending").strip().lower() in {"pending", "requested", "open"}
    ]
    return {
        "pending_count": len(pending),
        "items": pending[:5],
        "operator_summary": (
            f"{len(pending)} approval(s) need attention." if pending else "No approvals are waiting."
        ),
    }


def _next_actions(
    autonomic: dict[str, Any],
    connectivity: dict[str, Any],
    approvals: dict[str, Any],
    latest_proof: dict[str, Any],
) -> list[str]:
    actions = [str(item) for item in list(autonomic.get("recommended_actions") or []) if str(item or "").strip()]
    pending_count = int(approvals.get("pending_count") or 0)
    if pending_count:
        actions.insert(0, f"Review {pending_count} pending approval(s).")
    if latest_proof.get("status") in {"failed", "needs_attention"}:
        actions.insert(0, "Run Autonomic Mesh again to repair routes and retry the proof.")
    share_advice = str(connectivity.get("share_advice") or "").strip()
    if share_advice and share_advice not in actions:
        actions.append(share_advice)
    if not actions:
        actions.append("Press Activate Autonomic Mesh to discover, repair, enlist, prove, and explain this mesh.")
    return actions[:5]


def build_app_status(mesh: SovereignMesh) -> dict[str, Any]:
    manifest = dict(_safe({}, mesh.get_manifest))
    autonomic = dict(
        _safe(
            {
                "status": "unknown",
                "operator_summary": "Autonomic Mesh status is not available yet.",
                "routes": {"routes": [], "count": 0, "healthy": 0},
                "recommended_actions": [],
            },
            mesh.autonomy_status,
        )
    )
    connectivity = dict(_safe({"status": "unknown", "lan_urls": [], "share_advice": ""}, mesh.connectivity_diagnostics, limit=24))
    peers = dict(_safe({"peers": [], "count": 0}, mesh.list_peers, limit=12))
    missions = dict(_safe({"missions": []}, mesh.list_missions, limit=12))
    approvals_raw = dict(_safe({"approvals": []}, mesh.list_approvals, limit=12, status="pending"))
    latest_proof = _latest_proof(missions)
    approvals = _pending_approvals(approvals_raw)
    return {
        "status": "ok",
        "node": _node_summary(mesh, manifest),
        "app_urls": _app_urls(mesh, connectivity),
        "mesh_quality": _mesh_quality(autonomic, peers),
        "autonomy": {
            "status": autonomic.get("status") or "unknown",
            "mode": autonomic.get("mode") or "assisted",
            "operator_summary": autonomic.get("operator_summary") or "",
            "last_run": autonomic.get("last_run") or {},
        },
        "route_health": autonomic.get("routes") or {"routes": [], "count": 0, "healthy": 0},
        "latest_proof": latest_proof,
        "approvals": approvals,
        "next_actions": _next_actions(autonomic, connectivity, approvals, latest_proof),
        "generated_at": _utcnow(),
    }


__all__ = ["build_app_status"]
