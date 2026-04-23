from __future__ import annotations

import datetime as dt
import os
from typing import Any

from mesh import SovereignMesh
from mesh_protocol import SCHEMA_VERSION


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
            result_ref = dict(mission.get("result_ref") or {})
            result_bundle_ref = dict(mission.get("result_bundle_ref") or {})
            return {
                "status": mission.get("status") or "unknown",
                "mission_id": mission.get("id") or "",
                "request_id": mission.get("request_id") or "",
                "title": title or "Mesh proof",
                "updated_at": mission.get("updated_at") or mission.get("created_at") or "",
                "origin_peer_id": mission.get("origin_peer_id") or "",
                "result_ref": result_ref,
                "result_bundle_ref": result_bundle_ref,
                "artifact_id": result_bundle_ref.get("id") or result_bundle_ref.get("artifact_id") or result_ref.get("id") or result_ref.get("artifact_id") or "",
                "digest": result_bundle_ref.get("digest") or result_ref.get("digest") or "",
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


def _setup_timeline(
    *,
    autonomy: dict[str, Any],
    route_health: dict[str, Any],
    latest_proof: dict[str, Any],
    workers: dict[str, Any],
    artifact_sync: dict[str, Any],
) -> list[dict[str, Any]]:
    timeline: list[dict[str, Any]] = []
    last_run = dict((autonomy or {}).get("last_run") or {})
    for action in list(last_run.get("actions") or [])[-12:]:
        action = dict(action or {})
        kind = str(action.get("kind") or "").strip()
        if not kind:
            continue
        timeline.append(
            {
                "kind": kind,
                "status": str(action.get("status") or "info").strip() or "info",
                "summary": str(action.get("summary") or kind.replace("_", " ")).strip(),
                "peer_id": str(action.get("peer_id") or "").strip(),
                "created_at": str(action.get("created_at") or "").strip(),
                "details": dict(action.get("details") or {}),
            }
        )

    if not any(item["kind"] == "route_verified" for item in timeline) and int(route_health.get("healthy") or 0):
        timeline.append(
            {
                "kind": "route_verified",
                "status": "ok",
                "summary": f"{int(route_health.get('healthy') or 0)} peer route(s) are fresh and reachable.",
                "peer_id": "",
                "created_at": "",
                "details": {"healthy": int(route_health.get("healthy") or 0)},
            }
        )
    if not any(item["kind"] == "worker_ready" for item in timeline) and int(workers.get("count") or 0):
        timeline.append(
            {
                "kind": "worker_ready",
                "status": "ok",
                "summary": f"{int(workers.get('count') or 0)} local worker(s) are advertising capacity.",
                "peer_id": "",
                "created_at": "",
                "details": {"count": int(workers.get("count") or 0)},
            }
        )
    if not any(item["kind"] == "artifact_verified" for item in timeline) and int(artifact_sync.get("verified_count") or 0):
        timeline.append(
            {
                "kind": "artifact_verified",
                "status": "ok",
                "summary": f"{int(artifact_sync.get('verified_count') or 0)} replicated artifact(s) have mirror verification.",
                "peer_id": "",
                "created_at": str(artifact_sync.get("latest_synced_at") or ""),
                "details": {"verified_count": int(artifact_sync.get("verified_count") or 0)},
            }
        )
    if latest_proof.get("status") == "completed" and not any(item["kind"] == "proof_completed" for item in timeline):
        timeline.append(
            {
                "kind": "proof_completed",
                "status": "ok",
                "summary": latest_proof.get("summary") or "Whole-mesh proof completed.",
                "peer_id": "",
                "created_at": str(latest_proof.get("updated_at") or ""),
                "details": {"mission_id": latest_proof.get("mission_id") or ""},
            }
        )
    if not timeline:
        timeline.append(
            {
                "kind": "setup_checked",
                "status": "ready",
                "summary": "Setup Doctor is ready. Press Activate Mesh to discover, repair, enlist, and prove.",
                "peer_id": "",
                "created_at": "",
                "details": {},
            }
        )
    return timeline[-8:]


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


def _configured_operator_token() -> bool:
    return bool((os.environ.get("OCP_OPERATOR_TOKEN") or os.environ.get("OCP_CONTROL_TOKEN") or "").strip())


def _route_fix(routes: dict[str, Any]) -> str:
    for route in list(routes.get("routes") or []):
        route = dict(route or {})
        status = str(route.get("status") or "").strip().lower()
        freshness = str(route.get("freshness") or "").strip().lower()
        if status != "reachable" or freshness in {"stale", "failed"}:
            return str(
                route.get("operator_hint")
                or route.get("operator_summary")
                or "Press Activate Mesh to probe and repair this peer route."
            )
    return "Press Activate Mesh to probe and repair peer routes."


def _route_issue_text(route: dict[str, Any]) -> str:
    return " ".join(
        str(route.get(key) or "")
        for key in ("operator_hint", "operator_summary", "last_error")
    ).lower()


def _route_has_identity_change(route: dict[str, Any]) -> bool:
    text = _route_issue_text(route)
    return "different ocp node" in text or ("route reached" in text and "expected" in text)


def _route_has_firewall_hint(route: dict[str, Any]) -> bool:
    text = _route_issue_text(route)
    return any(
        token in text
        for token in (
            "firewall",
            "allow python",
            "allow ocp",
            "connection refused",
            "actively refused",
            "timed out",
            "timeout",
        )
    )


def _recovery_state(*, status: str, proof_status: str, last_run: dict[str, Any]) -> str:
    run_status = str(last_run.get("status") or "").strip().lower()
    action_kinds = {str(action.get("kind") or "").strip() for action in list(last_run.get("actions") or [])}
    if run_status == "running" or proof_status in {"planned", "queued", "running", "accepted"}:
        return "repairing"
    if status == "needs_attention":
        return "needs_attention"
    if {"route_repaired", "peer_synced"} & action_kinds:
        return "repaired"
    return "healthy"


def _primary_peer(route_health: dict[str, Any], execution_readiness: dict[str, Any], artifact_sync: dict[str, Any]) -> dict[str, Any]:
    routes = [dict(route or {}) for route in list(route_health.get("routes") or [])]
    targets = [dict(target or {}) for target in list(execution_readiness.get("targets") or [])]
    artifact_sources = {
        str(item.get("source_peer_id") or "").strip()
        for item in list(artifact_sync.get("items") or [])
        if str(item.get("source_peer_id") or "").strip()
    }
    route_by_peer = {
        str(route.get("peer_id") or "").strip(): route
        for route in routes
        if str(route.get("peer_id") or "").strip()
    }

    def build(peer_id: str, display_name: str, *, role: str, status: str, summary: str, route: str = "") -> dict[str, Any]:
        return {
            "peer_id": peer_id,
            "display_name": display_name or peer_id,
            "role": role,
            "status": status,
            "route": route,
            "summary": summary,
        }

    for target in targets:
        peer_id = str(target.get("peer_id") or "").strip()
        if not peer_id or str(target.get("role") or "").strip().lower() == "local":
            continue
        route = route_by_peer.get(peer_id, {})
        freshness = str(route.get("freshness") or target.get("route_freshness") or "").strip().lower()
        target_status = str(target.get("status") or "").strip().lower()
        if target_status == "ready" and freshness in {"fresh", "aging", ""}:
            display_name = str(target.get("display_name") or peer_id).strip()
            route_url = str(route.get("best_route") or "").strip()
            summary = f"{display_name} is best for compute right now."
            if peer_id in artifact_sources:
                summary = f"{display_name} is ready for compute and holds proof artifacts."
            return build(peer_id, display_name, role="compute", status=target_status or "ready", summary=summary, route=route_url)

    for route in routes:
        peer_id = str(route.get("peer_id") or "").strip()
        freshness = str(route.get("freshness") or "").strip().lower()
        status = str(route.get("status") or "").strip().lower()
        if not peer_id or status != "reachable" or freshness not in {"fresh", "aging"}:
            continue
        display_name = str(route.get("display_name") or peer_id).strip()
        role = "artifact_source" if peer_id in artifact_sources else "peer"
        summary = (
            f"{display_name} is the current artifact source."
            if role == "artifact_source"
            else f"{display_name} is reachable and ready for the demo."
        )
        return build(peer_id, display_name, role=role, status=status or "reachable", summary=summary, route=str(route.get("best_route") or "").strip())

    for route in routes[:1]:
        peer_id = str(route.get("peer_id") or "").strip()
        if not peer_id:
            continue
        display_name = str(route.get("display_name") or peer_id).strip()
        return build(
            peer_id,
            display_name,
            role="peer",
            status=str(route.get("status") or "unknown").strip() or "unknown",
            summary=str(route.get("operator_summary") or f"{display_name} is the clearest peer OCP can see right now.").strip(),
            route=str(route.get("best_route") or "").strip(),
        )

    return {}


def _device_roles(
    *,
    node: dict[str, Any],
    execution_readiness: dict[str, Any],
    artifact_sync: dict[str, Any],
    approvals: dict[str, Any],
    route_health: dict[str, Any],
) -> list[dict[str, Any]]:
    roles: list[dict[str, Any]] = []
    seen: set[str] = set()

    def append(peer_id: str, display_name: str, role: str, status: str, summary: str) -> None:
        peer_token = str(peer_id or "").strip()
        if not peer_token or peer_token in seen:
            return
        seen.add(peer_token)
        roles.append(
            {
                "peer_id": peer_token,
                "display_name": str(display_name or peer_token).strip() or peer_token,
                "role": role,
                "status": str(status or "unknown").strip() or "unknown",
                "summary": str(summary or "").strip(),
            }
        )

    local_id = str(node.get("node_id") or "local").strip() or "local"
    append(
        local_id,
        str(node.get("display_name") or "This Mac").strip() or "This Mac",
        "local_command",
        "ready",
        "This Mac is the local command node.",
    )

    for target in list(execution_readiness.get("targets") or []):
        target = dict(target or {})
        peer_id = str(target.get("peer_id") or "").strip()
        if not peer_id or peer_id == local_id or str(target.get("status") or "").strip().lower() != "ready":
            continue
        append(
            peer_id,
            str(target.get("display_name") or peer_id).strip(),
            "compute",
            "ready",
            f"{str(target.get('display_name') or peer_id).strip()} is ready for compute work.",
        )

    for approval in list(approvals.get("items") or []):
        approval = dict(approval or {})
        metadata = dict(approval.get("metadata") or {})
        peer_id = str(metadata.get("candidate_peer_id") or "").strip()
        if not peer_id:
            continue
        append(
            peer_id,
            str(metadata.get("candidate_display_name") or peer_id).strip(),
            "approval_only",
            str(approval.get("status") or "pending").strip() or "pending",
            f"{str(metadata.get('candidate_display_name') or peer_id).strip()} is waiting for approval before OCP uses it.",
        )

    for item in list(artifact_sync.get("items") or []):
        item = dict(item or {})
        peer_id = str(item.get("source_peer_id") or "").strip()
        if not peer_id:
            continue
        append(
            peer_id,
            peer_id,
            "artifact_source",
            str(item.get("verification_status") or "verified").strip() or "verified",
            f"{peer_id} is holding proof artifacts.",
        )

    for route in list(route_health.get("routes") or []):
        route = dict(route or {})
        peer_id = str(route.get("peer_id") or "").strip()
        if not peer_id:
            continue
        freshness = str(route.get("freshness") or "").strip().lower()
        status = str(route.get("status") or "").strip().lower()
        if status == "reachable" and freshness in {"fresh", "aging"}:
            append(
                peer_id,
                str(route.get("display_name") or peer_id).strip(),
                "route_verified",
                freshness or "reachable",
                f"{str(route.get('display_name') or peer_id).strip()} has a proven route.",
            )

    return roles[:6]


def _blocker_code(
    *,
    sharing_mode: str,
    token_configured: bool,
    peer_count: int,
    route_count: int,
    healthy_routes: int,
    proof_status: str,
    route_health: dict[str, Any],
) -> str:
    routes = [dict(route or {}) for route in list(route_health.get("routes") or [])]
    if proof_status in {"failed", "needs_attention", "cancelled"}:
        return "proof_failed"
    if sharing_mode == "lan" and not token_configured:
        return "token_missing"
    if any(_route_has_identity_change(route) for route in routes):
        return "identity_changed"
    if any(_route_has_firewall_hint(route) for route in routes):
        return "firewall_suspected"
    if route_count and healthy_routes < route_count:
        return "stale_route"
    if sharing_mode == "local" and not peer_count:
        return "local_only"
    return ""


def _setup_story(
    *,
    status: str,
    recovery_state: str,
    blocker_code: str,
    next_fix: str,
    primary_peer: dict[str, Any],
    latest_proof: dict[str, Any],
    pending_approvals: int,
) -> list[str]:
    proof_status = str(latest_proof.get("status") or "none").strip().lower()
    lines: list[str] = []

    if status == "strong":
        lines.append("Mesh is strong.")
    elif recovery_state == "repairing":
        lines.append("OCP is repairing routes and proving the mesh.")
    elif blocker_code == "identity_changed":
        lines.append("A saved peer route now answers as a different OCP node.")
    elif blocker_code == "firewall_suspected":
        lines.append("A peer route looks blocked by firewall or a stopped OCP process.")
    elif blocker_code == "proof_failed":
        lines.append("The latest whole-mesh proof did not complete.")
    elif status == "local_only":
        lines.append("This node is local only right now.")
    else:
        lines.append("Press Activate Mesh to discover, repair, enlist, prove, and explain this mesh.")

    if primary_peer.get("summary"):
        lines.append(str(primary_peer.get("summary") or "").strip())

    if pending_approvals:
        lines.append(f"{pending_approvals} approval(s) need attention.")
    elif proof_status == "completed":
        lines.append("Whole-mesh proof completed.")
    elif proof_status in {"planned", "queued", "running", "accepted"}:
        lines.append("Whole-mesh proof is in flight.")

    if next_fix:
        lines.append(str(next_fix).strip())

    deduped: list[str] = []
    for line in lines:
        if line and line not in deduped:
            deduped.append(line)
    return deduped[:4]


def _setup_projection(
    *,
    node: dict[str, Any],
    mesh_quality: dict[str, Any],
    route_health: dict[str, Any],
    connectivity: dict[str, Any],
    approvals: dict[str, Any],
    latest_proof: dict[str, Any],
    autonomy: dict[str, Any],
    app_urls: dict[str, Any],
    execution_readiness: dict[str, Any],
    artifact_sync: dict[str, Any],
    timeline: list[dict[str, Any]],
) -> dict[str, Any]:
    sharing_mode = str(connectivity.get("sharing_mode") or "").strip().lower()
    token_configured = _configured_operator_token()
    peer_count = int(mesh_quality.get("peer_count") or 0)
    route_count = int(mesh_quality.get("route_count") or 0)
    healthy_routes = int(mesh_quality.get("healthy_routes") or 0)
    proof_status = str(latest_proof.get("status") or "none").strip().lower()
    last_run = dict((autonomy or {}).get("last_run") or {})
    run_status = str(last_run.get("status") or "").strip().lower()
    pending_approvals = int(approvals.get("pending_count") or 0)

    status = "ready"
    label = "Ready for setup"
    blocking_issue = ""
    next_fix = "Press Activate Mesh to discover nearby devices, repair routes, and run a proof."

    if run_status == "running":
        status = "proving"
        label = "Proving mesh"
        next_fix = "Keep this page open while OCP finishes route checks and proof execution."
    elif proof_status == "completed" and route_count and healthy_routes == route_count:
        status = "strong"
        label = "Mesh strong"
        next_fix = "No fix needed. The current mesh proof completed."
    elif proof_status in {"planned", "queued", "running", "accepted"}:
        status = "proving"
        label = "Proof running"
        next_fix = "Wait for the proof mission to finish, then refresh setup status."
    elif proof_status in {"failed", "needs_attention", "cancelled"}:
        status = "needs_attention"
        label = "Proof needs attention"
        blocking_issue = "The latest mesh proof did not complete."
        next_fix = "Press Activate Mesh again so OCP can repair routes and retry once."
    elif pending_approvals:
        status = "needs_attention"
        label = "Approval needed"
        blocking_issue = f"{pending_approvals} approval(s) need review."
        next_fix = "Open Advanced Control and approve or reject the pending helper request."
    elif sharing_mode == "lan" and not token_configured:
        status = "needs_attention"
        label = "LAN token needed"
        blocking_issue = "This node is LAN-reachable but no operator token is configured for phone control."
        next_fix = "Restart with OCP_OPERATOR_TOKEN set, or start Mesh Mode from the Mac launcher."
    elif route_count and healthy_routes < route_count:
        status = "needs_attention"
        label = "Route needs repair"
        blocking_issue = "One or more peer routes are stale or unreachable."
        next_fix = _route_fix(route_health)
    elif sharing_mode == "local" and not peer_count:
        status = "local_only"
        label = "Local only"
        blocking_issue = "This node is only listening on this computer."
        next_fix = "Start Mesh Mode to use your phone or spare laptop on the same Wi-Fi."
    elif route_count and healthy_routes == route_count:
        status = "ready"
        label = "Routes ready"
        next_fix = "Press Activate Mesh to run a whole-mesh proof."

    blocker_code = _blocker_code(
        sharing_mode=sharing_mode,
        token_configured=token_configured,
        peer_count=peer_count,
        route_count=route_count,
        healthy_routes=healthy_routes,
        proof_status=proof_status,
        route_health=route_health,
    )
    recovery_state = _recovery_state(status=status, proof_status=proof_status, last_run=last_run)
    primary_peer = _primary_peer(route_health, execution_readiness, artifact_sync)
    device_roles = _device_roles(
        node=node,
        execution_readiness=execution_readiness,
        artifact_sync=artifact_sync,
        approvals=approvals,
        route_health=route_health,
    )
    story = _setup_story(
        status=status,
        recovery_state=recovery_state,
        blocker_code=blocker_code,
        next_fix=next_fix,
        primary_peer=primary_peer,
        latest_proof=latest_proof,
        pending_approvals=pending_approvals,
    )

    if status == "strong":
        operator_summary = "Mesh is strong. Devices have proven routes and the latest proof completed."
    elif blocking_issue:
        operator_summary = f"{blocking_issue} {next_fix}"
    else:
        operator_summary = next_fix

    return {
        "status": status,
        "label": label,
        "primary_action": "activate_mesh",
        "bind_mode": sharing_mode or "unknown",
        "phone_url": app_urls.get("phone_url") or app_urls.get("app_url") or "",
        "token_status": "configured" if token_configured else "loopback_only",
        "known_peer_count": peer_count,
        "healthy_route_count": healthy_routes,
        "route_count": route_count,
        "latest_proof_status": proof_status,
        "recovery_state": recovery_state,
        "primary_peer": primary_peer,
        "device_roles": device_roles,
        "blocking_issue": blocking_issue,
        "blocker_code": blocker_code,
        "next_fix": next_fix,
        "operator_summary": operator_summary,
        "story": story,
        "timeline": timeline,
    }


def _worker_capacity(worker: dict[str, Any]) -> dict[str, Any]:
    max_jobs = max(1, int(worker.get("max_concurrent_jobs") or 1))
    active = max(0, int(worker.get("active_attempts") or 0))
    available = max(0, max_jobs - active)
    return {
        "worker_id": worker.get("id") or "",
        "peer_id": worker.get("peer_id") or "",
        "status": worker.get("status") or "unknown",
        "capabilities": list(worker.get("capabilities") or []),
        "resources": dict(worker.get("resources") or {}),
        "max_concurrent_jobs": max_jobs,
        "available_slots": available,
        "operator_summary": (
            f"{worker.get('id') or 'Worker'} has {available} available slot(s)."
            if str(worker.get("status") or "").lower() in {"active", "ready"}
            else f"{worker.get('id') or 'Worker'} is {worker.get('status') or 'unknown'}."
        ),
    }


def _execution_readiness(
    *,
    mesh: SovereignMesh,
    workers: dict[str, Any],
    peers: dict[str, Any],
    route_health: dict[str, Any],
) -> dict[str, Any]:
    local_workers = [_worker_capacity(dict(worker or {})) for worker in list(workers.get("workers") or [])]
    local_ready = [worker for worker in local_workers if worker["status"] in {"active", "ready"} and worker["available_slots"] > 0]
    route_by_peer = {
        str(route.get("peer_id") or "").strip(): dict(route or {})
        for route in list(route_health.get("routes") or [])
        if str(route.get("peer_id") or "").strip()
    }
    targets = [
        {
            "peer_id": getattr(mesh, "node_id", "local"),
            "display_name": getattr(mesh, "display_name", "This node"),
            "role": "local",
            "status": "ready" if local_ready else "no_worker_capacity",
            "worker_count": len(local_workers),
            "reasons": ["local worker registered"] if local_ready else ["no local worker capacity advertised"],
        }
    ]
    remote_ready = 0
    for peer in list(peers.get("peers") or []):
        peer = dict(peer or {})
        peer_id = str(peer.get("peer_id") or "").strip()
        metadata = dict(peer.get("metadata") or {})
        remote_workers = list(metadata.get("remote_workers") or [])
        route = route_by_peer.get(peer_id, {})
        route_ready = (
            str(route.get("status") or "").lower() == "reachable"
            and str(route.get("freshness") or "").lower() in {"fresh", "aging"}
        )
        worker_ready = any(str(worker.get("status") or "").lower() in {"active", "ready"} for worker in remote_workers)
        if route_ready and worker_ready:
            remote_ready += 1
        reasons = []
        reasons.append("fresh route" if route_ready else "route not proven")
        reasons.append("worker advertised" if worker_ready else "no worker advertised")
        targets.append(
            {
                "peer_id": peer_id,
                "display_name": peer.get("display_name") or peer_id,
                "role": "remote",
                "status": "ready" if route_ready and worker_ready else "needs_attention",
                "worker_count": len(remote_workers),
                "route_status": route.get("status") or "",
                "route_freshness": route.get("freshness") or "",
                "reasons": reasons,
            }
        )
    if local_ready or remote_ready:
        status = "ready"
        summary = f"Execution is ready: {len(local_ready)} local worker(s), {remote_ready} remote peer target(s)."
    elif targets:
        status = "needs_worker"
        summary = "Routes may be present, but no ready worker capacity is advertised yet."
    else:
        status = "isolated"
        summary = "No execution targets are visible yet."
    return {
        "status": status,
        "local": {"worker_count": len(local_workers), "ready_worker_count": len(local_ready)},
        "targets": targets,
        "worker_capacity": local_workers,
        "operator_summary": summary,
    }


def _artifact_sync(mesh: SovereignMesh) -> dict[str, Any]:
    artifacts = dict(_safe({"artifacts": [], "count": 0}, mesh.list_artifacts, limit=25))
    synced = []
    verified_count = 0
    latest_synced_at = ""
    for artifact in list(artifacts.get("artifacts") or []):
        metadata = dict(artifact.get("metadata") or {})
        sync = dict(metadata.get("artifact_sync") or {})
        if not sync:
            continue
        synced.append(
            {
                "artifact_id": artifact.get("id") or "",
                "digest": artifact.get("digest") or "",
                "source_peer_id": sync.get("source_peer_id") or "",
                "verification_status": sync.get("verification_status") or "",
                "pinned": bool(sync.get("pinned") or artifact.get("pinned")),
                "synced_at": sync.get("synced_at") or artifact.get("created_at") or "",
                "remote_auth": sync.get("remote_auth") or {"type": "none", "status": "not_used"},
            }
        )
        if str(sync.get("verification_status") or "").lower() == "verified":
            verified_count += 1
        latest_synced_at = max(latest_synced_at, str(sync.get("synced_at") or ""))
    return {
        "status": "verified" if verified_count else ("none" if not synced else "attention"),
        "replicated_count": len(synced),
        "verified_count": verified_count,
        "latest_synced_at": latest_synced_at,
        "items": synced[:5],
        "operator_summary": (
            f"{verified_count} replicated artifact(s) verified."
            if verified_count
            else "No replicated artifacts have been verified yet."
        ),
    }


def _protocol_status(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "release": manifest.get("protocol_release") or "0.1",
        "version": manifest.get("protocol_version") or "",
        "schema_version": SCHEMA_VERSION,
        "contract_url": "/mesh/contract",
        "operator_summary": "The live /mesh contract includes protocol schemas for app status, route health, execution readiness, and artifact replication.",
    }


def build_app_status(mesh: SovereignMesh) -> dict[str, Any]:
    manifest = dict(_safe({}, mesh.get_manifest))
    node = _node_summary(mesh, manifest)
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
    workers = dict(_safe({"workers": [], "count": 0}, mesh.list_workers, limit=24))
    approvals_raw = dict(_safe({"approvals": []}, mesh.list_approvals, limit=12, status="pending"))
    latest_proof = _latest_proof(missions)
    approvals = _pending_approvals(approvals_raw)
    mesh_quality = _mesh_quality(autonomic, peers)
    app_urls = _app_urls(mesh, connectivity)
    route_health = autonomic.get("routes") or {"routes": [], "count": 0, "healthy": 0}
    artifact_sync = _artifact_sync(mesh)
    execution_readiness = _execution_readiness(
        mesh=mesh,
        workers=workers,
        peers=peers,
        route_health=route_health,
    )
    timeline = _setup_timeline(
        autonomy=autonomic,
        route_health=route_health,
        latest_proof=latest_proof,
        workers=workers,
        artifact_sync=artifact_sync,
    )
    setup = _setup_projection(
        node=node,
        mesh_quality=mesh_quality,
        route_health=route_health,
        connectivity=connectivity,
        approvals=approvals,
        latest_proof=latest_proof,
        autonomy=autonomic,
        app_urls=app_urls,
        execution_readiness=execution_readiness,
        artifact_sync=artifact_sync,
        timeline=timeline,
    )
    return {
        "status": "ok",
        "node": node,
        "app_urls": app_urls,
        "mesh_quality": mesh_quality,
        "protocol": _protocol_status(manifest),
        "setup": setup,
        "autonomy": {
            "status": autonomic.get("status") or "unknown",
            "mode": autonomic.get("mode") or "assisted",
            "operator_summary": autonomic.get("operator_summary") or "",
            "last_run": autonomic.get("last_run") or {},
        },
        "route_health": route_health,
        "execution_readiness": execution_readiness,
        "artifact_sync": artifact_sync,
        "latest_proof": latest_proof,
        "approvals": approvals,
        "next_actions": _next_actions(autonomic, connectivity, approvals, latest_proof),
        "generated_at": _utcnow(),
    }


__all__ = ["build_app_status"]
