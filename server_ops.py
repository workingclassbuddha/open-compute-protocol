from __future__ import annotations

from typing import Any

from mesh import SovereignMesh


def _extract_path_id(path: str, prefix: str, suffix: str = "") -> str:
    token = str(path or "")
    if suffix:
        token = token[: -len(suffix)]
    return token[len(prefix) :].strip("/")


def mesh_pressure(mesh: SovereignMesh) -> dict[str, Any]:
    return mesh.mesh_pressure()


def list_helpers(mesh: SovereignMesh, *, limit: int = 100) -> dict[str, Any]:
    return mesh.list_helpers(limit=limit)


def plan_helper_enlistment(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.plan_helper_enlistment(
        job=dict(data.get("job") or {}),
        limit=int(data.get("limit") or 6),
    )


def enlist_helper(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.enlist_helper(
        (data.get("peer_id") or "").strip(),
        mode=(data.get("mode") or "on_demand").strip(),
        role=(data.get("role") or "helper").strip(),
        reason=(data.get("reason") or "operator_enlist").strip(),
        source=(data.get("source") or "operator").strip(),
    )


def drain_helper(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.drain_helper(
        (data.get("peer_id") or "").strip(),
        drain_reason=(data.get("drain_reason") or data.get("reason") or "operator_drain").strip(),
        source=(data.get("source") or "operator").strip(),
    )


def retire_helper(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.retire_helper(
        (data.get("peer_id") or "").strip(),
        reason=(data.get("reason") or "operator_retire").strip(),
        source=(data.get("source") or "operator").strip(),
    )


def auto_seek_help(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.auto_seek_help(
        job=dict(data.get("job") or {}),
        max_enlist=int(data.get("max_enlist") or 2),
        mode=(data.get("mode") or "on_demand").strip(),
        reason=(data.get("reason") or "auto_pressure").strip(),
        allow_remote_seek=bool(data.get("allow_remote_seek") or False),
        seek_hosts=list(data.get("seek_hosts") or []) or None,
    )


def list_offload_preferences(
    mesh: SovereignMesh,
    *,
    limit: int = 100,
    peer_id: str = "",
    workload_class: str = "",
) -> dict[str, Any]:
    return mesh.list_offload_preferences(
        limit=limit,
        peer_id=peer_id,
        workload_class=workload_class,
    )


def set_offload_preference(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.set_offload_preference(
        (data.get("peer_id") or "").strip(),
        workload_class=(data.get("workload_class") or "default").strip(),
        preference=(data.get("preference") or "allow").strip(),
        source=(data.get("source") or "operator").strip(),
        metadata=dict(data.get("metadata") or {}),
    )


def evaluate_autonomous_offload(mesh: SovereignMesh) -> dict[str, Any]:
    return mesh.evaluate_autonomous_offload()


def run_autonomous_offload(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.run_autonomous_offload(
        job=dict(data.get("job") or {}),
        actor_agent_id=(data.get("actor_agent_id") or "ocp-control-ui").strip(),
    )


def list_workers(mesh: SovereignMesh, *, limit: int = 25) -> dict[str, Any]:
    return mesh.list_workers(limit=limit)


def register_worker(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "ok",
        "worker": mesh.register_worker(
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


def heartbeat_worker(mesh: SovereignMesh, worker_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "ok",
        "worker": mesh.heartbeat_worker(
            str(worker_id or "").strip(),
            status=(data.get("status") or "").strip(),
            metadata=dict(data.get("metadata") or {}),
        ),
    }


def heartbeat_worker_from_path(mesh: SovereignMesh, path: str, data: dict[str, Any]) -> dict[str, Any]:
    return heartbeat_worker(mesh, _extract_path_id(path, "/mesh/workers/", "/heartbeat"), data)


def poll_worker(mesh: SovereignMesh, worker_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.poll_jobs(str(worker_id or "").strip(), limit=int(data.get("limit") or 10))


def poll_worker_from_path(mesh: SovereignMesh, path: str, data: dict[str, Any]) -> dict[str, Any]:
    return poll_worker(mesh, _extract_path_id(path, "/mesh/workers/", "/poll"), data)


def claim_worker_job(mesh: SovereignMesh, worker_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.claim_next_job(
        str(worker_id or "").strip(),
        job_id=(data.get("job_id") or "").strip(),
        ttl_seconds=int(data.get("ttl_seconds") or 0),
    )


def claim_worker_job_from_path(mesh: SovereignMesh, path: str, data: dict[str, Any]) -> dict[str, Any]:
    return claim_worker_job(mesh, _extract_path_id(path, "/mesh/workers/", "/claim"), data)


def list_notifications(
    mesh: SovereignMesh,
    *,
    limit: int = 25,
    status: str = "",
    target_peer_id: str = "",
    target_agent_id: str = "",
) -> dict[str, Any]:
    return mesh.list_notifications(
        limit=limit,
        status=status,
        target_peer_id=target_peer_id,
        target_agent_id=target_agent_id,
    )


def publish_notification(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "ok",
        "notification": mesh.publish_notification(
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


def ack_notification(mesh: SovereignMesh, notification_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "ok",
        "notification": mesh.ack_notification(
            str(notification_id or "").strip(),
            status=(data.get("status") or "acked").strip(),
            actor_peer_id=(data.get("actor_peer_id") or "").strip(),
            actor_agent_id=(data.get("actor_agent_id") or "").strip(),
            reason=(data.get("reason") or "").strip(),
        ),
    }


def ack_notification_from_path(mesh: SovereignMesh, path: str, data: dict[str, Any]) -> dict[str, Any]:
    return ack_notification(mesh, _extract_path_id(path, "/mesh/notifications/", "/ack"), data)


def list_approvals(
    mesh: SovereignMesh,
    *,
    limit: int = 25,
    status: str = "",
    target_peer_id: str = "",
    target_agent_id: str = "",
) -> dict[str, Any]:
    return mesh.list_approvals(
        limit=limit,
        status=status,
        target_peer_id=target_peer_id,
        target_agent_id=target_agent_id,
    )


def create_approval_request(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.create_approval_request(
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


def resolve_approval(mesh: SovereignMesh, approval_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.resolve_approval(
        str(approval_id or "").strip(),
        decision=(data.get("decision") or "").strip(),
        operator_peer_id=(data.get("operator_peer_id") or "").strip(),
        operator_agent_id=(data.get("operator_agent_id") or "").strip(),
        reason=(data.get("reason") or "").strip(),
        metadata=dict(data.get("metadata") or {}),
    )


def resolve_approval_from_path(mesh: SovereignMesh, path: str, data: dict[str, Any]) -> dict[str, Any]:
    return resolve_approval(mesh, _extract_path_id(path, "/mesh/approvals/", "/resolve"), data)


def list_treaties(
    mesh: SovereignMesh,
    *,
    limit: int = 25,
    status: str = "",
    treaty_type: str = "",
) -> dict[str, Any]:
    return mesh.list_treaties(limit=limit, status=status, treaty_type=treaty_type)


def get_treaty(mesh: SovereignMesh, treaty_id: str) -> dict[str, Any]:
    return mesh.get_treaty(str(treaty_id or "").strip())


def get_treaty_from_path(mesh: SovereignMesh, path: str) -> dict[str, Any]:
    return get_treaty(mesh, _extract_path_id(path, "/mesh/treaties/"))


def propose_treaty(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "ok",
        "treaty": mesh.propose_treaty(
            treaty_id=(data.get("treaty_id") or "").strip(),
            title=(data.get("title") or "").strip(),
            summary=(data.get("summary") or "").strip(),
            treaty_type=(data.get("treaty_type") or "continuity").strip(),
            status=(data.get("status") or "active").strip(),
            parties=list(data.get("parties") or []),
            document=dict(data.get("document") or {}),
            metadata=dict(data.get("metadata") or {}),
            created_by_peer_id=(data.get("created_by_peer_id") or "").strip(),
            expires_at=(data.get("expires_at") or "").strip(),
        ),
    }


def audit_treaty_requirements(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.audit_treaty_requirements(
        list(data.get("treaty_requirements") or []),
        operation=(data.get("operation") or "").strip(),
    )


def list_secrets(mesh: SovereignMesh, *, limit: int = 25, scope: str = "") -> dict[str, Any]:
    return mesh.list_secrets(limit=limit, scope=scope)


def put_secret(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "ok",
        "secret": mesh.put_secret(
            (data.get("name") or "").strip(),
            data.get("value"),
            scope=(data.get("scope") or "").strip(),
            metadata=dict(data.get("metadata") or {}),
        ),
    }


def list_queue_messages(mesh: SovereignMesh, *, limit: int = 25, status: str = "") -> dict[str, Any]:
    return mesh.list_queue_messages(limit=limit, status=status)


def list_queue_events(
    mesh: SovereignMesh,
    *,
    since_seq: int = 0,
    limit: int = 50,
    queue_message_id: str = "",
    job_id: str = "",
) -> dict[str, Any]:
    return mesh.list_queue_events(
        since_seq=since_seq,
        limit=limit,
        queue_message_id=queue_message_id,
        job_id=job_id,
    )


def queue_metrics(mesh: SovereignMesh) -> dict[str, Any]:
    return mesh.queue_metrics()


def replay_queue_message(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.replay_queue_message(
        queue_message_id=(data.get("queue_message_id") or "").strip(),
        job_id=(data.get("job_id") or "").strip(),
        reason=(data.get("reason") or "operator_replay").strip(),
    )


def set_queue_ack_deadline(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "ok",
        "queue_message": mesh.set_queue_ack_deadline(
            queue_message_id=(data.get("queue_message_id") or "").strip(),
            attempt_id=(data.get("attempt_id") or "").strip(),
            ttl_seconds=int(data.get("ttl_seconds") or 0),
            reason=(data.get("reason") or "operator_ack_deadline_update").strip(),
        ),
    }


def list_scheduler_decisions(
    mesh: SovereignMesh,
    *,
    limit: int = 25,
    status: str = "",
    target_type: str = "",
) -> dict[str, Any]:
    return mesh.list_scheduler_decisions(limit=limit, status=status, target_type=target_type)


__all__ = [
    "ack_notification",
    "ack_notification_from_path",
    "audit_treaty_requirements",
    "auto_seek_help",
    "claim_worker_job",
    "claim_worker_job_from_path",
    "create_approval_request",
    "drain_helper",
    "enlist_helper",
    "evaluate_autonomous_offload",
    "get_treaty",
    "get_treaty_from_path",
    "heartbeat_worker",
    "heartbeat_worker_from_path",
    "list_approvals",
    "list_helpers",
    "list_notifications",
    "list_offload_preferences",
    "list_queue_events",
    "list_queue_messages",
    "list_scheduler_decisions",
    "list_secrets",
    "list_treaties",
    "list_workers",
    "mesh_pressure",
    "plan_helper_enlistment",
    "poll_worker",
    "poll_worker_from_path",
    "propose_treaty",
    "publish_notification",
    "put_secret",
    "queue_metrics",
    "register_worker",
    "replay_queue_message",
    "resolve_approval",
    "resolve_approval_from_path",
    "retire_helper",
    "run_autonomous_offload",
    "set_offload_preference",
    "set_queue_ack_deadline",
]
