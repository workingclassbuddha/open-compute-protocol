from __future__ import annotations

from typing import Any

from mesh import SovereignMesh


def _extract_path_id(path: str, prefix: str, suffix: str = "") -> str:
    token = str(path or "")
    if suffix:
        token = token[: -len(suffix)]
    return token[len(prefix) :].strip("/")


def submit_job(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.accept_job_submission(data)


def schedule_job(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.schedule_job(
        dict(data.get("job") or {}),
        request_id=(data.get("request_id") or "").strip() or None,
        preferred_peer_id=(data.get("preferred_peer_id") or "").strip(),
        allow_local=bool(data.get("allow_local", True)),
        allow_remote=bool(data.get("allow_remote", True)),
    )


def get_job(mesh: SovereignMesh, job_id: str) -> dict[str, Any]:
    return mesh.get_job(str(job_id or "").strip())


def get_job_from_path(mesh: SovereignMesh, path: str) -> dict[str, Any]:
    return get_job(mesh, _extract_path_id(path, "/mesh/jobs/"))


def resume_job(mesh: SovereignMesh, job_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.resume_job(
        str(job_id or "").strip(),
        operator_id=(data.get("operator_id") or "").strip(),
        reason=(data.get("reason") or "operator_resume_latest").strip(),
    )


def resume_job_from_checkpoint(mesh: SovereignMesh, job_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.resume_job_from_checkpoint(
        str(job_id or "").strip(),
        checkpoint_artifact_id=(data.get("checkpoint_artifact_id") or "").strip(),
        operator_id=(data.get("operator_id") or "").strip(),
        reason=(data.get("reason") or "operator_resume_checkpoint").strip(),
    )


def restart_job(mesh: SovereignMesh, job_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.restart_job(
        str(job_id or "").strip(),
        operator_id=(data.get("operator_id") or "").strip(),
        reason=(data.get("reason") or "operator_restart").strip(),
    )


def cancel_job(mesh: SovereignMesh, job_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "cancelled",
        "job": mesh.cancel_job(
            str(job_id or "").strip(),
            reason=(data.get("reason") or "").strip(),
        ),
    }


def list_missions(
    mesh: SovereignMesh,
    *,
    limit: int = 25,
    status: str = "",
) -> dict[str, Any]:
    return mesh.list_missions(limit=limit, status=status)


def get_mission(mesh: SovereignMesh, mission_id: str) -> dict[str, Any]:
    return mesh.get_mission(str(mission_id or "").strip())


def get_mission_from_path(mesh: SovereignMesh, path: str) -> dict[str, Any]:
    return get_mission(mesh, _extract_path_id(path, "/mesh/missions/"))


def launch_mission(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.launch_mission(
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


def get_mission_continuity(mesh: SovereignMesh, mission_id: str) -> dict[str, Any]:
    return mesh.get_mission_continuity(str(mission_id or "").strip())


def export_mission_continuity(mesh: SovereignMesh, mission_id: str, data: dict[str, Any]) -> dict[str, Any]:
    raw_dry_run = data.get("dry_run", True)
    dry_run = not (
        isinstance(raw_dry_run, str)
        and raw_dry_run.strip().lower() in {"0", "false", "no", "off"}
    ) and bool(raw_dry_run)
    return mesh.export_mission_continuity_vessel(
        str(mission_id or "").strip(),
        dry_run=dry_run,
        operator_id=(data.get("operator_id") or "").strip(),
        reason=(data.get("reason") or "").strip(),
    )


def verify_continuity_vessel(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.verify_continuity_vessel(
        (data.get("artifact_id") or data.get("vessel_artifact_id") or "").strip()
    )


def plan_continuity_restore(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.plan_continuity_restore(
        (data.get("artifact_id") or data.get("vessel_artifact_id") or "").strip(),
        target_peer_id=(data.get("target_peer_id") or "").strip(),
        operator_id=(data.get("operator_id") or "").strip(),
        reason=(data.get("reason") or "").strip(),
    )


def cancel_mission(mesh: SovereignMesh, mission_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.cancel_mission(
        str(mission_id or "").strip(),
        operator_id=(data.get("operator_id") or "").strip(),
        reason=(data.get("reason") or "mission_cancelled").strip(),
    )


def resume_mission(mesh: SovereignMesh, mission_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.resume_mission(
        str(mission_id or "").strip(),
        operator_id=(data.get("operator_id") or "").strip(),
        reason=(data.get("reason") or "mission_resume_latest").strip(),
    )


def resume_mission_from_checkpoint(mesh: SovereignMesh, mission_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.resume_mission_from_checkpoint(
        str(mission_id or "").strip(),
        operator_id=(data.get("operator_id") or "").strip(),
        reason=(data.get("reason") or "mission_resume_checkpoint").strip(),
        checkpoint_artifact_id=(data.get("checkpoint_artifact_id") or "").strip(),
    )


def restart_mission(mesh: SovereignMesh, mission_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.restart_mission(
        str(mission_id or "").strip(),
        operator_id=(data.get("operator_id") or "").strip(),
        reason=(data.get("reason") or "mission_restart").strip(),
    )


def list_cooperative_tasks(
    mesh: SovereignMesh,
    *,
    limit: int = 25,
    state: str = "",
) -> dict[str, Any]:
    return mesh.list_cooperative_tasks(limit=limit, state=state)


def get_cooperative_task(mesh: SovereignMesh, task_id: str) -> dict[str, Any]:
    return mesh.get_cooperative_task(str(task_id or "").strip())


def get_cooperative_task_from_path(mesh: SovereignMesh, path: str) -> dict[str, Any]:
    return get_cooperative_task(mesh, _extract_path_id(path, "/mesh/cooperative-tasks/"))


def launch_cooperative_task(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.launch_cooperative_task(
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


__all__ = [
    "cancel_job",
    "cancel_mission",
    "export_mission_continuity",
    "get_cooperative_task",
    "get_cooperative_task_from_path",
    "get_job",
    "get_job_from_path",
    "get_mission",
    "get_mission_continuity",
    "get_mission_from_path",
    "launch_cooperative_task",
    "launch_mission",
    "list_cooperative_tasks",
    "list_missions",
    "plan_continuity_restore",
    "restart_job",
    "restart_mission",
    "resume_job",
    "resume_job_from_checkpoint",
    "resume_mission",
    "resume_mission_from_checkpoint",
    "schedule_job",
    "submit_job",
    "verify_continuity_vessel",
]
