"""
Grouped route tables for the standalone OCP HTTP host.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

QueryParams = Mapping[str, list[str]]


@dataclass(frozen=True)
class RouteSpec:
    handler_name: str
    argument_kind: str = "none"
    path: str | None = None
    prefix: str | None = None
    suffix: str = ""

    def matches(self, request_path: str) -> bool:
        if self.path is not None:
            return request_path == self.path
        if self.prefix is None or not request_path.startswith(self.prefix):
            return False
        if self.suffix and not request_path.endswith(self.suffix):
            return False
        return True


GET_ROUTE_GROUPS: dict[str, tuple[RouteSpec, ...]] = {
    "pages": (
        RouteSpec(path="/", handler_name="_handle_easy_page"),
        RouteSpec(path="/easy", handler_name="_handle_easy_page"),
        RouteSpec(path="/control", handler_name="_handle_control_page"),
        RouteSpec(path="/control/mobile", handler_name="_handle_control_page"),
    ),
    "control": (
        RouteSpec(path="/mesh/control/stream", handler_name="_handle_control_stream", argument_kind="params"),
    ),
    "runtime": (
        RouteSpec(path="/mesh/contract", handler_name="_handle_mesh_contract"),
        RouteSpec(path="/mesh/manifest", handler_name="_handle_mesh_manifest"),
        RouteSpec(path="/mesh/device-profile", handler_name="_handle_mesh_device_profile"),
        RouteSpec(path="/mesh/connectivity/diagnostics", handler_name="_handle_mesh_connectivity_diagnostics"),
        RouteSpec(path="/mesh/discovery/candidates", handler_name="_handle_mesh_discovery_candidates", argument_kind="params"),
        RouteSpec(path="/mesh/peers", handler_name="_handle_mesh_peers", argument_kind="params"),
        RouteSpec(path="/mesh/stream", handler_name="_handle_mesh_stream", argument_kind="params"),
        RouteSpec(prefix="/mesh/jobs/", handler_name="_handle_mesh_job_get", argument_kind="path"),
    ),
    "missions": (
        RouteSpec(path="/mesh/missions", handler_name="_handle_mesh_missions", argument_kind="params"),
        RouteSpec(prefix="/mesh/missions/", suffix="/continuity", handler_name="_handle_mesh_mission_continuity_get", argument_kind="path"),
        RouteSpec(prefix="/mesh/missions/", handler_name="_handle_mesh_mission_get", argument_kind="path"),
        RouteSpec(path="/mesh/cooperative-tasks", handler_name="_handle_mesh_cooperative_tasks", argument_kind="params"),
        RouteSpec(prefix="/mesh/cooperative-tasks/", handler_name="_handle_mesh_cooperative_task_get", argument_kind="path"),
    ),
    "ops": (
        RouteSpec(path="/mesh/pressure", handler_name="_handle_mesh_pressure"),
        RouteSpec(path="/mesh/helpers", handler_name="_handle_mesh_helpers", argument_kind="params"),
        RouteSpec(path="/mesh/helpers/preferences", handler_name="_handle_mesh_helpers_preferences", argument_kind="params"),
        RouteSpec(path="/mesh/helpers/autonomy", handler_name="_handle_mesh_helpers_autonomy"),
        RouteSpec(path="/mesh/workers", handler_name="_handle_mesh_workers", argument_kind="params"),
        RouteSpec(path="/mesh/notifications", handler_name="_handle_mesh_notifications", argument_kind="params"),
        RouteSpec(path="/mesh/approvals", handler_name="_handle_mesh_approvals", argument_kind="params"),
        RouteSpec(path="/mesh/treaties", handler_name="_handle_mesh_treaties", argument_kind="params"),
        RouteSpec(prefix="/mesh/treaties/", handler_name="_handle_mesh_treaty_get", argument_kind="path"),
        RouteSpec(path="/mesh/secrets", handler_name="_handle_mesh_secrets", argument_kind="params"),
        RouteSpec(path="/mesh/queue", handler_name="_handle_mesh_queue", argument_kind="params"),
        RouteSpec(path="/mesh/queue/events", handler_name="_handle_mesh_queue_events", argument_kind="params"),
        RouteSpec(path="/mesh/queue/metrics", handler_name="_handle_mesh_queue_metrics"),
        RouteSpec(path="/mesh/scheduler/decisions", handler_name="_handle_mesh_scheduler_decisions", argument_kind="params"),
    ),
    "artifacts": (
        RouteSpec(path="/mesh/artifacts", handler_name="_handle_mesh_artifact_list", argument_kind="params"),
        RouteSpec(prefix="/mesh/artifacts/", handler_name="_handle_mesh_artifact_get", argument_kind="path_params"),
    ),
}


POST_ROUTE_GROUPS: dict[str, tuple[RouteSpec, ...]] = {
    "runtime": (
        RouteSpec(path="/mesh/handshake", handler_name="_handle_mesh_handshake", argument_kind="data"),
        RouteSpec(path="/mesh/device-profile", handler_name="_handle_mesh_device_profile_update", argument_kind="data"),
        RouteSpec(path="/mesh/discovery/seek", handler_name="_handle_mesh_discovery_seek", argument_kind="data"),
        RouteSpec(path="/mesh/discovery/scan-local", handler_name="_handle_mesh_discovery_scan_local", argument_kind="data"),
        RouteSpec(path="/mesh/peers/connect", handler_name="_handle_mesh_peers_connect", argument_kind="data"),
        RouteSpec(path="/mesh/peers/connect-all", handler_name="_handle_mesh_peers_connect_all", argument_kind="data"),
        RouteSpec(path="/mesh/peers/sync", handler_name="_handle_mesh_peers_sync", argument_kind="data"),
        RouteSpec(path="/mesh/lease/acquire", handler_name="_handle_mesh_lease_acquire", argument_kind="data"),
        RouteSpec(path="/mesh/lease/heartbeat", handler_name="_handle_mesh_lease_heartbeat", argument_kind="data"),
        RouteSpec(path="/mesh/lease/release", handler_name="_handle_mesh_lease_release", argument_kind="data"),
        RouteSpec(prefix="/mesh/workers/", suffix="/heartbeat", handler_name="_handle_mesh_worker_heartbeat", argument_kind="path_data"),
        RouteSpec(prefix="/mesh/workers/", suffix="/poll", handler_name="_handle_mesh_worker_poll", argument_kind="path_data"),
        RouteSpec(prefix="/mesh/workers/", suffix="/claim", handler_name="_handle_mesh_worker_claim", argument_kind="path_data"),
        RouteSpec(prefix="/mesh/jobs/attempts/", suffix="/heartbeat", handler_name="_handle_mesh_attempt_heartbeat", argument_kind="path_data"),
        RouteSpec(prefix="/mesh/jobs/attempts/", suffix="/complete", handler_name="_handle_mesh_attempt_complete", argument_kind="path_data"),
        RouteSpec(prefix="/mesh/jobs/attempts/", suffix="/fail", handler_name="_handle_mesh_attempt_fail", argument_kind="path_data"),
        RouteSpec(path="/mesh/agents/handoff", handler_name="_handle_mesh_handoff", argument_kind="data"),
    ),
    "missions": (
        RouteSpec(path="/mesh/jobs/submit", handler_name="_handle_mesh_job_submit", argument_kind="data"),
        RouteSpec(path="/mesh/jobs/schedule", handler_name="_handle_mesh_job_schedule", argument_kind="data"),
        RouteSpec(path="/mesh/missions/launch", handler_name="_handle_mesh_mission_launch", argument_kind="data"),
        RouteSpec(path="/mesh/missions/test-launch", handler_name="_handle_mesh_mission_test_launch", argument_kind="data"),
        RouteSpec(path="/mesh/missions/test-mesh-launch", handler_name="_handle_mesh_mission_test_mesh_launch", argument_kind="data"),
        RouteSpec(path="/mesh/continuity/vessels/verify", handler_name="_handle_mesh_continuity_vessel_verify", argument_kind="data"),
        RouteSpec(path="/mesh/continuity/vessels/restore-plan", handler_name="_handle_mesh_continuity_restore_plan", argument_kind="data"),
        RouteSpec(prefix="/mesh/missions/", suffix="/continuity/export", handler_name="_handle_mesh_mission_continuity_export", argument_kind="path_data"),
        RouteSpec(prefix="/mesh/missions/", suffix="/cancel", handler_name="_handle_mesh_mission_cancel", argument_kind="path_data"),
        RouteSpec(prefix="/mesh/missions/", suffix="/resume-from-checkpoint", handler_name="_handle_mesh_mission_resume_from_checkpoint", argument_kind="path_data"),
        RouteSpec(prefix="/mesh/missions/", suffix="/resume", handler_name="_handle_mesh_mission_resume", argument_kind="path_data"),
        RouteSpec(prefix="/mesh/missions/", suffix="/restart", handler_name="_handle_mesh_mission_restart", argument_kind="path_data"),
        RouteSpec(prefix="/mesh/jobs/", suffix="/resume-from-checkpoint", handler_name="_handle_mesh_job_resume_from_checkpoint", argument_kind="path_data"),
        RouteSpec(prefix="/mesh/jobs/", suffix="/resume", handler_name="_handle_mesh_job_resume", argument_kind="path_data"),
        RouteSpec(prefix="/mesh/jobs/", suffix="/restart", handler_name="_handle_mesh_job_restart", argument_kind="path_data"),
        RouteSpec(prefix="/mesh/jobs/", suffix="/cancel", handler_name="_handle_mesh_job_cancel", argument_kind="path_data"),
        RouteSpec(path="/mesh/cooperative-tasks/launch", handler_name="_handle_mesh_cooperative_task_launch", argument_kind="data"),
    ),
    "ops": (
        RouteSpec(path="/mesh/helpers/plan", handler_name="_handle_mesh_helpers_plan", argument_kind="data"),
        RouteSpec(path="/mesh/helpers/enlist", handler_name="_handle_mesh_helpers_enlist", argument_kind="data"),
        RouteSpec(path="/mesh/helpers/drain", handler_name="_handle_mesh_helpers_drain", argument_kind="data"),
        RouteSpec(path="/mesh/helpers/retire", handler_name="_handle_mesh_helpers_retire", argument_kind="data"),
        RouteSpec(path="/mesh/helpers/auto-seek", handler_name="_handle_mesh_helpers_auto_seek", argument_kind="data"),
        RouteSpec(path="/mesh/helpers/preferences/set", handler_name="_handle_mesh_helpers_preferences_set", argument_kind="data"),
        RouteSpec(path="/mesh/helpers/autonomy/run", handler_name="_handle_mesh_helpers_autonomy_run", argument_kind="data"),
        RouteSpec(path="/mesh/workers/register", handler_name="_handle_mesh_worker_register", argument_kind="data"),
        RouteSpec(path="/mesh/notifications/publish", handler_name="_handle_mesh_notification_publish", argument_kind="data"),
        RouteSpec(prefix="/mesh/notifications/", suffix="/ack", handler_name="_handle_mesh_notification_ack", argument_kind="path_data"),
        RouteSpec(path="/mesh/approvals/request", handler_name="_handle_mesh_approval_request", argument_kind="data"),
        RouteSpec(prefix="/mesh/approvals/", suffix="/resolve", handler_name="_handle_mesh_approval_resolve", argument_kind="path_data"),
        RouteSpec(path="/mesh/treaties/propose", handler_name="_handle_mesh_treaty_propose", argument_kind="data"),
        RouteSpec(path="/mesh/treaties/audit", handler_name="_handle_mesh_treaty_audit", argument_kind="data"),
        RouteSpec(path="/mesh/secrets/put", handler_name="_handle_mesh_secret_put", argument_kind="data"),
        RouteSpec(path="/mesh/queue/replay", handler_name="_handle_mesh_queue_replay", argument_kind="data"),
        RouteSpec(path="/mesh/queue/ack-deadline", handler_name="_handle_mesh_queue_ack_deadline", argument_kind="data"),
    ),
    "artifacts": (
        RouteSpec(path="/mesh/artifacts/publish", handler_name="_handle_mesh_artifact_publish", argument_kind="data"),
        RouteSpec(path="/mesh/artifacts/replicate", handler_name="_handle_mesh_artifact_replicate", argument_kind="data"),
        RouteSpec(path="/mesh/artifacts/replicate-graph", handler_name="_handle_mesh_artifact_replicate_graph", argument_kind="data"),
        RouteSpec(path="/mesh/artifacts/pin", handler_name="_handle_mesh_artifact_pin", argument_kind="data"),
        RouteSpec(path="/mesh/artifacts/verify-mirror", handler_name="_handle_mesh_artifact_verify_mirror", argument_kind="data"),
        RouteSpec(path="/mesh/artifacts/purge", handler_name="_handle_mesh_artifact_purge", argument_kind="data"),
    ),
}


def _iter_route_specs(route_groups: dict[str, tuple[RouteSpec, ...]]):
    for group in route_groups.values():
        for spec in group:
            yield spec


def resolve_get_route(path: str) -> RouteSpec | None:
    return next((spec for spec in _iter_route_specs(GET_ROUTE_GROUPS) if spec.matches(path)), None)


def resolve_post_route(path: str) -> RouteSpec | None:
    return next((spec for spec in _iter_route_specs(POST_ROUTE_GROUPS) if spec.matches(path)), None)


def _invoke_route(
    handler: Any,
    spec: RouteSpec,
    *,
    path: str,
    params: QueryParams | None = None,
    data: dict[str, Any] | None = None,
) -> None:
    route_handler = getattr(handler, spec.handler_name)
    if spec.argument_kind == "none":
        route_handler()
        return
    if spec.argument_kind == "params":
        route_handler(params or {})
        return
    if spec.argument_kind == "data":
        route_handler(data or {})
        return
    if spec.argument_kind == "path":
        route_handler(path)
        return
    if spec.argument_kind == "path_params":
        route_handler(path, params or {})
        return
    if spec.argument_kind == "path_data":
        route_handler(path, data or {})
        return
    raise ValueError(f"unsupported route argument kind: {spec.argument_kind}")


def dispatch_get(handler: Any, path: str, params: QueryParams) -> bool:
    spec = resolve_get_route(path)
    if spec is None:
        return False
    _invoke_route(handler, spec, path=path, params=params)
    return True


def dispatch_post(handler: Any, path: str, data: dict[str, Any]) -> bool:
    spec = resolve_post_route(path)
    if spec is None:
        return False
    _invoke_route(handler, spec, path=path, data=data)
    return True
