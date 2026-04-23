from __future__ import annotations

import hmac
import ipaddress
import os
from typing import Any

from server_app import build_app_manifest as _build_app_manifest, build_app_page as _build_app_page
from server_app_status import build_app_status as _build_app_status
from server_artifacts import (
    get_artifact_from_path as _get_artifact_from_path_impl,
    list_artifacts as _list_artifacts_impl,
    publish_artifact as _publish_artifact_impl,
    purge_expired_artifacts as _purge_expired_artifacts_impl,
    replicate_artifact as _replicate_artifact_impl,
    replicate_artifact_graph as _replicate_artifact_graph_impl,
    set_artifact_pin as _set_artifact_pin_impl,
    verify_artifact_mirror as _verify_artifact_mirror_impl,
)
from server_connect import (
    build_easy_page as _build_easy_page,
    connect_all_peers as _connect_all_peers_impl,
    connect_peer as _connect_peer_impl,
    connectivity_diagnostics as _connectivity_diagnostics_impl,
    launch_mesh_test_mission as _launch_mesh_test_mission_impl,
    launch_test_mission as _launch_test_mission_impl,
    list_discovery_candidates as _list_discovery_candidates_impl,
    scan_local_peers as _scan_local_peers_impl,
    seek_discovery_peers as _seek_discovery_peers_impl,
    sync_peer as _sync_peer_impl,
)
from server_contract import build_contract_snapshot as _build_contract_snapshot, validate_route_request as _validate_route_request
from server_control import handle_control_stream as _handle_control_stream_impl
from server_control_page import build_control_page as _build_control_page
from server_missions import (
    cancel_job as _cancel_job_impl,
    cancel_mission as _cancel_mission_impl,
    export_mission_continuity as _export_mission_continuity_impl,
    get_cooperative_task_from_path as _get_cooperative_task_from_path_impl,
    get_job_from_path as _get_job_from_path_impl,
    get_mission_continuity as _get_mission_continuity_impl,
    get_mission_from_path as _get_mission_from_path_impl,
    launch_cooperative_task as _launch_cooperative_task_impl,
    launch_mission as _launch_mission_impl,
    list_cooperative_tasks as _list_cooperative_tasks_impl,
    list_missions as _list_missions_impl,
    plan_continuity_restore as _plan_continuity_restore_impl,
    restart_job as _restart_job_impl,
    restart_mission as _restart_mission_impl,
    resume_job as _resume_job_impl,
    resume_job_from_checkpoint as _resume_job_from_checkpoint_impl,
    resume_mission as _resume_mission_impl,
    resume_mission_from_checkpoint as _resume_mission_from_checkpoint_impl,
    schedule_job as _schedule_job_impl,
    submit_job as _submit_job_impl,
    verify_continuity_vessel as _verify_continuity_vessel_impl,
)
from server_ops import (
    ack_notification_from_path as _ack_notification_from_path_impl,
    activate_autonomic_mesh as _activate_autonomic_mesh_impl,
    audit_treaty_requirements as _audit_treaty_requirements_impl,
    autonomy_status as _autonomy_status_impl,
    auto_seek_help as _auto_seek_help_impl,
    claim_worker_job_from_path as _claim_worker_job_from_path_impl,
    create_approval_request as _create_approval_request_impl,
    drain_helper as _drain_helper_impl,
    enlist_helper as _enlist_helper_impl,
    evaluate_autonomous_offload as _evaluate_autonomous_offload_impl,
    get_treaty_from_path as _get_treaty_from_path_impl,
    heartbeat_worker_from_path as _heartbeat_worker_from_path_impl,
    list_approvals as _list_approvals_impl,
    list_helpers as _list_helpers_impl,
    list_notifications as _list_notifications_impl,
    list_offload_preferences as _list_offload_preferences_impl,
    list_queue_events as _list_queue_events_impl,
    list_queue_messages as _list_queue_messages_impl,
    list_scheduler_decisions as _list_scheduler_decisions_impl,
    list_secrets as _list_secrets_impl,
    list_treaties as _list_treaties_impl,
    list_workers as _list_workers_impl,
    mesh_pressure as _mesh_pressure_impl,
    plan_helper_enlistment as _plan_helper_enlistment_impl,
    probe_routes as _probe_routes_impl,
    poll_worker_from_path as _poll_worker_from_path_impl,
    propose_treaty as _propose_treaty_impl,
    publish_notification as _publish_notification_impl,
    put_secret as _put_secret_impl,
    queue_metrics as _queue_metrics_impl,
    register_worker as _register_worker_impl,
    replay_queue_message as _replay_queue_message_impl,
    resolve_approval_from_path as _resolve_approval_from_path_impl,
    retire_helper as _retire_helper_impl,
    routes_health as _routes_health_impl,
    run_autonomous_offload as _run_autonomous_offload_impl,
    set_offload_preference as _set_offload_preference_impl,
    set_queue_ack_deadline as _set_queue_ack_deadline_impl,
)
from server_routes import (
    dispatch_get as _dispatch_get_request_impl,
    dispatch_post as _dispatch_post_request_impl,
    resolve_post_route as _resolve_post_route,
)
from server_runtime import (
    accept_handshake as _accept_handshake_impl,
    accept_handoff as _accept_handoff_impl,
    acquire_lease as _acquire_lease_impl,
    complete_attempt_from_path as _complete_attempt_from_path_impl,
    fail_attempt_from_path as _fail_attempt_from_path_impl,
    get_device_profile as _get_device_profile_impl,
    get_manifest as _get_manifest_impl,
    heartbeat_attempt_from_path as _heartbeat_attempt_from_path_impl,
    heartbeat_lease as _heartbeat_lease_impl,
    list_peers as _list_peers_runtime_impl,
    release_lease as _release_lease_impl,
    stream_snapshot as _stream_snapshot_impl,
    update_device_profile as _update_device_profile_impl,
)


def _path_token(path: str, prefix: str, suffix: str = "") -> str:
    token = str(path or "")
    if suffix:
        token = token[: -len(suffix)]
    return token[len(prefix) :].strip("/")


_SIGNED_PEER_POST_HANDLERS = {
    "_handle_mesh_handshake",
    "_handle_mesh_job_submit",
    "_handle_mesh_artifact_publish",
    "_handle_mesh_handoff",
}


def _configured_operator_token() -> str:
    return (
        os.environ.get("OCP_OPERATOR_TOKEN")
        or os.environ.get("OCP_CONTROL_TOKEN")
        or ""
    ).strip()


def _extract_bearer_token(value: str) -> str:
    sample = str(value or "").strip()
    if sample.lower().startswith("bearer "):
        return sample[7:].strip()
    return sample


def _request_operator_token(headers) -> str:
    if headers is None:
        return ""
    for key in ("X-OCP-Operator-Token", "X-OCP-Control-Token", "Authorization"):
        token = _extract_bearer_token(headers.get(key, ""))
        if token:
            return token
    return ""


def _client_host(handler) -> str:
    client_address = getattr(handler, "client_address", None)
    if isinstance(client_address, tuple) and client_address:
        return str(client_address[0] or "").strip()
    return ""


def _is_loopback_client(host: str) -> bool:
    token = str(host or "").strip().lower()
    if not token:
        return True
    if token == "localhost":
        return True
    try:
        return ipaddress.ip_address(token).is_loopback
    except ValueError:
        return False


def _operator_auth_required(path: str) -> bool:
    spec = _resolve_post_route(path)
    if spec is None:
        return False
    return spec.handler_name not in _SIGNED_PEER_POST_HANDLERS


def _operator_authorized(handler) -> bool:
    configured = _configured_operator_token()
    presented = _request_operator_token(getattr(handler, "headers", None))
    if configured:
        return bool(presented and hmac.compare_digest(presented, configured))
    return _is_loopback_client(_client_host(handler))


def _operator_auth_failure(method: str, path: str, handler) -> dict[str, Any]:
    return {
        "error": "operator authorization required",
        "method": str(method or "").strip().upper(),
        "path": path,
        "client_address": _client_host(handler),
        "detail": (
            "set OCP_OPERATOR_TOKEN and send X-OCP-Operator-Token or Authorization Bearer credentials"
            if _configured_operator_token()
            else "raw mesh mutation routes are limited to loopback unless OCP_OPERATOR_TOKEN is configured"
        ),
    }


def _query_bool(params: dict[str, list[str]], key: str, *, default: bool) -> bool:
    raw = params.get(key, ["1" if default else "0"])[0]
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


class OCPRouteHandlerMixin:
    def _handle_control_page(self):
        self._send_html(_build_control_page(self._mesh()))

    def _handle_easy_page(self):
        self._send_html(_build_easy_page(self._mesh()))

    def _handle_app_page(self):
        self._send_html(_build_app_page(self._mesh()))

    def _handle_app_manifest(self):
        self._send_manifest_json(_build_app_manifest(self._mesh()))

    def _handle_mesh_app_status(self):
        self._send_json(_build_app_status(self._mesh()))

    def _handle_control_stream(self, params):
        _handle_control_stream_impl(self, self._mesh(), params)

    def _handle_mesh_contract(self):
        self._send_json(_build_contract_snapshot())

    def _handle_mesh_manifest(self):
        self._send_json(_get_manifest_impl(self._mesh()))

    def _handle_mesh_device_profile(self):
        self._send_json(_get_device_profile_impl(self._mesh()))

    def _handle_mesh_device_profile_update(self, data):
        self._send_json(_update_device_profile_impl(self._mesh(), data))

    def _handle_mesh_peers(self, params):
        self._send_json(_list_peers_runtime_impl(self._mesh(), limit=int(params.get("limit", ["25"])[0])))

    def _handle_mesh_peers_sync(self, data):
        self._send_json(_sync_peer_impl(self._mesh(), data))

    def _handle_mesh_discovery_candidates(self, params):
        self._send_json(
            _list_discovery_candidates_impl(
                self._mesh(),
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
            )
        )

    def _handle_mesh_discovery_seek(self, data):
        self._send_json(_seek_discovery_peers_impl(self._mesh(), data))

    def _handle_mesh_discovery_scan_local(self, data):
        self._send_json(_scan_local_peers_impl(self._mesh(), data))

    def _handle_mesh_connectivity_diagnostics(self):
        self._send_json(_connectivity_diagnostics_impl(self._mesh(), limit=24))

    def _handle_mesh_autonomy_status(self):
        self._send_json(_autonomy_status_impl(self._mesh()))

    def _handle_mesh_autonomy_activate(self, data):
        self._send_json(_activate_autonomic_mesh_impl(self._mesh(), data))

    def _handle_mesh_routes_health(self):
        self._send_json(_routes_health_impl(self._mesh(), limit=50))

    def _handle_mesh_routes_probe(self, data):
        self._send_json(_probe_routes_impl(self._mesh(), data))

    def _handle_mesh_peers_connect(self, data):
        self._send_json(_connect_peer_impl(self._mesh(), data))

    def _handle_mesh_peers_connect_all(self, data):
        self._send_json(_connect_all_peers_impl(self._mesh(), data))

    def _handle_mesh_mission_test_mesh_launch(self, data):
        self._send_json(_launch_mesh_test_mission_impl(self._mesh(), data))

    def _handle_mesh_stream(self, params):
        self._send_json(
            _stream_snapshot_impl(
                self._mesh(),
                since_seq=int(params.get("since", ["0"])[0]),
                limit=int(params.get("limit", ["50"])[0]),
            )
        )

    def _handle_mesh_handshake(self, data):
        self._send_json(_accept_handshake_impl(self._mesh(), data))

    def _handle_mesh_lease_acquire(self, data):
        self._send_json(_acquire_lease_impl(self._mesh(), data))

    def _handle_mesh_lease_heartbeat(self, data):
        self._send_json(_heartbeat_lease_impl(self._mesh(), data))

    def _handle_mesh_lease_release(self, data):
        self._send_json(_release_lease_impl(self._mesh(), data))

    def _handle_mesh_job_submit(self, data):
        self._send_json(_submit_job_impl(self._mesh(), data))

    def _handle_mesh_job_schedule(self, data):
        self._send_json(_schedule_job_impl(self._mesh(), data))

    def _handle_mesh_job_get(self, path: str):
        self._send_json(_get_job_from_path_impl(self._mesh(), path))

    def _handle_mesh_missions(self, params):
        self._send_json(
            _list_missions_impl(
                self._mesh(),
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
            )
        )

    def _handle_mesh_mission_continuity_get(self, path: str):
        self._send_json(_get_mission_continuity_impl(self._mesh(), _path_token(path, "/mesh/missions/", "/continuity")))

    def _handle_mesh_mission_continuity_export(self, path: str, data):
        self._send_json(
            _export_mission_continuity_impl(
                self._mesh(),
                _path_token(path, "/mesh/missions/", "/continuity/export"),
                data,
            )
        )

    def _handle_mesh_continuity_vessel_verify(self, data):
        self._send_json(_verify_continuity_vessel_impl(self._mesh(), data))

    def _handle_mesh_continuity_restore_plan(self, data):
        self._send_json(_plan_continuity_restore_impl(self._mesh(), data))

    def _handle_mesh_treaties(self, params):
        self._send_json(
            _list_treaties_impl(
                self._mesh(),
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
                treaty_type=params.get("treaty_type", [""])[0],
            )
        )

    def _handle_mesh_treaty_get(self, path: str):
        self._send_json(_get_treaty_from_path_impl(self._mesh(), path))

    def _handle_mesh_treaty_propose(self, data):
        self._send_json(_propose_treaty_impl(self._mesh(), data))

    def _handle_mesh_treaty_audit(self, data):
        self._send_json(_audit_treaty_requirements_impl(self._mesh(), data))

    def _handle_mesh_mission_get(self, path: str):
        self._send_json(_get_mission_from_path_impl(self._mesh(), path))

    def _handle_mesh_mission_launch(self, data):
        self._send_json(_launch_mission_impl(self._mesh(), data))

    def _handle_mesh_mission_test_launch(self, data):
        self._send_json(_launch_test_mission_impl(self._mesh(), data))

    def _handle_mesh_mission_cancel(self, path: str, data):
        self._send_json(_cancel_mission_impl(self._mesh(), _path_token(path, "/mesh/missions/", "/cancel"), data))

    def _handle_mesh_mission_resume(self, path: str, data):
        self._send_json(_resume_mission_impl(self._mesh(), _path_token(path, "/mesh/missions/", "/resume"), data))

    def _handle_mesh_mission_resume_from_checkpoint(self, path: str, data):
        self._send_json(
            _resume_mission_from_checkpoint_impl(
                self._mesh(),
                _path_token(path, "/mesh/missions/", "/resume-from-checkpoint"),
                data,
            )
        )

    def _handle_mesh_mission_restart(self, path: str, data):
        self._send_json(_restart_mission_impl(self._mesh(), _path_token(path, "/mesh/missions/", "/restart"), data))

    def _handle_mesh_cooperative_tasks(self, params):
        self._send_json(
            _list_cooperative_tasks_impl(
                self._mesh(),
                limit=int(params.get("limit", ["25"])[0]),
                state=params.get("state", [""])[0],
            )
        )

    def _handle_mesh_cooperative_task_get(self, path: str):
        self._send_json(_get_cooperative_task_from_path_impl(self._mesh(), path))

    def _handle_mesh_cooperative_task_launch(self, data):
        self._send_json(_launch_cooperative_task_impl(self._mesh(), data))

    def _handle_mesh_pressure(self):
        self._send_json(_mesh_pressure_impl(self._mesh()))

    def _handle_mesh_helpers(self, params):
        self._send_json(_list_helpers_impl(self._mesh(), limit=int(params.get("limit", ["100"])[0])))

    def _handle_mesh_helpers_plan(self, data):
        self._send_json(_plan_helper_enlistment_impl(self._mesh(), data))

    def _handle_mesh_helpers_enlist(self, data):
        self._send_json(_enlist_helper_impl(self._mesh(), data))

    def _handle_mesh_helpers_drain(self, data):
        self._send_json(_drain_helper_impl(self._mesh(), data))

    def _handle_mesh_helpers_retire(self, data):
        self._send_json(_retire_helper_impl(self._mesh(), data))

    def _handle_mesh_helpers_auto_seek(self, data):
        self._send_json(_auto_seek_help_impl(self._mesh(), data))

    def _handle_mesh_helpers_preferences(self, params):
        self._send_json(
            _list_offload_preferences_impl(
                self._mesh(),
                limit=int(params.get("limit", ["100"])[0]),
                peer_id=params.get("peer_id", [""])[0],
                workload_class=params.get("workload_class", [""])[0],
            )
        )

    def _handle_mesh_helpers_preferences_set(self, data):
        self._send_json(_set_offload_preference_impl(self._mesh(), data))

    def _handle_mesh_helpers_autonomy(self):
        self._send_json(_evaluate_autonomous_offload_impl(self._mesh()))

    def _handle_mesh_helpers_autonomy_run(self, data):
        self._send_json(_run_autonomous_offload_impl(self._mesh(), data))

    def _handle_mesh_job_resume(self, path: str, data):
        self._send_json(_resume_job_impl(self._mesh(), _path_token(path, "/mesh/jobs/", "/resume"), data))

    def _handle_mesh_job_resume_from_checkpoint(self, path: str, data):
        self._send_json(
            _resume_job_from_checkpoint_impl(
                self._mesh(),
                _path_token(path, "/mesh/jobs/", "/resume-from-checkpoint"),
                data,
            )
        )

    def _handle_mesh_job_restart(self, path: str, data):
        self._send_json(_restart_job_impl(self._mesh(), _path_token(path, "/mesh/jobs/", "/restart"), data))

    def _handle_mesh_workers(self, params):
        self._send_json(_list_workers_impl(self._mesh(), limit=int(params.get("limit", ["25"])[0])))

    def _handle_mesh_notifications(self, params):
        self._send_json(
            _list_notifications_impl(
                self._mesh(),
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
                target_peer_id=params.get("target_peer_id", [""])[0],
                target_agent_id=params.get("target_agent_id", [""])[0],
            )
        )

    def _handle_mesh_notification_publish(self, data):
        self._send_json(_publish_notification_impl(self._mesh(), data))

    def _handle_mesh_notification_ack(self, path: str, data):
        self._send_json(_ack_notification_from_path_impl(self._mesh(), path, data))

    def _handle_mesh_approvals(self, params):
        self._send_json(
            _list_approvals_impl(
                self._mesh(),
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
                target_peer_id=params.get("target_peer_id", [""])[0],
                target_agent_id=params.get("target_agent_id", [""])[0],
            )
        )

    def _handle_mesh_approval_request(self, data):
        self._send_json(_create_approval_request_impl(self._mesh(), data))

    def _handle_mesh_approval_resolve(self, path: str, data):
        self._send_json(_resolve_approval_from_path_impl(self._mesh(), path, data))

    def _handle_mesh_secrets(self, params):
        self._send_json(
            _list_secrets_impl(
                self._mesh(),
                limit=int(params.get("limit", ["25"])[0]),
                scope=params.get("scope", [""])[0],
            )
        )

    def _handle_mesh_secret_put(self, data):
        self._send_json(_put_secret_impl(self._mesh(), data))

    def _handle_mesh_queue(self, params):
        self._send_json(
            _list_queue_messages_impl(
                self._mesh(),
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
            )
        )

    def _handle_mesh_queue_events(self, params):
        cursor = params.get("since", params.get("since_seq", ["0"]))[0]
        self._send_json(
            _list_queue_events_impl(
                self._mesh(),
                since_seq=int(cursor),
                limit=int(params.get("limit", ["50"])[0]),
                queue_message_id=params.get("queue_message_id", [""])[0],
                job_id=params.get("job_id", [""])[0],
            )
        )

    def _handle_mesh_queue_metrics(self):
        self._send_json(_queue_metrics_impl(self._mesh()))

    def _handle_mesh_queue_replay(self, data):
        self._send_json(_replay_queue_message_impl(self._mesh(), data))

    def _handle_mesh_queue_ack_deadline(self, data):
        self._send_json(_set_queue_ack_deadline_impl(self._mesh(), data))

    def _handle_mesh_scheduler_decisions(self, params):
        self._send_json(
            _list_scheduler_decisions_impl(
                self._mesh(),
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
                target_type=params.get("target_type", [""])[0],
            )
        )

    def _handle_mesh_worker_register(self, data):
        self._send_json(_register_worker_impl(self._mesh(), data))

    def _handle_mesh_worker_heartbeat(self, path: str, data):
        self._send_json(_heartbeat_worker_from_path_impl(self._mesh(), path, data))

    def _handle_mesh_worker_poll(self, path: str, data):
        self._send_json(_poll_worker_from_path_impl(self._mesh(), path, data))

    def _handle_mesh_worker_claim(self, path: str, data):
        self._send_json(_claim_worker_job_from_path_impl(self._mesh(), path, data))

    def _handle_mesh_attempt_heartbeat(self, path: str, data):
        self._send_json(_heartbeat_attempt_from_path_impl(self._mesh(), path, data))

    def _handle_mesh_attempt_complete(self, path: str, data):
        self._send_json(_complete_attempt_from_path_impl(self._mesh(), path, data))

    def _handle_mesh_attempt_fail(self, path: str, data):
        self._send_json(_fail_attempt_from_path_impl(self._mesh(), path, data))

    def _handle_mesh_job_cancel(self, path: str, data):
        self._send_json(_cancel_job_impl(self._mesh(), _path_token(path, "/mesh/jobs/", "/cancel"), data))

    def _handle_mesh_artifact_publish(self, data):
        self._send_json(_publish_artifact_impl(self._mesh(), data))

    def _handle_mesh_artifact_list(self, params):
        self._send_json(
            _list_artifacts_impl(
                self._mesh(),
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
        self._send_json(_get_artifact_from_path_impl(self._mesh(), path, params))

    def _handle_mesh_artifact_purge(self, data):
        self._send_json(_purge_expired_artifacts_impl(self._mesh(), data))

    def _handle_mesh_artifact_replicate(self, data):
        self._send_json(_replicate_artifact_impl(self._mesh(), data))

    def _handle_mesh_artifact_replicate_graph(self, data):
        self._send_json(_replicate_artifact_graph_impl(self._mesh(), data))

    def _handle_mesh_artifact_pin(self, data):
        self._send_json(_set_artifact_pin_impl(self._mesh(), data))

    def _handle_mesh_artifact_verify_mirror(self, data):
        self._send_json(_verify_artifact_mirror_impl(self._mesh(), data))

    def _handle_mesh_handoff(self, data):
        self._send_json(_accept_handoff_impl(self._mesh(), data))

    def _dispatch_get_request(self, path: str, params: dict[str, list[str]]) -> bool:
        if path.startswith("/mesh/artifacts/") and _query_bool(params, "include_content", default=True):
            if not _operator_authorized(self) and not self._artifact_content_is_public(path):
                self._send_json(_operator_auth_failure("GET", path, self), 401)
                return True
        return _dispatch_get_request_impl(self, path, params)

    def _dispatch_post_request(self, path: str, data: dict[str, Any]) -> bool:
        if _operator_auth_required(path) and not _operator_authorized(self):
            self._send_json(_operator_auth_failure("POST", path, self), 401)
            return True
        validation = _validate_route_request("POST", path, data)
        if validation.get("status") == "invalid":
            self._send_json(
                {
                    "error": "protocol validation failed",
                    "protocol_validation": validation,
                },
                400,
            )
            return True
        return _dispatch_post_request_impl(self, path, data)

    def _artifact_content_is_public(self, path: str) -> bool:
        try:
            artifact = self._mesh().get_artifact(_path_token(path, "/mesh/artifacts/"), include_content=False)
            return self._mesh()._policy_allows_peer(dict(artifact.get("policy") or {}), None)
        except Exception:
            return False


__all__ = ["OCPRouteHandlerMixin"]
