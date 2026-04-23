"""
HTTP contract metadata for the OCP mesh route surface.

This module intentionally describes the current runtime contract without
enforcing it yet. It gives operators, docs, and future conformance tests a
single code-owned map of the `/mesh/*` API.
"""

from __future__ import annotations

from typing import Any

from mesh_protocol import (
    build_protocol_conformance_snapshot,
    build_protocol_schema_snapshot,
    get_protocol_schema,
    validate_protocol_object,
)
from server_routes import GET_ROUTE_GROUPS, POST_ROUTE_GROUPS, RouteSpec

CONTRACT_VERSION = "ocp-http-contract/v1alpha1"


PATH_PARAM_BY_PREFIX = {
    "/mesh/artifacts/": "artifact_id",
    "/mesh/approvals/": "approval_id",
    "/mesh/cooperative-tasks/": "task_id",
    "/mesh/jobs/attempts/": "attempt_id",
    "/mesh/jobs/": "job_id",
    "/mesh/missions/": "mission_id",
    "/mesh/notifications/": "notification_id",
    "/mesh/treaties/": "treaty_id",
    "/mesh/workers/": "worker_id",
}


QUERY_FIELDS: dict[str, dict[str, str]] = {
    "_handle_control_stream": {"since": "integer", "limit": "integer", "once": "boolean"},
    "_handle_mesh_artifact_get": {"include_content": "boolean"},
    "_handle_mesh_artifact_list": {
        "limit": "integer",
        "artifact_kind": "string",
        "digest": "string",
        "job_id": "string",
        "attempt_id": "string",
        "parent_artifact_id": "string",
        "owner_peer_id": "string",
        "media_type": "string",
        "retention_class": "string",
    },
    "_handle_mesh_approvals": {
        "limit": "integer",
        "status": "string",
        "target_peer_id": "string",
        "target_agent_id": "string",
    },
    "_handle_mesh_cooperative_tasks": {"limit": "integer", "state": "string"},
    "_handle_mesh_discovery_candidates": {"limit": "integer", "status": "string"},
    "_handle_mesh_helpers": {"limit": "integer"},
    "_handle_mesh_helpers_preferences": {"limit": "integer", "peer_id": "string", "workload_class": "string"},
    "_handle_mesh_missions": {"limit": "integer", "status": "string"},
    "_handle_mesh_notifications": {
        "limit": "integer",
        "status": "string",
        "target_peer_id": "string",
        "target_agent_id": "string",
    },
    "_handle_mesh_peers": {"limit": "integer"},
    "_handle_mesh_queue": {"limit": "integer", "status": "string"},
    "_handle_mesh_queue_events": {
        "since": "integer",
        "since_seq": "integer",
        "limit": "integer",
        "queue_message_id": "string",
        "job_id": "string",
    },
    "_handle_mesh_scheduler_decisions": {"limit": "integer", "status": "string", "target_type": "string"},
    "_handle_mesh_secrets": {"limit": "integer", "scope": "string"},
    "_handle_mesh_stream": {"since": "integer", "limit": "integer"},
    "_handle_mesh_treaties": {"limit": "integer", "status": "string", "treaty_type": "string"},
    "_handle_mesh_workers": {"limit": "integer"},
}


BODY_FIELDS: dict[str, dict[str, str]] = {
    "_handle_mesh_autonomy_activate": {"mode": "string", "run_proof": "boolean", "repair": "boolean", "limit": "integer"},
    "_handle_mesh_approval_request": {"title": "string", "summary": "string", "metadata": "object"},
    "_handle_mesh_approval_resolve": {"decision": "string", "operator_peer_id": "string", "metadata": "object"},
    "_handle_mesh_artifact_pin": {"artifact_id": "string", "pinned": "boolean", "reason": "string"},
    "_handle_mesh_artifact_publish": {"artifact": "object", "content": "any", "metadata": "object"},
    "_handle_mesh_artifact_purge": {"limit": "integer"},
    "_handle_mesh_artifact_replicate": {"peer_id": "string", "artifact_id": "string", "digest": "string", "pin": "boolean"},
    "_handle_mesh_artifact_replicate_graph": {"peer_id": "string", "artifact_id": "string", "digest": "string", "pin": "boolean"},
    "_handle_mesh_artifact_verify_mirror": {"artifact_id": "string", "peer_id": "string", "source_artifact_id": "string", "digest": "string"},
    "_handle_mesh_attempt_complete": {"result": "object", "executor": "string", "metadata": "object"},
    "_handle_mesh_attempt_fail": {"error": "string", "retryable": "boolean", "metadata": "object"},
    "_handle_mesh_attempt_heartbeat": {"ttl_seconds": "integer", "metadata": "object"},
    "_handle_mesh_continuity_restore_plan": {"artifact_id": "string", "target_peer_id": "string", "constraints": "object"},
    "_handle_mesh_continuity_vessel_verify": {"artifact_id": "string", "include_content": "boolean"},
    "_handle_mesh_device_profile_update": {"device_profile": "object"},
    "_handle_mesh_discovery_scan_local": {"trust_tier": "string", "timeout": "number", "limit": "integer", "port": "integer"},
    "_handle_mesh_discovery_seek": {
        "base_urls": "array",
        "hosts": "array",
        "cidr": "string",
        "port": "integer",
        "trust_tier": "string",
        "auto_connect": "boolean",
        "limit": "integer",
    },
    "_handle_mesh_handoff": {"handoff": "signed_envelope"},
    "_handle_mesh_handshake": {"peer_card": "signed_envelope", "manifest": "object"},
    "_handle_mesh_helpers_auto_seek": {"limit": "integer", "trust_tier": "string"},
    "_handle_mesh_helpers_drain": {"peer_id": "string", "reason": "string"},
    "_handle_mesh_helpers_enlist": {"peer_id": "string", "reason": "string"},
    "_handle_mesh_helpers_plan": {"limit": "integer", "workload_class": "string"},
    "_handle_mesh_helpers_preferences_set": {"peer_id": "string", "workload_class": "string", "preference": "string"},
    "_handle_mesh_helpers_retire": {"peer_id": "string", "reason": "string"},
    "_handle_mesh_helpers_autonomy_run": {"dry_run": "boolean", "reason": "string"},
    "_handle_mesh_job_cancel": {"operator_id": "string", "reason": "string"},
    "_handle_mesh_job_restart": {"operator_id": "string", "reason": "string", "metadata": "object"},
    "_handle_mesh_job_resume": {"operator_id": "string", "metadata": "object"},
    "_handle_mesh_job_resume_from_checkpoint": {"checkpoint_ref": "object", "operator_id": "string", "metadata": "object"},
    "_handle_mesh_job_schedule": {"job": "object", "request_id": "string"},
    "_handle_mesh_job_submit": {"job": "signed_envelope|object"},
    "_handle_mesh_lease_acquire": {"peer_id": "string", "resource": "string", "ttl_seconds": "integer"},
    "_handle_mesh_lease_heartbeat": {"lease_id": "string", "ttl_seconds": "integer"},
    "_handle_mesh_lease_release": {"lease_id": "string", "status": "string"},
    "_handle_mesh_mission_cancel": {"operator_id": "string", "reason": "string"},
    "_handle_mesh_mission_continuity_export": {"dry_run": "boolean", "include_artifacts": "boolean", "metadata": "object"},
    "_handle_mesh_mission_launch": {"title": "string", "intent": "string", "job": "object", "continuity": "object"},
    "_handle_mesh_mission_restart": {"operator_id": "string", "reason": "string", "metadata": "object"},
    "_handle_mesh_mission_resume": {"operator_id": "string", "metadata": "object"},
    "_handle_mesh_mission_resume_from_checkpoint": {"checkpoint_ref": "object", "operator_id": "string", "metadata": "object"},
    "_handle_mesh_mission_test_launch": {"peer_id": "string", "intent": "string"},
    "_handle_mesh_mission_test_mesh_launch": {"include_local": "boolean", "limit": "integer"},
    "_handle_mesh_notification_ack": {"status": "string", "actor_peer_id": "string"},
    "_handle_mesh_notification_publish": {"notification_type": "string", "title": "string", "body": "string"},
    "_handle_mesh_peers_connect": {"base_url": "string", "peer_id": "string", "trust_tier": "string"},
    "_handle_mesh_peers_connect_all": {"limit": "integer", "trust_tier": "string", "refresh_manifest": "boolean"},
    "_handle_mesh_peers_sync": {"peer_id": "string", "limit": "integer", "refresh_manifest": "boolean"},
    "_handle_mesh_queue_ack_deadline": {
        "queue_message_id": "string",
        "ttl_seconds": "integer",
        "ack_deadline_seconds": "integer",
    },
    "_handle_mesh_queue_replay": {"queue_message_id": "string", "reason": "string"},
    "_handle_mesh_routes_probe": {"peer_id": "string", "base_url": "string", "timeout": "number", "limit": "integer"},
    "_handle_mesh_secret_put": {"name": "string", "scope": "string", "value": "string", "metadata": "object"},
    "_handle_mesh_treaty_audit": {"treaty_requirements": "array", "operation": "string", "metadata": "object"},
    "_handle_mesh_treaty_propose": {"treaty_id": "string", "title": "string", "summary": "string", "treaty_type": "string"},
    "_handle_mesh_worker_claim": {"job_id": "string", "ttl_seconds": "integer"},
    "_handle_mesh_worker_heartbeat": {"status": "string", "resources": "object", "metadata": "object"},
    "_handle_mesh_worker_poll": {"limit": "integer"},
    "_handle_mesh_worker_register": {"worker_id": "string", "agent_id": "string", "capabilities": "array"},
    "_handle_mesh_cooperative_task_launch": {"name": "string", "base_job": "object", "shards": "array"},
}


REQUEST_REFS: dict[str, str] = {
    "_handle_mesh_artifact_publish": "SignedEnvelope",
    "_handle_mesh_autonomy_activate": "AutonomicActivateRequest",
    "_handle_mesh_continuity_restore_plan": "ContinuityRestorePlanRequest",
    "_handle_mesh_handoff": "SignedEnvelope",
    "_handle_mesh_handshake": "SignedEnvelope",
    "_handle_mesh_job_submit": "SignedEnvelope",
    "_handle_mesh_mission_continuity_export": "ContinuityVesselExportRequest",
    "_handle_mesh_routes_probe": "RouteProbeRequest",
    "_handle_mesh_treaty_audit": "TreatyAuditRequest",
}


RESPONSE_REFS: dict[str, str] = {
    "_handle_control_stream": "ControlStreamSSE",
    "_handle_mesh_app_status": "AppStatus",
    "_handle_mesh_contract": "ContractSnapshot",
    "_handle_mesh_artifact_get": "Artifact",
    "_handle_mesh_artifact_list": "ArtifactList",
    "_handle_mesh_artifact_pin": "ArtifactPinResponse",
    "_handle_mesh_artifact_publish": "ArtifactPublishResponse",
    "_handle_mesh_artifact_purge": "ArtifactPurgeResponse",
    "_handle_mesh_artifact_replicate": "ArtifactReplicationResponse",
    "_handle_mesh_artifact_replicate_graph": "ArtifactGraphReplicationResponse",
    "_handle_mesh_artifact_verify_mirror": "ArtifactMirrorVerification",
    "_handle_mesh_autonomy_activate": "AutonomicRun",
    "_handle_mesh_autonomy_status": "AutonomicMeshStatus",
    "_handle_mesh_approvals": "ApprovalList",
    "_handle_mesh_cooperative_task_get": "CooperativeTask",
    "_handle_mesh_cooperative_tasks": "CooperativeTaskList",
    "_handle_mesh_continuity_restore_plan": "ContinuityRestorePlan",
    "_handle_mesh_continuity_vessel_verify": "ContinuityVesselVerification",
    "_handle_mesh_device_profile": "DeviceProfileResponse",
    "_handle_mesh_device_profile_update": "DeviceProfileResponse",
    "_handle_mesh_discovery_candidates": "DiscoveryCandidateList",
    "_handle_mesh_handshake": "HandshakeResponse",
    "_handle_mesh_helpers": "HelperList",
    "_handle_mesh_helpers_autonomy": "AutonomousOffloadEvaluation",
    "_handle_mesh_job_get": "Job",
    "_handle_mesh_manifest": "MeshManifest",
    "_handle_mesh_mission_continuity_export": "ContinuityVesselExport",
    "_handle_mesh_mission_continuity_get": "MissionContinuitySummary",
    "_handle_mesh_mission_get": "Mission",
    "_handle_mesh_missions": "MissionList",
    "_handle_mesh_notifications": "NotificationList",
    "_handle_mesh_peers": "PeerList",
    "_handle_mesh_pressure": "MeshPressure",
    "_handle_mesh_queue": "QueueMessageList",
    "_handle_mesh_queue_events": "QueueEventList",
    "_handle_mesh_queue_metrics": "QueueMetrics",
    "_handle_mesh_routes_health": "RouteHealthList",
    "_handle_mesh_routes_probe": "RouteProbeResult",
    "_handle_mesh_scheduler_decisions": "SchedulerDecisionList",
    "_handle_mesh_secrets": "SecretList",
    "_handle_mesh_stream": "EventStreamSnapshot",
    "_handle_mesh_treaties": "TreatyList",
    "_handle_mesh_treaty_get": "Treaty",
    "_handle_mesh_workers": "WorkerList",
}


def path_template(spec: RouteSpec) -> str:
    if spec.path is not None:
        return spec.path
    prefix = spec.prefix or ""
    param = PATH_PARAM_BY_PREFIX.get(prefix, "id")
    return f"{prefix}{{{param}}}{spec.suffix}"


def path_params(spec: RouteSpec) -> dict[str, str]:
    if spec.path is not None or spec.prefix is None:
        return {}
    return {PATH_PARAM_BY_PREFIX.get(spec.prefix, "id"): "string"}


def _request_schema(spec: RouteSpec) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "argument_kind": spec.argument_kind,
        "schema_ref": REQUEST_REFS.get(spec.handler_name, ""),
        "path": path_params(spec),
        "query": dict(QUERY_FIELDS.get(spec.handler_name, {})),
        "body": dict(BODY_FIELDS.get(spec.handler_name, {})),
    }
    return {key: value for key, value in schema.items() if value or key == "argument_kind"}


def _response_schema(spec: RouteSpec) -> dict[str, Any]:
    schema_ref = RESPONSE_REFS.get(spec.handler_name, "Object")
    return {
        "type": "object",
        "schema_ref": schema_ref,
        "schema_available": get_protocol_schema(schema_ref) is not None,
    }


def route_contract(method: str, group: str, spec: RouteSpec) -> dict[str, Any]:
    template = path_template(spec)
    return {
        "id": f"{method.lower()}:{template}",
        "method": method.upper(),
        "group": group,
        "path": template,
        "handler": spec.handler_name,
        "request": _request_schema(spec),
        "response": _response_schema(spec),
    }


def _iter_contracts(route_groups: dict[str, tuple[RouteSpec, ...]], method: str):
    for group, specs in route_groups.items():
        for spec in specs:
            template = path_template(spec)
            if not template.startswith("/mesh/"):
                continue
            yield route_contract(method, group, spec)


def build_contract_snapshot() -> dict[str, Any]:
    schema_snapshot = build_protocol_schema_snapshot()
    conformance_snapshot = build_protocol_conformance_snapshot()
    endpoints = [
        *_iter_contracts(GET_ROUTE_GROUPS, "GET"),
        *_iter_contracts(POST_ROUTE_GROUPS, "POST"),
    ]
    groups: dict[str, dict[str, Any]] = {}
    for endpoint in endpoints:
        group = groups.setdefault(endpoint["group"], {"count": 0, "methods": []})
        group["count"] += 1
        if endpoint["method"] not in group["methods"]:
            group["methods"].append(endpoint["method"])
    for group in groups.values():
        group["methods"].sort()
    return {
        "status": "ok",
        "contract_version": CONTRACT_VERSION,
        "schema_version": schema_snapshot["schema_version"],
        "protocol_surface": "/mesh/*",
        "endpoint_count": len(endpoints),
        "schema_count": schema_snapshot["count"],
        "groups": groups,
        "endpoints": endpoints,
        "schemas": schema_snapshot["schemas"],
        "conformance": conformance_snapshot,
    }


def contract_for(method: str, path: str) -> dict[str, Any] | None:
    method = str(method or "").strip().upper()
    route_groups = GET_ROUTE_GROUPS if method == "GET" else POST_ROUTE_GROUPS if method == "POST" else {}
    for group, specs in route_groups.items():
        for spec in specs:
            if spec.matches(path):
                return route_contract(method, group, spec)
    return None


def validate_route_request(method: str, path: str, payload: Any) -> dict[str, Any]:
    contract = contract_for(method, path)
    if contract is None:
        return {"status": "skipped", "reason": "route_not_found", "method": method, "path": path}
    schema_ref = str(dict(contract.get("request") or {}).get("schema_ref") or "").strip()
    if not schema_ref:
        return {"status": "skipped", "reason": "schema_not_declared", "method": method, "path": path}
    validation = validate_protocol_object(schema_ref, payload)
    return {
        **validation,
        "method": str(method or "").strip().upper(),
        "path": path,
        "contract_id": contract["id"],
    }
