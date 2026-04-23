"""
Small protocol schema registry for core OCP wire objects.

The registry is intentionally descriptive rather than enforcing. It gives the
runtime, docs, and future conformance tests a shared vocabulary for the most
important objects moving across the `/mesh/*` surface.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

SCHEMA_VERSION = "ocp-protocol-schemas/v1alpha1"


PROTOCOL_SCHEMAS: dict[str, dict[str, Any]] = {
    "SignedEnvelope": {
        "type": "object",
        "required": ["request", "body"],
        "properties": {
            "request": {
                "type": "object",
                "required": [
                    "node_id",
                    "timestamp",
                    "nonce",
                    "request_id",
                    "protocol_family",
                    "protocol_release",
                    "protocol_version",
                    "signature_scheme",
                    "signature",
                ],
                "properties": {
                    "node_id": {"type": "string"},
                    "timestamp": {"type": "string", "format": "date-time"},
                    "nonce": {"type": "string"},
                    "request_id": {"type": "string"},
                    "protocol_family": {"type": "string"},
                    "protocol_release": {"type": "string"},
                    "implementation": {"type": "string"},
                    "protocol_version": {"type": "string"},
                    "signature_scheme": {"type": "string"},
                    "signature": {"type": "string"},
                },
            },
            "body": {"type": "object"},
        },
    },
    "MeshManifest": {
        "type": "object",
        "required": ["protocol", "protocol_short_name", "protocol_release", "protocol_version", "organism_card"],
        "properties": {
            "protocol": {"type": "string"},
            "protocol_short_name": {"type": "string"},
            "protocol_release": {"type": "string"},
            "protocol_version": {"type": "string"},
            "implementation": {"type": "object"},
            "organism_card": {"$ref": "#/schemas/PeerCard"},
            "device_profile": {"type": "object"},
            "sync_policy": {"type": "object"},
            "agent_presence": {"type": "array", "items": {"type": "object"}},
            "workers": {"type": "array", "items": {"type": "object"}},
            "queue_metrics": {"type": "object"},
            "continuity_capabilities": {"type": "object"},
            "treaty_capabilities": {"type": "object"},
            "governance_summary": {"type": "object"},
        },
    },
    "PeerCard": {
        "type": "object",
        "required": ["organism_id", "node_id", "public_key", "protocol_version"],
        "properties": {
            "organism_id": {"type": "string"},
            "node_id": {"type": "string"},
            "display_name": {"type": "string"},
            "public_key": {"type": "string"},
            "endpoint_url": {"type": "string"},
            "protocol_version": {"type": "string"},
            "trust_tier": {"type": "string"},
            "capability_cards": {"type": "array", "items": {"type": "object"}},
            "device_profile": {"type": "object"},
            "habitat_roles": {"type": "array", "items": {"type": "string"}},
            "continuity_capabilities": {"type": "object"},
            "treaty_capabilities": {"type": "object"},
            "governance_summary": {"type": "object"},
        },
    },
    "JobSubmission": {
        "type": "object",
        "required": ["kind"],
        "properties": {
            "kind": {"type": "string"},
            "dispatch_mode": {"type": "string"},
            "requirements": {"type": "object"},
            "policy": {"type": "object"},
            "payload": {"type": "object"},
            "metadata": {"type": "object"},
            "artifact_inputs": {"type": "array", "items": {"$ref": "#/schemas/ArtifactDescriptor"}},
            "continuity": {"type": "object"},
        },
    },
    "JobSubmissionEnvelope": {
        "type": "object",
        "description": "A direct job object or a signed envelope carrying a job submission body.",
        "one_of": [{"$ref": "#/schemas/JobSubmission"}, {"$ref": "#/schemas/SignedEnvelope"}],
    },
    "JobSubmissionRequest": {
        "type": "object",
        "description": "Accepted job submission transport shapes for the HTTP surface.",
        "one_of": [
            {"$ref": "#/schemas/SignedEnvelope"},
            {
                "type": "object",
                "required": ["job"],
                "properties": {"job": {"$ref": "#/schemas/JobSubmission"}},
            },
            {"$ref": "#/schemas/JobSubmission"},
        ],
    },
    "ArtifactDescriptor": {
        "type": "object",
        "required": ["artifact_id", "digest"],
        "properties": {
            "artifact_id": {"type": "string"},
            "digest": {"type": "string"},
            "media_type": {"type": "string"},
            "size_bytes": {"type": "integer"},
            "artifact_kind": {"type": "string"},
            "role": {"type": "string"},
            "uri": {"type": "string"},
            "annotations": {"type": "object"},
            "metadata": {"type": "object"},
        },
    },
    "Artifact": {
        "type": "object",
        "required": ["id", "digest", "media_type", "artifact_kind"],
        "properties": {
            "id": {"type": "string"},
            "digest": {"type": "string"},
            "media_type": {"type": "string"},
            "artifact_kind": {"type": "string"},
            "size_bytes": {"type": "integer"},
            "content": {"type": "any"},
            "metadata": {"type": "object"},
            "descriptor": {"$ref": "#/schemas/ArtifactDescriptor"},
            "created_at": {"type": "string"},
        },
    },
    "ArtifactPublishRequest": {
        "type": "object",
        "properties": {
            "artifact": {"type": "object"},
            "content": {"type": "any"},
            "media_type": {"type": "string"},
            "metadata": {"type": "object"},
            "descriptor": {"$ref": "#/schemas/ArtifactDescriptor"},
        },
    },
    "ArtifactReplicationAuth": {
        "type": "object",
        "description": "Explicit remote-content authorization. Tokens are request-only and must never be persisted or echoed.",
        "properties": {
            "type": {"type": "string"},
            "token": {"type": "string"},
            "redacted": {"type": "boolean"},
            "status": {"type": "string"},
        },
    },
    "ArtifactReplicateRequest": {
        "type": "object",
        "properties": {
            "peer_id": {"type": "string"},
            "artifact_id": {"type": "string"},
            "digest": {"type": "string"},
            "base_url": {"type": "string"},
            "pin": {"type": "boolean"},
            "remote_auth": {"$ref": "#/schemas/ArtifactReplicationAuth"},
        },
    },
    "ArtifactReplicateResponse": {
        "type": "object",
        "required": ["status"],
        "properties": {
            "status": {"type": "string"},
            "artifact": {"$ref": "#/schemas/Artifact"},
            "source": {"type": "object"},
            "verification": {"type": "object"},
            "route_proof": {"type": "object"},
            "remote_auth": {"$ref": "#/schemas/ArtifactReplicationAuth"},
            "governance": {"type": "object"},
        },
    },
    "ArtifactGraphReplicateResponse": {
        "type": "object",
        "required": ["status"],
        "properties": {
            "status": {"type": "string"},
            "root": {"$ref": "#/schemas/ArtifactReplicateResponse"},
            "artifacts": {"type": "array", "items": {"$ref": "#/schemas/Artifact"}},
            "graph": {"type": "object"},
            "route_proof": {"type": "object"},
            "remote_auth": {"$ref": "#/schemas/ArtifactReplicationAuth"},
            "governance": {"type": "object"},
        },
    },
    "MissionContinuitySummary": {
        "type": "object",
        "required": ["mission_id", "continuity"],
        "properties": {
            "mission_id": {"type": "string"},
            "status": {"type": "string"},
            "continuity": {"type": "object"},
            "checkpoints": {"type": "array", "items": {"$ref": "#/schemas/ArtifactDescriptor"}},
            "latest_checkpoint_ref": {"$ref": "#/schemas/ArtifactDescriptor"},
            "recommended_treaty_device": {"type": "object"},
            "safe_devices": {"type": "array", "items": {"type": "object"}},
            "governance": {"type": "object"},
            "treaty_validation": {"type": "object"},
        },
    },
    "ContinuityVesselExport": {
        "type": "object",
        "properties": {
            "status": {"type": "string"},
            "mission_id": {"type": "string"},
            "dry_run": {"type": "boolean"},
            "vessel": {"$ref": "#/schemas/ArtifactDescriptor"},
            "witness": {"$ref": "#/schemas/ArtifactDescriptor"},
            "governance": {"type": "object"},
        },
    },
    "ContinuityVesselExportRequest": {
        "type": "object",
        "properties": {
            "dry_run": {"type": "boolean"},
            "include_artifacts": {"type": "boolean"},
            "metadata": {"type": "object"},
        },
    },
    "ContinuityVesselVerification": {
        "type": "object",
        "properties": {
            "status": {"type": "string"},
            "artifact_id": {"type": "string"},
            "valid": {"type": "boolean"},
            "vessel": {"type": "object"},
            "witnesses": {"type": "array", "items": {"type": "object"}},
            "referenced_artifacts": {"type": "array", "items": {"type": "object"}},
            "governance": {"type": "object"},
        },
    },
    "ContinuityRestorePlan": {
        "type": "object",
        "properties": {
            "status": {"type": "string"},
            "recommended_action": {"type": "string"},
            "recommended_treaty_device": {"type": "object"},
            "safe_devices": {"type": "array", "items": {"type": "object"}},
            "blockers": {"type": "array", "items": {"type": "string"}},
            "warnings": {"type": "array", "items": {"type": "string"}},
            "artifact_readiness": {"type": "object"},
            "governance": {"type": "object"},
        },
    },
    "ContinuityRestorePlanRequest": {
        "type": "object",
        "properties": {
            "artifact_id": {"type": "string"},
            "target_peer_id": {"type": "string"},
            "constraints": {"type": "object"},
        },
    },
    "Treaty": {
        "type": "object",
        "required": ["id", "treaty_type", "status", "document"],
        "properties": {
            "id": {"type": "string"},
            "title": {"type": "string"},
            "summary": {"type": "string"},
            "treaty_type": {"type": "string"},
            "status": {"type": "string"},
            "document": {"type": "object"},
            "metadata": {"type": "object"},
            "created_at": {"type": "string"},
            "updated_at": {"type": "string"},
        },
    },
    "TreatyAudit": {
        "type": "object",
        "required": ["status", "validation"],
        "properties": {
            "status": {"type": "string"},
            "operation": {"type": "string"},
            "validation": {
                "type": "object",
                "properties": {
                    "required": {"type": "array", "items": {"type": "string"}},
                    "satisfied": {"type": "boolean"},
                    "missing": {"type": "array", "items": {"type": "string"}},
                    "inactive": {"type": "array", "items": {"type": "string"}},
                },
            },
            "guidance": {"type": "string"},
            "metadata": {"type": "object"},
        },
    },
    "TreatyAuditRequest": {
        "type": "object",
        "properties": {
            "treaty_requirements": {"type": "array", "items": {"type": "string"}},
            "operation": {"type": "string"},
            "metadata": {"type": "object"},
        },
    },
    "RouteCandidate": {
        "type": "object",
        "required": ["base_url", "source", "status"],
        "properties": {
            "base_url": {"type": "string"},
            "source": {"type": "string"},
            "status": {"type": "string"},
            "latency_ms": {"type": "any"},
            "checked_at": {"type": "string"},
            "last_success_at": {"type": "string"},
            "last_error": {"type": "string"},
            "observed_peer_id": {"type": "string"},
            "freshness": {"type": "string"},
            "failure_count": {"type": "integer"},
            "next_probe_after": {"type": "string"},
            "operator_hint": {"type": "string"},
        },
    },
    "RouteHealth": {
        "type": "object",
        "required": ["peer_id", "status", "candidates"],
        "properties": {
            "peer_id": {"type": "string"},
            "display_name": {"type": "string"},
            "status": {"type": "string"},
            "best_route": {"type": "string"},
            "last_reachable_base_url": {"type": "string"},
            "checked_at": {"type": "string"},
            "last_success_at": {"type": "string"},
            "last_error": {"type": "string"},
            "freshness": {"type": "string"},
            "age_seconds": {"type": "any"},
            "failure_count": {"type": "integer"},
            "next_probe_after": {"type": "string"},
            "operator_hint": {"type": "string"},
            "operator_summary": {"type": "string"},
            "candidates": {"type": "array", "items": {"$ref": "#/schemas/RouteCandidate"}},
        },
    },
    "RouteHealthList": {
        "type": "object",
        "required": ["status", "routes"],
        "properties": {
            "status": {"type": "string"},
            "peer_id": {"type": "string"},
            "count": {"type": "integer"},
            "healthy": {"type": "integer"},
            "routes": {"type": "array", "items": {"$ref": "#/schemas/RouteHealth"}},
            "operator_summary": {"type": "string"},
            "generated_at": {"type": "string"},
        },
    },
    "RouteProofFreshness": {
        "type": "object",
        "properties": {
            "status": {"type": "string"},
            "peer_id": {"type": "string"},
            "best_route": {"type": "string"},
            "freshness": {"type": "string"},
            "checked_at": {"type": "string"},
            "last_success_at": {"type": "string"},
            "source": {"type": "string"},
            "operator_summary": {"type": "string"},
        },
    },
    "RouteProbeRequest": {
        "type": "object",
        "properties": {
            "peer_id": {"type": "string"},
            "base_url": {"type": "string"},
            "timeout": {"type": "number"},
            "limit": {"type": "integer"},
        },
    },
    "RouteProbeResult": {
        "type": "object",
        "required": ["status"],
        "properties": {
            "status": {"type": "string"},
            "peer_id": {"type": "string"},
            "checked": {"type": "integer"},
            "reachable": {"type": "integer"},
            "best_route": {"type": "string"},
            "count": {"type": "integer"},
            "results": {"type": "array", "items": {"type": "object"}},
            "candidates": {"type": "array", "items": {"$ref": "#/schemas/RouteCandidate"}},
            "operator_hint": {"type": "string"},
            "operator_summary": {"type": "string"},
            "generated_at": {"type": "string"},
        },
    },
    "AutonomicAction": {
        "type": "object",
        "required": ["kind", "status", "summary", "created_at"],
        "properties": {
            "id": {"type": "string"},
            "kind": {"type": "string"},
            "status": {"type": "string"},
            "summary": {"type": "string"},
            "peer_id": {"type": "string"},
            "details": {"type": "object"},
            "created_at": {"type": "string"},
        },
    },
    "AutonomicActivateRequest": {
        "type": "object",
        "properties": {
            "mode": {"type": "string"},
            "limit": {"type": "integer"},
            "scan_timeout": {"type": "number"},
            "timeout": {"type": "number"},
            "run_proof": {"type": "boolean"},
            "repair": {"type": "boolean"},
            "max_enlist": {"type": "integer"},
            "actor_agent_id": {"type": "string"},
            "request_id": {"type": "string"},
        },
    },
    "AutonomicRun": {
        "type": "object",
        "required": ["status", "summary", "actions"],
        "properties": {
            "status": {"type": "string"},
            "request_id": {"type": "string"},
            "mode": {"type": "string"},
            "summary": {"type": "string"},
            "operator_summary": {"type": "string"},
            "actions": {"type": "array", "items": {"$ref": "#/schemas/AutonomicAction"}},
            "routes": {"$ref": "#/schemas/RouteHealthList"},
            "proof": {"type": "object"},
            "helpers": {"type": "object"},
            "approvals": {"type": "array", "items": {"type": "object"}},
            "run": {"type": "object"},
            "result": {"type": "object"},
            "generated_at": {"type": "string"},
        },
    },
    "AutonomicMeshStatus": {
        "type": "object",
        "required": ["status", "mode", "routes"],
        "properties": {
            "status": {"type": "string"},
            "mode": {"type": "string"},
            "peer_id": {"type": "string"},
            "operator_summary": {"type": "string"},
            "routes": {"$ref": "#/schemas/RouteHealthList"},
            "pressure": {"type": "object"},
            "helper_autonomy": {"type": "object"},
            "connectivity": {"type": "object"},
            "last_run": {"type": "object"},
            "recommended_actions": {"type": "array", "items": {"type": "string"}},
            "generated_at": {"type": "string"},
        },
    },
    "WorkerCapacity": {
        "type": "object",
        "properties": {
            "worker_id": {"type": "string"},
            "peer_id": {"type": "string"},
            "status": {"type": "string"},
            "capabilities": {"type": "array", "items": {"type": "string"}},
            "resources": {"type": "object"},
            "max_concurrent_jobs": {"type": "integer"},
            "available_slots": {"type": "integer"},
            "operator_summary": {"type": "string"},
        },
    },
    "ExecutionReadiness": {
        "type": "object",
        "properties": {
            "status": {"type": "string"},
            "local": {"type": "object"},
            "targets": {"type": "array", "items": {"type": "object"}},
            "worker_capacity": {"type": "array", "items": {"$ref": "#/schemas/WorkerCapacity"}},
            "operator_summary": {"type": "string"},
        },
    },
    "SetupTimelineEvent": {
        "type": "object",
        "required": ["kind", "status", "summary"],
        "properties": {
            "kind": {"type": "string"},
            "status": {"type": "string"},
            "summary": {"type": "string"},
            "peer_id": {"type": "string"},
            "created_at": {"type": "string"},
            "details": {"type": "object"},
        },
    },
    "AppStatusSample": {
        "type": "object",
        "description": "Operator/app-facing normalized app status point for local charts.",
        "required": ["id", "sampled_at", "node_id", "mesh_score"],
        "properties": {
            "id": {"type": "string"},
            "sampled_at": {"type": "string"},
            "node_id": {"type": "string"},
            "setup_status": {"type": "string"},
            "mesh_score": {"type": "integer"},
            "known_peer_count": {"type": "integer"},
            "route_count": {"type": "integer"},
            "healthy_route_count": {"type": "integer"},
            "latest_proof_status": {"type": "string"},
            "execution_ready_targets": {"type": "integer"},
            "local_ready_workers": {"type": "integer"},
            "artifact_verified_count": {"type": "integer"},
            "pending_approvals": {"type": "integer"},
            "payload": {"type": "object"},
        },
    },
    "AppStatusHistory": {
        "type": "object",
        "required": ["status", "count", "samples"],
        "properties": {
            "status": {"type": "string"},
            "count": {"type": "integer"},
            "limit": {"type": "integer"},
            "samples": {"type": "array", "items": {"$ref": "#/schemas/AppStatusSample"}},
            "generated_at": {"type": "string"},
        },
    },
    "AppHistorySampleRequest": {
        "type": "object",
        "properties": {
            "source": {"type": "string"},
        },
    },
    "AppHistorySampleResponse": {
        "type": "object",
        "required": ["status", "sample"],
        "properties": {
            "status": {"type": "string"},
            "sample": {"$ref": "#/schemas/AppStatusSample"},
            "retention_limit": {"type": "integer"},
        },
    },
    "AppStatus": {
        "type": "object",
        "description": "Operator-facing compact status for the installable OCP app home.",
        "required": ["status", "node", "app_urls", "mesh_quality", "setup", "next_actions"],
        "properties": {
            "status": {"type": "string"},
            "node": {"type": "object"},
            "app_urls": {
                "type": "object",
                "properties": {
                    "base_url": {"type": "string"},
                    "app_url": {"type": "string"},
                    "setup_url": {"type": "string"},
                    "control_url": {"type": "string"},
                    "phone_url": {"type": "string"},
                    "lan_urls": {"type": "array", "items": {"type": "string"}},
                    "sharing_mode": {"type": "string"},
                    "share_advice": {"type": "string"},
                },
            },
            "mesh_quality": {
                "type": "object",
                "properties": {
                    "status": {"type": "string"},
                    "label": {"type": "string"},
                    "peer_count": {"type": "integer"},
                    "route_count": {"type": "integer"},
                    "healthy_routes": {"type": "integer"},
                    "operator_summary": {"type": "string"},
                },
            },
            "protocol": {"type": "object"},
            "setup": {
                "type": "object",
                "properties": {
                    "status": {"type": "string"},
                    "label": {"type": "string"},
                    "primary_action": {"type": "string"},
                    "bind_mode": {"type": "string"},
                    "phone_url": {"type": "string"},
                    "token_status": {"type": "string"},
                    "known_peer_count": {"type": "integer"},
                    "healthy_route_count": {"type": "integer"},
                    "route_count": {"type": "integer"},
                    "latest_proof_status": {"type": "string"},
                    "recovery_state": {"type": "string"},
                    "primary_peer": {
                        "type": "object",
                        "properties": {
                            "peer_id": {"type": "string"},
                            "display_name": {"type": "string"},
                            "role": {"type": "string"},
                            "status": {"type": "string"},
                            "route": {"type": "string"},
                            "summary": {"type": "string"},
                        },
                    },
                    "device_roles": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "peer_id": {"type": "string"},
                                "display_name": {"type": "string"},
                                "role": {"type": "string"},
                                "status": {"type": "string"},
                                "summary": {"type": "string"},
                            },
                        },
                    },
                    "blocking_issue": {"type": "string"},
                    "blocker_code": {"type": "string"},
                    "next_fix": {"type": "string"},
                    "operator_summary": {"type": "string"},
                    "story": {"type": "array", "items": {"type": "string"}},
                    "timeline": {"type": "array", "items": {"$ref": "#/schemas/SetupTimelineEvent"}},
                },
            },
            "autonomy": {"type": "object"},
            "route_health": {"$ref": "#/schemas/RouteHealthList"},
            "execution_readiness": {"$ref": "#/schemas/ExecutionReadiness"},
            "artifact_sync": {"type": "object"},
            "latest_proof": {"type": "object"},
            "approvals": {"type": "object"},
            "next_actions": {"type": "array", "items": {"type": "string"}},
            "generated_at": {"type": "string"},
        },
    },
    "PeerAdvisory": {
        "type": "object",
        "properties": {
            "peer_id": {"type": "string"},
            "source": {"type": "string"},
            "continuity_capabilities": {"type": "object"},
            "treaty_capabilities": {"type": "object"},
            "treaty_compatibility": {"type": "object"},
            "missing_capabilities": {"type": "array", "items": {"type": "string"}},
            "operator_summary": {"type": "string"},
            "recommended_action": {"type": "string"},
        },
    },
    "ConformanceFixture": {
        "type": "object",
        "required": ["id", "schema_ref", "value", "validation"],
        "properties": {
            "id": {"type": "string"},
            "schema_ref": {"type": "string"},
            "purpose": {"type": "string"},
            "value": {"type": "object"},
            "validation": {"type": "object"},
        },
    },
    "ProtocolConformanceSnapshot": {
        "type": "object",
        "required": ["status", "fixture_count", "invalid_fixture_count", "fixtures"],
        "properties": {
            "status": {"type": "string"},
            "schema_version": {"type": "string"},
            "fixture_count": {"type": "integer"},
            "invalid_fixture_count": {"type": "integer"},
            "fixtures": {"type": "array", "items": {"$ref": "#/schemas/ConformanceFixture"}},
        },
    },
    "ContractSnapshot": {
        "type": "object",
        "required": ["status", "contract_version", "schema_version", "protocol_surface", "endpoints", "schemas"],
        "properties": {
            "status": {"type": "string"},
            "contract_version": {"type": "string"},
            "schema_version": {"type": "string"},
            "protocol_surface": {"type": "string"},
            "endpoint_count": {"type": "integer"},
            "schema_count": {"type": "integer"},
            "groups": {"type": "object"},
            "endpoints": {"type": "array", "items": {"type": "object"}},
            "schemas": {"type": "object"},
            "conformance": {"$ref": "#/schemas/ProtocolConformanceSnapshot"},
        },
    },
}


def get_protocol_schema(name: str) -> dict[str, Any] | None:
    schema = PROTOCOL_SCHEMAS.get(str(name or "").strip())
    return deepcopy(schema) if schema is not None else None


def list_protocol_schemas() -> dict[str, dict[str, Any]]:
    return deepcopy(PROTOCOL_SCHEMAS)


def build_protocol_schema_snapshot() -> dict[str, Any]:
    schemas = list_protocol_schemas()
    return {
        "schema_version": SCHEMA_VERSION,
        "count": len(schemas),
        "schemas": schemas,
    }


def _issue(path: str, message: str, *, expected: str = "", actual: str = "") -> dict[str, str]:
    result = {"path": path, "message": message}
    if expected:
        result["expected"] = expected
    if actual:
        result["actual"] = actual
    return result


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _type_matches(expected: str, value: Any) -> bool:
    if expected == "any":
        return True
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    return True


def _schema_from_ref(ref: str) -> dict[str, Any] | None:
    prefix = "#/schemas/"
    if not str(ref or "").startswith(prefix):
        return None
    return PROTOCOL_SCHEMAS.get(str(ref)[len(prefix) :])


def _validate_schema(schema: dict[str, Any], value: Any, *, path: str) -> list[dict[str, str]]:
    if "$ref" in schema:
        resolved = _schema_from_ref(str(schema.get("$ref") or ""))
        if resolved is None:
            return [_issue(path, f"unknown schema ref {schema.get('$ref')}")]
        return _validate_schema(resolved, value, path=path)

    one_of = list(schema.get("one_of") or schema.get("oneOf") or [])
    if one_of:
        candidate_issues: list[list[dict[str, str]]] = []
        for candidate in one_of:
            issues = _validate_schema(dict(candidate or {}), value, path=path)
            if not issues:
                return []
            candidate_issues.append(issues)
        detail = "; ".join(
            issue.get("message", "")
            for issues in candidate_issues[:3]
            for issue in issues[:1]
            if issue.get("message")
        )
        return [_issue(path, f"value does not match any accepted schema shape{': ' + detail if detail else ''}")]

    expected_type = str(schema.get("type") or "").strip()
    if expected_type and not _type_matches(expected_type, value):
        return [_issue(path, "invalid type", expected=expected_type, actual=_type_name(value))]

    issues: list[dict[str, str]] = []
    if expected_type == "object" and isinstance(value, dict):
        for field in list(schema.get("required") or []):
            if field not in value:
                issues.append(_issue(f"{path}.{field}", "required field is missing"))
        properties = dict(schema.get("properties") or {})
        for field, field_schema in properties.items():
            if field not in value:
                continue
            issues.extend(
                _validate_schema(
                    dict(field_schema or {}),
                    value.get(field),
                    path=f"{path}.{field}",
                )
            )
    elif expected_type == "array" and isinstance(value, list):
        item_schema = dict(schema.get("items") or {})
        if item_schema:
            for index, item in enumerate(value):
                issues.extend(_validate_schema(item_schema, item, path=f"{path}[{index}]"))
    return issues


def validate_protocol_object(schema_ref: str, value: Any, *, path: str = "$") -> dict[str, Any]:
    schema_name = str(schema_ref or "").strip()
    schema = PROTOCOL_SCHEMAS.get(schema_name)
    if schema is None:
        return {
            "status": "invalid",
            "schema_ref": schema_name,
            "issues": [_issue(path, f"unknown protocol schema {schema_name or '<empty>'}")],
        }
    issues = _validate_schema(schema, value, path=path)
    return {
        "status": "invalid" if issues else "ok",
        "schema_ref": schema_name,
        "issues": issues,
    }
