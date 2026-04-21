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
