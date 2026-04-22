from __future__ import annotations

from copy import deepcopy
from typing import Any

from .schemas import SCHEMA_VERSION, validate_protocol_object


def _fixture_entry(
    fixture_id: str,
    *,
    schema_ref: str,
    purpose: str,
    value: dict[str, Any],
) -> dict[str, Any]:
    payload = deepcopy(value)
    return {
        "id": str(fixture_id or "").strip(),
        "schema_ref": str(schema_ref or "").strip(),
        "purpose": str(purpose or "").strip(),
        "value": payload,
        "validation": validate_protocol_object(schema_ref, payload),
    }


def build_protocol_conformance_snapshot() -> dict[str, Any]:
    fixtures = [
        _fixture_entry(
            "signed-envelope-minimal",
            schema_ref="SignedEnvelope",
            purpose="Canonical minimal signed envelope shape used across POST /mesh routes.",
            value={
                "request": {
                    "node_id": "fixture-node",
                    "timestamp": "2026-01-01T00:00:00Z",
                    "nonce": "fixture-nonce",
                    "request_id": "fixture-request",
                    "protocol_family": "Open Compute Protocol",
                    "protocol_release": "0.1",
                    "implementation": "Sovereign Mesh",
                    "protocol_version": "sovereign-mesh/v1",
                    "signature_scheme": "ed25519",
                    "signature": "fixture-signature",
                },
                "body": {"artifact": {"descriptor": {"artifact_id": "artifact-fixture", "digest": "f00d"}}},
            },
        ),
        _fixture_entry(
            "mesh-manifest-minimal",
            schema_ref="MeshManifest",
            purpose="Minimal live-manifest shape advertised by a node.",
            value={
                "protocol": "Open Compute Protocol",
                "protocol_short_name": "OCP",
                "protocol_release": "0.1",
                "protocol_version": "sovereign-mesh/v1",
                "implementation": {"name": "Sovereign Mesh"},
                "organism_card": {
                    "organism_id": "fixture-node",
                    "node_id": "fixture-node",
                    "display_name": "Fixture Node",
                    "public_key": "fixture-public-key",
                    "endpoint_url": "http://127.0.0.1:8421",
                    "protocol_version": "sovereign-mesh/v1",
                    "trust_tier": "self",
                    "device_profile": {"device_class": "full", "form_factor": "workstation"},
                    "continuity_capabilities": {"mission_continuity": True, "custody_review": True},
                    "treaty_capabilities": {"treaty_documents": True, "continuity_validation": True},
                },
                "device_profile": {"device_class": "full", "form_factor": "workstation"},
                "sync_policy": {"sleep_capable": False},
                "continuity_capabilities": {"mission_continuity": True, "custody_review": True},
                "treaty_capabilities": {"treaty_documents": True, "continuity_validation": True},
                "governance_summary": {"active_treaty_ids": ["treaty/fixture-v1"]},
            },
        ),
        _fixture_entry(
            "job-submission-request",
            schema_ref="JobSubmissionRequest",
            purpose="Representative queued Python job submission body.",
            value={
                "job": {
                    "kind": "python.inline",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["python"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"code": "print('fixture job')"},
                    "metadata": {"origin": "conformance-fixture"},
                }
            },
        ),
        _fixture_entry(
            "artifact-minimal",
            schema_ref="Artifact",
            purpose="Artifact descriptor/result shape returned by /mesh/artifacts endpoints.",
            value={
                "id": "artifact-fixture",
                "digest": "abc123",
                "media_type": "application/json",
                "artifact_kind": "bundle",
                "size_bytes": 42,
                "metadata": {"job_id": "job-fixture"},
                "descriptor": {
                    "artifact_id": "artifact-fixture",
                    "digest": "abc123",
                    "media_type": "application/json",
                    "size_bytes": 42,
                    "artifact_kind": "bundle",
                },
                "created_at": "2026-01-01T00:00:00Z",
            },
        ),
        _fixture_entry(
            "continuity-restore-request",
            schema_ref="ContinuityRestorePlanRequest",
            purpose="Dry-run restore planning request for a continuity vessel.",
            value={
                "artifact_id": "vessel-fixture",
                "target_peer_id": "trusted-peer",
                "constraints": {"prefer_device_classes": ["relay", "full"]},
            },
        ),
        _fixture_entry(
            "treaty-audit-request",
            schema_ref="TreatyAuditRequest",
            purpose="Treaty validation request for continuity-bound operations.",
            value={
                "treaty_requirements": ["treaty/fixture-v1"],
                "operation": "continuity_restore",
                "metadata": {"source": "contract-fixture"},
            },
        ),
        _fixture_entry(
            "contract-snapshot-minimal",
            schema_ref="ContractSnapshot",
            purpose="Minimal /mesh/contract response shape.",
            value={
                "status": "ok",
                "contract_version": "ocp-http-contract/v1alpha1",
                "schema_version": SCHEMA_VERSION,
                "protocol_surface": "/mesh/*",
                "endpoint_count": 1,
                "schema_count": 1,
                "groups": {"runtime": {"count": 1, "methods": ["GET"]}},
                "endpoints": [
                    {
                        "id": "get:/mesh/manifest",
                        "method": "GET",
                        "group": "runtime",
                        "path": "/mesh/manifest",
                        "handler": "_handle_mesh_manifest",
                        "request": {"argument_kind": "none"},
                        "response": {"type": "object", "schema_ref": "MeshManifest", "schema_available": True},
                    }
                ],
                "schemas": {"MeshManifest": {"type": "object"}},
                "conformance": {
                    "status": "ok",
                    "fixture_count": 0,
                    "invalid_fixture_count": 0,
                    "fixtures": [],
                },
            },
        ),
    ]
    invalid_fixture_count = sum(1 for fixture in fixtures if fixture["validation"]["status"] != "ok")
    return {
        "status": "ok" if invalid_fixture_count == 0 else "attention_needed",
        "schema_version": SCHEMA_VERSION,
        "fixture_count": len(fixtures),
        "invalid_fixture_count": invalid_fixture_count,
        "fixtures": fixtures,
    }


__all__ = ["build_protocol_conformance_snapshot"]
