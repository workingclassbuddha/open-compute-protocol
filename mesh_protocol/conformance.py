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
                    "signature_scheme": "ed25519-sha512-v1",
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
            "artifact-replicate-request-operator-mediated",
            schema_ref="ArtifactReplicateRequest",
            purpose="Explicit operator-mediated artifact pull request without persisting remote credentials.",
            value={
                "peer_id": "beta-node",
                "artifact_id": "artifact-fixture",
                "pin": True,
                "remote_auth": {"type": "operator_token", "token": "fixture-token"},
            },
        ),
        _fixture_entry(
            "artifact-replicate-response-redacted-auth",
            schema_ref="ArtifactReplicateResponse",
            purpose="Artifact replication response with route proof and redacted remote auth metadata.",
            value={
                "status": "replicated",
                "artifact": {
                    "id": "artifact-local",
                    "digest": "abc123",
                    "media_type": "application/json",
                    "artifact_kind": "bundle",
                },
                "source": {"peer_id": "beta-node", "artifact_id": "artifact-fixture", "digest": "abc123"},
                "verification": {"status": "verified", "verified": True},
                "route_proof": {"status": "fresh", "best_route": "http://192.168.1.22:8421"},
                "remote_auth": {"type": "operator_token", "status": "used", "redacted": True},
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
            "route-health-reachable",
            schema_ref="RouteHealth",
            purpose="A proven HTTP route for a nearby trusted peer.",
            value={
                "peer_id": "beta-node",
                "display_name": "Beta",
                "status": "reachable",
                "best_route": "http://192.168.1.22:8421",
                "last_reachable_base_url": "http://192.168.1.22:8421",
                "checked_at": "2026-01-01T00:00:00Z",
                "last_success_at": "2026-01-01T00:00:00Z",
                "last_error": "",
                "freshness": "fresh",
                "age_seconds": 0,
                "failure_count": 0,
                "next_probe_after": "",
                "operator_hint": "",
                "operator_summary": "Beta is reachable at http://192.168.1.22:8421.",
                "candidates": [
                    {
                        "base_url": "http://192.168.1.22:8421",
                        "source": "last_reachable",
                        "status": "reachable",
                        "latency_ms": 12,
                        "checked_at": "2026-01-01T00:00:00Z",
                        "last_success_at": "2026-01-01T00:00:00Z",
                        "last_error": "",
                        "freshness": "fresh",
                        "failure_count": 0,
                        "next_probe_after": "",
                        "operator_hint": "",
                    }
                ],
            },
        ),
        _fixture_entry(
            "autonomic-activate-request",
            schema_ref="AutonomicActivateRequest",
            purpose="Assisted one-button mesh activation request from the phone control surface.",
            value={
                "mode": "assisted",
                "limit": 24,
                "scan_timeout": 0.8,
                "timeout": 3.0,
                "run_proof": True,
                "repair": True,
                "max_enlist": 2,
                "actor_agent_id": "ocp-mobile-ui",
                "request_id": "autonomic-fixture",
            },
        ),
        _fixture_entry(
            "app-status-operator-home",
            schema_ref="AppStatus",
            purpose="Compact operator-facing status used by the installable OCP app home.",
            value={
                "status": "ok",
                "node": {
                    "node_id": "alpha-node",
                    "display_name": "Alpha",
                    "device_class": "full",
                    "form_factor": "laptop",
                    "protocol_release": "0.1",
                    "protocol_version": "sovereign-mesh/v1",
                },
                "app_urls": {
                    "base_url": "http://192.168.1.10:8421",
                    "app_url": "http://192.168.1.10:8421/app",
                    "setup_url": "http://192.168.1.10:8421/easy",
                    "control_url": "http://192.168.1.10:8421/control",
                    "phone_url": "http://192.168.1.10:8421/app",
                    "lan_urls": ["http://192.168.1.10:8421/app"],
                    "sharing_mode": "lan",
                    "share_advice": "",
                },
                "mesh_quality": {
                    "status": "strong",
                    "label": "Mesh strong",
                    "peer_count": 1,
                    "route_count": 1,
                    "healthy_routes": 1,
                    "operator_summary": "Mesh is strong.",
                },
                "setup": {
                    "status": "strong",
                    "label": "Mesh strong",
                    "primary_action": "activate_mesh",
                    "bind_mode": "lan",
                    "phone_url": "http://192.168.1.10:8421/app",
                    "token_status": "configured",
                    "known_peer_count": 1,
                    "healthy_route_count": 1,
                    "route_count": 1,
                    "latest_proof_status": "completed",
                    "recovery_state": "healthy",
                    "primary_peer": {
                        "peer_id": "beta-node",
                        "display_name": "Beta",
                        "role": "compute",
                        "status": "ready",
                        "route": "http://192.168.1.22:8421",
                        "summary": "Beta is best for compute right now.",
                    },
                    "device_roles": [
                        {
                            "peer_id": "alpha-node",
                            "display_name": "Alpha",
                            "role": "local_command",
                            "status": "ready",
                            "summary": "This Mac is the local command node.",
                        },
                        {
                            "peer_id": "beta-node",
                            "display_name": "Beta",
                            "role": "compute",
                            "status": "ready",
                            "summary": "Beta is ready for compute work.",
                        },
                    ],
                    "blocking_issue": "",
                    "blocker_code": "",
                    "next_fix": "No fix needed. The current mesh proof completed.",
                    "operator_summary": "Mesh is strong. Devices have proven routes and the latest proof completed.",
                    "story": [
                        "Mesh is strong.",
                        "Beta is best for compute right now.",
                        "Whole-mesh proof completed.",
                    ],
                    "timeline": [
                        {
                            "kind": "proof_completed",
                            "status": "ok",
                            "summary": "Whole-mesh proof completed.",
                            "created_at": "2026-01-01T00:00:00Z",
                        }
                    ],
                },
                "protocol": {
                    "release": "0.1",
                    "version": "sovereign-mesh/v1",
                    "schema_version": SCHEMA_VERSION,
                    "contract_url": "/mesh/contract",
                },
                "autonomy": {"status": "ok", "mode": "assisted", "operator_summary": "Mesh is strong."},
                "route_health": {
                    "status": "ok",
                    "count": 1,
                    "healthy": 1,
                    "routes": [
                        {
                            "peer_id": "beta-node",
                            "status": "reachable",
                            "candidates": [],
                        }
                    ],
                },
                "execution_readiness": {
                    "status": "ready",
                    "local": {"worker_count": 1, "ready_worker_count": 1},
                    "targets": [{"peer_id": "alpha-node", "status": "ready", "reasons": ["local worker registered"]}],
                    "worker_capacity": [
                        {
                            "worker_id": "alpha-default-worker",
                            "peer_id": "alpha-node",
                            "status": "active",
                            "capabilities": ["worker-runtime", "shell"],
                            "resources": {"cpu": 1},
                            "max_concurrent_jobs": 1,
                            "available_slots": 1,
                        }
                    ],
                    "operator_summary": "Execution is ready.",
                },
                "artifact_sync": {
                    "status": "verified",
                    "replicated_count": 1,
                    "verified_count": 1,
                    "items": [],
                    "operator_summary": "1 replicated artifact(s) verified.",
                },
                "latest_proof": {
                    "status": "completed",
                    "mission_id": "mission-fixture",
                    "title": "Whole Mesh Test Mission",
                    "summary": "Whole Mesh Test Mission is completed.",
                },
                "approvals": {"pending_count": 0, "items": [], "operator_summary": "No approvals are waiting."},
                "next_actions": ["Mesh is ready."],
                "generated_at": "2026-01-01T00:00:00Z",
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
