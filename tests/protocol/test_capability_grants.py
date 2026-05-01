from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from mesh_artifacts.service import MeshArtifactService
from mesh_protocol.conformance import build_protocol_conformance_snapshot
from mesh_protocol.capability_grants import (
    redact_capability_grant,
    validate_capability_grant,
)
from mesh_protocol.schemas import get_protocol_schema, validate_protocol_object


def fixture_grant(**overrides):
    grant = {
        "grant_id": "grant-alpha-private-artifact",
        "issuer_peer_id": "beta-node",
        "subject_peer_id": "alpha-node",
        "audience_peer_id": "beta-node",
        "scope": {
            "action": "artifact.read",
            "artifact_id": "artifact-private",
            "digest": "abc123",
            "max_uses": 1,
        },
        "issued_at": "2026-01-01T00:00:00Z",
        "expires_at": "2026-01-01T00:05:00Z",
        "nonce": "grant-nonce",
        "signature_scheme": "ed25519-sha512-v1",
        "signature": "fixture-signature",
    }
    grant.update(overrides)
    return grant


class CapabilityGrantProtocolTests(unittest.TestCase):
    def test_capability_grant_schema_accepts_scoped_artifact_read_grant(self):
        grant = fixture_grant()

        self.assertIn("CapabilityGrant", get_protocol_schema("CapabilityGrant")["title"])
        validation = validate_protocol_object("CapabilityGrant", grant)

        self.assertEqual(validation["status"], "ok")

    def test_capability_grant_validation_rejects_expired_grants(self):
        grant = fixture_grant(expires_at="2026-01-01T00:00:30Z")

        validation = validate_capability_grant(
            grant,
            now=datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(validation["status"], "expired")
        self.assertIn("expires_at", validation["issues"][0]["path"])

    def test_capability_grant_redaction_keeps_scope_but_removes_signature_material(self):
        grant = fixture_grant(signature="sensitive-signature", nonce="sensitive-nonce")

        redacted = redact_capability_grant(grant)

        self.assertEqual(redacted["type"], "capability_grant")
        self.assertEqual(redacted["status"], "declared")
        self.assertEqual(redacted["grant_id"], "grant-alpha-private-artifact")
        self.assertEqual(redacted["scope"]["action"], "artifact.read")
        self.assertTrue(redacted["redacted"])
        serialized = repr(redacted)
        self.assertNotIn("sensitive-signature", serialized)
        self.assertNotIn("sensitive-nonce", serialized)

    def test_artifact_remote_auth_summary_accepts_unexpired_capability_grant(self):
        mesh = SimpleNamespace()
        mesh.MeshPolicyError = ValueError
        service = MeshArtifactService.__new__(MeshArtifactService)
        service.mesh = mesh

        summary = service._remote_auth_summary(
            {"type": "capability_grant", "grant": fixture_grant()},
            now=datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(summary["type"], "capability_grant")
        self.assertEqual(summary["status"], "declared")
        self.assertEqual(summary["grant_id"], "grant-alpha-private-artifact")
        self.assertTrue(summary["redacted"])

    def test_conformance_snapshot_includes_capability_grant_fixture(self):
        snapshot = build_protocol_conformance_snapshot()
        fixture_ids = {fixture["id"] for fixture in snapshot["fixtures"]}

        self.assertIn("capability-grant-artifact-read", fixture_ids)
        self.assertIn("artifact-replicate-request-capability-grant", fixture_ids)
        self.assertEqual(snapshot["invalid_fixture_count"], 0)


if __name__ == "__main__":
    unittest.main()
