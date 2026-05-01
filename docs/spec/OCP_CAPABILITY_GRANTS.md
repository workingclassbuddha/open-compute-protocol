# OCP Capability Grants Alpha

This document defines the first alpha shape for signed scoped capability grants. It is a protocol-boundary draft, not a complete authorization system.

## Status

Capability grants are currently:

- described in `mesh_protocol` schemas as `CapabilityGrant`
- included in conformance fixtures
- accepted as a declared `remote_auth` shape for artifact replication summaries
- expiry-checked and redacted before being stored or returned

Capability grants are not yet:

- cryptographically verified by the artifact HTTP content gate
- used as a full replacement for operator-token private artifact pulls
- backed by durable revocation or key rotation
- enforced as a complete peer permission system

Operator-token `remote_auth` remains the working fallback for private artifact replication in this alpha.

## Grant Shape

```json
{
  "grant_id": "grant-alpha-private-artifact",
  "issuer_peer_id": "beta-node",
  "subject_peer_id": "alpha-node",
  "audience_peer_id": "beta-node",
  "scope": {
    "action": "artifact.read",
    "artifact_id": "artifact-private",
    "digest": "abc123",
    "max_uses": 1
  },
  "issued_at": "2026-01-01T00:00:00Z",
  "expires_at": "2026-01-01T00:05:00Z",
  "nonce": "grant-nonce",
  "signature_scheme": "ed25519-sha512-v1",
  "signature": "..."
}
```

## Scope

The initial intended scope is private artifact read/replication:

- `action`: `artifact.read`
- `artifact_id`: optional artifact id constraint
- `digest`: optional digest constraint
- `max_uses`: optional replay-budget hint
- `constraints`: extension object for future restrictions

Future scopes may cover route probing, worker claims, mission restore, or other peer operations.

## Redaction

Grant proof material must not be stored or echoed. Redacted summaries keep only:

- type
- status
- grant id
- issuer, subject, and audience peer ids
- scope
- expiry
- `redacted: true`

They must remove signatures, nonces, tokens, proof payloads, and secrets.

## Validation

Alpha validation checks:

- required fields
- scope object and action
- ISO-8601 expiry
- expiry relative to current time

It does not yet verify signatures. Signature verification is required before grants can authorize private content over HTTP.

## Artifact Replication

Artifact replication now has two documented `remote_auth` paths:

- `operator_token`: working fallback for private remote pulls
- `capability_grant`: declared alpha grant path with validation and redaction

Until grant enforcement lands, private HTTP artifact content still requires operator authorization or public artifact policy.

## Next Enforcement Steps

1. Canonicalize the grant signing payload.
2. Verify grant signatures against issuer peer keys.
3. Check audience and subject against the serving and requesting nodes.
4. Enforce artifact id and digest constraints in the HTTP content gate.
5. Add replay/use tracking for `max_uses`.
6. Add revocation and key rotation.
7. Keep operator-token remote auth as an explicit fallback during migration.
