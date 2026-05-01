from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any


SENSITIVE_GRANT_FIELDS = {"signature", "nonce", "proof", "token", "secret"}


def _parse_datetime(value: Any) -> datetime | None:
    token = str(value or "").strip()
    if not token:
        return None
    if token.endswith("Z"):
        token = f"{token[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(token)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _issue(path: str, message: str) -> dict[str, str]:
    return {"path": path, "message": message}


def validate_capability_grant(grant: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    payload = dict(grant or {})
    issues: list[dict[str, str]] = []
    for field in (
        "grant_id",
        "issuer_peer_id",
        "subject_peer_id",
        "audience_peer_id",
        "scope",
        "issued_at",
        "expires_at",
        "signature_scheme",
        "signature",
    ):
        value = payload.get(field)
        if field not in payload or value is None or value == "":
            issues.append(_issue(f"$.{field}", "required field is missing"))

    scope = payload.get("scope")
    if not isinstance(scope, dict):
        issues.append(_issue("$.scope", "scope must be an object"))
    elif not str(scope.get("action") or "").strip():
        issues.append(_issue("$.scope.action", "scope action is required"))

    expires_at = _parse_datetime(payload.get("expires_at"))
    if expires_at is None:
        issues.append(_issue("$.expires_at", "expires_at must be an ISO-8601 timestamp"))
    else:
        comparison_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        if expires_at <= comparison_time:
            issues.append(_issue("$.expires_at", "capability grant is expired"))

    status = "ok"
    if any(issue["path"] == "$.expires_at" and "expired" in issue["message"] for issue in issues):
        status = "expired"
    elif issues:
        status = "invalid"
    return {
        "status": status,
        "grant_id": str(payload.get("grant_id") or "").strip(),
        "issuer_peer_id": str(payload.get("issuer_peer_id") or "").strip(),
        "subject_peer_id": str(payload.get("subject_peer_id") or "").strip(),
        "audience_peer_id": str(payload.get("audience_peer_id") or "").strip(),
        "scope": deepcopy(scope) if isinstance(scope, dict) else {},
        "expires_at": str(payload.get("expires_at") or "").strip(),
        "issues": issues,
    }


def redact_capability_grant(grant: dict[str, Any], *, status: str = "declared") -> dict[str, Any]:
    payload = dict(grant or {})
    scope = payload.get("scope")
    redacted = {
        "type": "capability_grant",
        "status": str(status or "declared").strip(),
        "redacted": True,
        "grant_id": str(payload.get("grant_id") or "").strip(),
        "issuer_peer_id": str(payload.get("issuer_peer_id") or "").strip(),
        "subject_peer_id": str(payload.get("subject_peer_id") or "").strip(),
        "audience_peer_id": str(payload.get("audience_peer_id") or "").strip(),
        "scope": deepcopy(scope) if isinstance(scope, dict) else {},
        "expires_at": str(payload.get("expires_at") or "").strip(),
    }
    return {key: value for key, value in redacted.items() if value not in ("", {}, [])}
