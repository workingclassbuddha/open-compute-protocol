from __future__ import annotations

from typing import Any

from mesh import SovereignMesh


def _extract_path_id(path: str, prefix: str, suffix: str = "") -> str:
    token = str(path or "")
    if suffix:
        token = token[: -len(suffix)]
    return token[len(prefix) :].strip("/")


def get_manifest(mesh: SovereignMesh) -> dict[str, Any]:
    return mesh.get_manifest()


def get_device_profile(mesh: SovereignMesh) -> dict[str, Any]:
    return {"status": "ok", "device_profile": dict(mesh.device_profile)}


def update_device_profile(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.update_device_profile(dict(data.get("device_profile") or {}))


def list_peers(mesh: SovereignMesh, *, limit: int = 25) -> dict[str, Any]:
    return mesh.list_peers(limit=limit)


def stream_snapshot(mesh: SovereignMesh, *, since_seq: int = 0, limit: int = 50) -> dict[str, Any]:
    return mesh.stream_snapshot(since_seq=since_seq, limit=limit)


def accept_handshake(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.accept_handshake(data)


def acquire_lease(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.acquire_lease(**dict(data or {}))


def heartbeat_lease(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.heartbeat_lease(
        (data.get("lease_id") or "").strip(),
        ttl_seconds=int(data.get("ttl_seconds") or 300),
    )


def release_lease(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.release_lease(
        (data.get("lease_id") or "").strip(),
        status=(data.get("status") or "released").strip(),
    )


def heartbeat_attempt(mesh: SovereignMesh, attempt_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "ok",
        "attempt": mesh.heartbeat_job_attempt(
            str(attempt_id or "").strip(),
            ttl_seconds=int(data.get("ttl_seconds") or 300),
            metadata=dict(data.get("metadata") or {}),
        ),
    }


def heartbeat_attempt_from_path(mesh: SovereignMesh, path: str, data: dict[str, Any]) -> dict[str, Any]:
    return heartbeat_attempt(mesh, _extract_path_id(path, "/mesh/jobs/attempts/", "/heartbeat"), data)


def complete_attempt(mesh: SovereignMesh, attempt_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.complete_job_attempt(
        str(attempt_id or "").strip(),
        data.get("result"),
        media_type=(data.get("media_type") or "application/json").strip(),
        executor=(data.get("executor") or "").strip(),
        metadata=dict(data.get("metadata") or {}),
    )


def complete_attempt_from_path(mesh: SovereignMesh, path: str, data: dict[str, Any]) -> dict[str, Any]:
    return complete_attempt(mesh, _extract_path_id(path, "/mesh/jobs/attempts/", "/complete"), data)


def fail_attempt(mesh: SovereignMesh, attempt_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.fail_job_attempt(
        str(attempt_id or "").strip(),
        error=(data.get("error") or "job attempt failed").strip(),
        retryable=bool(data.get("retryable", True)),
        metadata=dict(data.get("metadata") or {}),
    )


def fail_attempt_from_path(mesh: SovereignMesh, path: str, data: dict[str, Any]) -> dict[str, Any]:
    return fail_attempt(mesh, _extract_path_id(path, "/mesh/jobs/attempts/", "/fail"), data)


def accept_handoff(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.accept_handoff(data)


__all__ = [
    "accept_handshake",
    "accept_handoff",
    "acquire_lease",
    "complete_attempt",
    "complete_attempt_from_path",
    "fail_attempt",
    "fail_attempt_from_path",
    "get_device_profile",
    "get_manifest",
    "heartbeat_attempt",
    "heartbeat_attempt_from_path",
    "heartbeat_lease",
    "list_peers",
    "release_lease",
    "stream_snapshot",
    "update_device_profile",
]
