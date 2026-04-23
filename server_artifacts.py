from __future__ import annotations

from typing import Any

from mesh import SovereignMesh


def _extract_path_id(path: str, prefix: str) -> str:
    return str(path or "").split(prefix, 1)[1]


def publish_artifact(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.accept_artifact_publish(data)


def list_artifacts(
    mesh: SovereignMesh,
    *,
    limit: int = 25,
    artifact_kind: str = "",
    digest: str = "",
    job_id: str = "",
    attempt_id: str = "",
    parent_artifact_id: str = "",
    owner_peer_id: str = "",
    media_type: str = "",
    retention_class: str = "",
) -> dict[str, Any]:
    return mesh.list_artifacts(
        limit=limit,
        artifact_kind=artifact_kind,
        digest=digest,
        job_id=job_id,
        attempt_id=attempt_id,
        parent_artifact_id=parent_artifact_id,
        owner_peer_id=owner_peer_id,
        media_type=media_type,
        retention_class=retention_class,
    )


def get_artifact(
    mesh: SovereignMesh,
    artifact_id: str,
    *,
    requester_peer_id: str = "",
    include_content: bool = True,
) -> dict[str, Any]:
    return mesh.get_artifact(
        str(artifact_id or "").strip(),
        requester_peer_id=requester_peer_id,
        include_content=include_content,
    )


def get_artifact_from_path(
    mesh: SovereignMesh,
    path: str,
    params: dict[str, list[str]],
    *,
    requester_peer_id: str = "",
) -> dict[str, Any]:
    return get_artifact(
        mesh,
        _extract_path_id(path, "/mesh/artifacts/"),
        requester_peer_id=requester_peer_id,
        include_content=params.get("include_content", ["1"])[0] != "0",
    )


def purge_expired_artifacts(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.purge_expired_artifacts(limit=int(data.get("limit") or 100))


def replicate_artifact(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.replicate_artifact_from_peer(
        (data.get("peer_id") or "").strip(),
        artifact_id=(data.get("artifact_id") or "").strip(),
        digest=(data.get("digest") or "").strip(),
        pin=bool(data.get("pin", False)),
    )


def replicate_artifact_graph(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.replicate_artifact_graph_from_peer(
        (data.get("peer_id") or "").strip(),
        artifact_id=(data.get("artifact_id") or "").strip(),
        digest=(data.get("digest") or "").strip(),
        pin=bool(data.get("pin", False)),
    )


def set_artifact_pin(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "ok",
        "artifact": mesh.set_artifact_pin(
            (data.get("artifact_id") or "").strip(),
            pinned=bool(data.get("pinned", True)),
            reason=(data.get("reason") or "operator_pin").strip(),
        ),
    }


def verify_artifact_mirror(mesh: SovereignMesh, data: dict[str, Any]) -> dict[str, Any]:
    return mesh.verify_artifact_mirror(
        (data.get("artifact_id") or "").strip(),
        peer_id=(data.get("peer_id") or "").strip(),
        source_artifact_id=(data.get("source_artifact_id") or "").strip(),
        digest=(data.get("digest") or "").strip(),
    )


__all__ = [
    "get_artifact",
    "get_artifact_from_path",
    "list_artifacts",
    "publish_artifact",
    "purge_expired_artifacts",
    "replicate_artifact",
    "replicate_artifact_graph",
    "set_artifact_pin",
    "verify_artifact_mirror",
]
