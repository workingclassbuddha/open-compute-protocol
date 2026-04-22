from __future__ import annotations

from importlib import import_module

__all__ = [
    "GolemMeshAdapter",
    "HostMeshAdapter",
    "MeshArtifactAccessError",
    "MeshPeerClient",
    "MeshPolicyError",
    "MeshReplayError",
    "MeshSignatureError",
    "PersonalMirrorMeshAdapter",
    "SovereignMesh",
]

_SOVEREIGN_EXPORTS = set(__all__)


def __getattr__(name: str):
    if name not in _SOVEREIGN_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    sovereign = import_module(".sovereign", __name__)
    value = getattr(sovereign, name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | _SOVEREIGN_EXPORTS)
