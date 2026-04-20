class MeshError(RuntimeError):
    pass


class MeshSignatureError(MeshError):
    pass


class MeshReplayError(MeshError):
    pass


class MeshPolicyError(MeshError):
    pass


class MeshArtifactAccessError(MeshError):
    pass
