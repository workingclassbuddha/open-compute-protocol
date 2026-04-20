from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Optional


class MeshArtifactService:
    """Content-addressed artifact lifecycle helpers for SovereignMesh."""

    def __init__(
        self,
        mesh,
        *,
        b64decode,
        b64encode,
        loads_json,
        normalize_policy,
        normalize_retention_class,
        oci_digest,
        sha256_bytes,
        utcnow,
    ):
        self.mesh = mesh
        self._b64decode = b64decode
        self._b64encode = b64encode
        self._loads_json = loads_json
        self._normalize_policy = normalize_policy
        self._normalize_retention_class = normalize_retention_class
        self._oci_digest = oci_digest
        self._sha256_bytes = sha256_bytes
        self._utcnow = utcnow

    def artifact_path(self, artifact_id: str) -> Path:
        return self.mesh.artifact_root / f"{artifact_id}.blob"

    def artifact_retention_policy(self, *, policy: Optional[dict], metadata: Optional[dict]) -> dict:
        artifact_metadata = dict(metadata or {})
        if self.artifact_is_pinned({"metadata": artifact_metadata}):
            return {
                "retention_class": "durable",
                "retention_seconds": 0,
                "retention_deadline_at": "",
            }
        artifact_kind = str(artifact_metadata.get("artifact_kind") or "").strip().lower()
        raw_retention = artifact_metadata.get("retention_class") or artifact_metadata.get("retention")
        if not raw_retention:
            raw_retention = dict(policy or {}).get("retention") or ""
        if raw_retention:
            retention_class = self._normalize_retention_class(str(raw_retention))
        elif artifact_kind == "log":
            retention_class = "session"
        else:
            retention_class = "durable"
        raw_seconds = artifact_metadata.get("retention_seconds")
        if raw_seconds in (None, ""):
            raw_seconds = dict(policy or {}).get("retention_seconds")
        if raw_seconds in (None, "") and isinstance(dict(policy or {}).get("retention"), (int, float)):
            raw_seconds = dict(policy or {}).get("retention")
        try:
            retention_seconds = max(0, int(raw_seconds))
        except Exception:
            retention_seconds = self.mesh.ARTIFACT_RETENTION_DEFAULTS.get(retention_class, 0)
        return {
            "retention_class": retention_class,
            "retention_seconds": retention_seconds,
            "retention_deadline_at": self.mesh._utc_after(retention_seconds) if retention_seconds > 0 else "",
        }

    def delete_artifact_row(self, row, *, reason: str = "retention_expired") -> None:
        path = Path(row["path"])
        try:
            path.unlink(missing_ok=True)
        except TypeError:
            if path.exists():
                path.unlink()
        with self.mesh._conn() as conn:
            conn.execute("DELETE FROM mesh_artifacts WHERE id=?", (row["id"],))
            conn.commit()
        self.mesh._record_event(
            "mesh.artifact.purged",
            peer_id=(row["owner_peer_id"] or "").strip() or self.mesh.node_id,
            payload={
                "artifact_id": row["id"],
                "digest": row["digest"],
                "reason": str(reason or "").strip(),
                "retention_class": row["retention_class"] or "durable",
            },
        )

    def artifact_metadata_dict(self, value: Any) -> dict:
        metadata = self._loads_json(value, {})
        return dict(metadata or {}) if isinstance(metadata, dict) else {}

    def artifact_is_pinned(self, artifact_like: Any) -> bool:
        metadata = self.artifact_metadata_dict(
            artifact_like.get("metadata") if isinstance(artifact_like, dict) else artifact_like["metadata"]
        )
        return bool(metadata.get("pinned")) or bool(dict(metadata.get("artifact_sync") or {}).get("pinned"))

    def artifact_row(self, artifact_id: str):
        with self.mesh._conn() as conn:
            return conn.execute("SELECT * FROM mesh_artifacts WHERE id=?", ((artifact_id or "").strip(),)).fetchone()

    def update_artifact_record(
        self,
        artifact_id: str,
        *,
        policy: Optional[dict] = None,
        metadata: Optional[dict] = None,
        retention_class: Optional[str] = None,
        retention_deadline_at: Optional[str] = None,
    ) -> dict:
        row = self.artifact_row(artifact_id)
        if row is None:
            raise self.mesh.MeshArtifactAccessError("artifact not found")
        updated_policy = self._normalize_policy(policy or self._loads_json(row["policy"], {}))
        updated_metadata = dict(self._loads_json(row["metadata"], {}))
        updated_metadata.update(dict(metadata or {}))
        updated_retention_class = (
            self._normalize_retention_class(retention_class)
            if retention_class is not None
            else (row["retention_class"] or "durable")
        )
        updated_retention_deadline = retention_deadline_at if retention_deadline_at is not None else (row["retention_deadline_at"] or "")
        with self.mesh._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_artifacts
                SET policy=?, metadata=?, retention_class=?, retention_deadline_at=?
                WHERE id=?
                """,
                (
                    json.dumps(updated_policy),
                    json.dumps(updated_metadata),
                    updated_retention_class,
                    updated_retention_deadline,
                    (artifact_id or "").strip(),
                ),
            )
            conn.commit()
            updated_row = conn.execute("SELECT * FROM mesh_artifacts WHERE id=?", ((artifact_id or "").strip(),)).fetchone()
        return self.mesh._row_to_artifact(updated_row)

    def purge_expired_rows(self, *, limit: int = 100) -> int:
        with self.mesh._conn() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM mesh_artifacts
                WHERE retention_deadline_at != ''
                  AND retention_deadline_at <= ?
                ORDER BY retention_deadline_at ASC, created_at ASC
                LIMIT ?
                """,
                (self._utcnow(), max(1, int(limit or 100))),
            ).fetchall()
        purged = 0
        for row in rows:
            if self.artifact_is_pinned(row):
                continue
            self.delete_artifact_row(row, reason="retention_expired")
            purged += 1
        return purged

    def publish_local_artifact(
        self,
        content: Any,
        *,
        media_type: str = "application/json",
        policy: Optional[dict] = None,
        metadata: Optional[dict] = None,
        owner_peer_id: Optional[str] = None,
    ) -> dict:
        if isinstance(content, bytes):
            payload_bytes = content
        elif isinstance(content, str):
            payload_bytes = content.encode("utf-8")
        else:
            payload_bytes = json.dumps(content, sort_keys=True, default=str).encode("utf-8")
        artifact_id = str(uuid.uuid4())
        digest = self._sha256_bytes(payload_bytes)
        path = self.artifact_path(artifact_id)
        path.write_bytes(payload_bytes)
        retention = self.artifact_retention_policy(policy=policy, metadata=metadata)
        artifact_metadata = dict(metadata or {})
        artifact_metadata.setdefault("content_sha256", digest)
        artifact_metadata.setdefault("oci_digest", self._oci_digest(digest))
        artifact_metadata.setdefault("size_bytes", len(payload_bytes))
        artifact_metadata.setdefault("media_type", media_type)
        ref = self.mesh.ArtifactRef(
            id=artifact_id,
            digest=digest,
            media_type=media_type,
            size_bytes=len(payload_bytes),
            owner_peer_id=(owner_peer_id or self.mesh.node_id),
            policy=self._normalize_policy(policy or {"classification": "trusted", "mode": "batch"}),
            path=str(path),
            created_at=self._utcnow(),
            metadata=artifact_metadata,
            retention_class=retention["retention_class"],
            retention_deadline_at=retention["retention_deadline_at"],
            download_url=f"{self.mesh.base_url}/mesh/artifacts/{artifact_id}",
        )
        with self.mesh._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_artifacts
                (id, digest, media_type, size_bytes, owner_peer_id, policy, path, metadata, retention_class, retention_deadline_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ref.id,
                    ref.digest,
                    ref.media_type,
                    ref.size_bytes,
                    ref.owner_peer_id,
                    json.dumps(ref.policy),
                    ref.path,
                    json.dumps(ref.metadata),
                    ref.retention_class,
                    ref.retention_deadline_at,
                    ref.created_at,
                ),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM mesh_artifacts WHERE id=?", (ref.id,)).fetchone()
        self.mesh._record_event(
            "mesh.artifact.published",
            peer_id=ref.owner_peer_id,
            payload={
                "artifact_id": ref.id,
                "digest": ref.digest,
                "media_type": ref.media_type,
                "retention_class": ref.retention_class,
            },
        )
        return self.mesh._row_to_artifact(row)

    def _reject_published_artifact(self, published: dict, *, message: str) -> None:
        artifact_path = Path(published["path"])
        try:
            artifact_path.unlink(missing_ok=True)
        except TypeError:
            if artifact_path.exists():
                artifact_path.unlink()
        with self.mesh._conn() as conn:
            conn.execute("DELETE FROM mesh_artifacts WHERE id=?", (published["id"],))
            conn.commit()
        raise self.mesh.MeshPolicyError(message)

    def accept_artifact_publish(self, envelope: dict) -> dict:
        peer_id, request_meta, body, _ = self.mesh._verify_envelope(envelope, route="/mesh/artifacts/publish")
        artifact = dict(body.get("artifact") or {})
        descriptor = dict(artifact.get("descriptor") or {})
        if artifact.get("json") is not None:
            content = artifact["json"]
            media_type = artifact.get("media_type") or "application/json"
        elif artifact.get("content_base64"):
            content = self._b64decode(artifact["content_base64"])
            media_type = artifact.get("media_type") or "application/octet-stream"
        else:
            content = artifact.get("content") or ""
            media_type = artifact.get("media_type") or "text/plain; charset=utf-8"
        published = self.publish_local_artifact(
            content,
            media_type=media_type,
            policy=artifact.get("policy") or {"classification": "trusted", "mode": "batch"},
            metadata={
                "request_id": request_meta.get("request_id"),
                "source_peer_id": peer_id,
                "provided_descriptor": descriptor,
                **dict(artifact.get("metadata") or {}),
            },
            owner_peer_id=peer_id,
        )
        expected_digest = (artifact.get("digest") or "").strip()
        expected_size = int(artifact.get("size_bytes") or descriptor.get("size") or 0)
        expected_media_type = str(descriptor.get("mediaType") or "").strip()
        expected_descriptor_digest = str(descriptor.get("digest") or "").strip()
        if expected_size and int(published["size_bytes"] or 0) != expected_size:
            self._reject_published_artifact(published, message="artifact size mismatch")
        if expected_media_type and expected_media_type != published["media_type"]:
            self._reject_published_artifact(published, message="artifact media type mismatch")
        if expected_digest and expected_digest != published["digest"]:
            self._reject_published_artifact(published, message="artifact digest mismatch")
        if expected_descriptor_digest and expected_descriptor_digest != self._oci_digest(published["digest"]):
            self._reject_published_artifact(published, message="artifact descriptor digest mismatch")
        return {"status": "published", "artifact": published}

    def get_artifact(self, artifact_id: str, *, requester_peer_id: str = "", include_content: bool = True) -> dict:
        self.purge_expired_rows(limit=20)
        with self.mesh._conn() as conn:
            row = conn.execute("SELECT * FROM mesh_artifacts WHERE id=?", ((artifact_id or "").strip(),)).fetchone()
        if row is None:
            raise self.mesh.MeshArtifactAccessError("artifact not found")
        if (
            not self.artifact_is_pinned(row)
            and (row["retention_deadline_at"] or "").strip()
            and (row["retention_deadline_at"] or "") <= self._utcnow()
        ):
            self.delete_artifact_row(row, reason="retention_expired")
            raise self.mesh.MeshArtifactAccessError("artifact expired")
        artifact = self.mesh._row_to_artifact(row)
        peer = self.mesh._row_to_peer(self.mesh._get_peer_row(requester_peer_id)) if requester_peer_id else None
        if requester_peer_id and not self.mesh._policy_allows_peer(artifact["policy"], peer):
            raise self.mesh.MeshArtifactAccessError("artifact policy denies access for peer")
        if include_content:
            payload_bytes = Path(artifact["path"]).read_bytes()
            artifact["content_base64"] = self._b64encode(payload_bytes)
        return artifact

    def list_artifacts(
        self,
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
    ) -> dict:
        self.purge_expired_rows(limit=max(20, int(limit or 25)))
        clauses: list[str] = []
        params: list[Any] = []
        owner_peer_token = (owner_peer_id or "").strip()
        if owner_peer_token:
            clauses.append("owner_peer_id=?")
            params.append(owner_peer_token)
        media_type_token = (media_type or "").strip()
        if media_type_token:
            clauses.append("media_type=?")
            params.append(media_type_token)
        digest_token = str(digest or "").strip().lower()
        if digest_token.startswith("sha256:"):
            digest_token = digest_token.split(":", 1)[1]
        if digest_token:
            clauses.append("digest=?")
            params.append(digest_token)
        retention_class_token = self._normalize_retention_class(retention_class) if retention_class else ""
        if retention_class_token:
            clauses.append("retention_class=?")
            params.append(retention_class_token)
        query = ["SELECT * FROM mesh_artifacts"]
        if clauses:
            query.append("WHERE " + " AND ".join(clauses))
        query.append("ORDER BY created_at DESC LIMIT ?")
        params.append(max(max(1, int(limit or 25)) * 12, 100))
        with self.mesh._conn() as conn:
            rows = conn.execute("\n".join(query), tuple(params)).fetchall()
        artifact_kind_token = (artifact_kind or "").strip().lower()
        job_id_token = (job_id or "").strip()
        attempt_id_token = (attempt_id or "").strip()
        parent_artifact_id_token = (parent_artifact_id or "").strip()
        artifacts = []
        for row in rows:
            artifact = self.mesh._row_to_artifact(row)
            metadata = dict(artifact.get("metadata") or {})
            if artifact_kind_token and (artifact.get("artifact_kind") or "").strip().lower() != artifact_kind_token:
                continue
            if job_id_token and (metadata.get("job_id") or "").strip() != job_id_token:
                continue
            if attempt_id_token and (metadata.get("attempt_id") or "").strip() != attempt_id_token:
                continue
            if parent_artifact_id_token and (metadata.get("parent_artifact_id") or "").strip() != parent_artifact_id_token:
                continue
            artifacts.append(artifact)
            if len(artifacts) >= max(1, int(limit or 25)):
                break
        return {
            "peer_id": self.mesh.node_id,
            "count": len(artifacts),
            "artifacts": artifacts,
            "filters": {
                "artifact_kind": artifact_kind_token,
                "digest": digest_token,
                "job_id": job_id_token,
                "attempt_id": attempt_id_token,
                "parent_artifact_id": parent_artifact_id_token,
                "owner_peer_id": owner_peer_token,
                "media_type": media_type_token,
                "retention_class": retention_class_token,
            },
        }

    def artifact_row_by_digest(self, digest: str):
        token = str(digest or "").strip().lower()
        if token.startswith("sha256:"):
            token = token.split(":", 1)[1]
        if not token:
            return None
        with self.mesh._conn() as conn:
            return conn.execute(
                "SELECT * FROM mesh_artifacts WHERE digest=? ORDER BY created_at DESC LIMIT 1",
                (token,),
            ).fetchone()

    def find_local_artifact_by_digest(self, digest: str) -> Optional[dict]:
        row = self.artifact_row_by_digest(digest)
        return self.mesh._row_to_artifact(row) if row is not None else None

    def resolve_remote_artifact(
        self,
        peer_id: str,
        *,
        artifact_id: str = "",
        digest: str = "",
        client=None,
        base_url: Optional[str] = None,
        include_content: bool = True,
    ) -> tuple[Any, dict, str]:
        peer_token = (peer_id or "").strip()
        artifact_token = (artifact_id or "").strip()
        digest_token = str(digest or "").strip().lower()
        if digest_token.startswith("sha256:"):
            digest_token = digest_token.split(":", 1)[1]
        if not artifact_token and not digest_token:
            raise self.mesh.MeshPolicyError("artifact_id or digest is required")
        remote_client, _ = self.mesh._resolve_peer_client(peer_token, client=client, base_url=base_url)
        if artifact_token:
            remote_artifact = remote_client.get_artifact(artifact_token, peer_id=self.mesh.node_id, include_content=include_content)
            return remote_client, remote_artifact, artifact_token
        listing = remote_client.list_artifacts(limit=1, digest=digest_token)
        if not list(listing.get("artifacts") or []):
            raise self.mesh.MeshArtifactAccessError("remote artifact not found")
        remote_ref = dict(listing["artifacts"][0] or {})
        artifact_token = str(remote_ref.get("id") or "").strip()
        remote_artifact = remote_client.get_artifact(artifact_token, peer_id=self.mesh.node_id, include_content=include_content)
        return remote_client, remote_artifact, artifact_token

    def artifact_json_payload(self, artifact: dict) -> dict:
        payload_bytes = b""
        if str(artifact.get("content_base64") or "").strip():
            payload_bytes = self._b64decode(artifact.get("content_base64") or "")
        elif artifact.get("path"):
            try:
                payload_bytes = Path(str(artifact.get("path") or "")).read_bytes()
            except Exception:
                payload_bytes = b""
        if not payload_bytes:
            return {}
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
            return dict(payload or {}) if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def artifact_graph_targets(self, artifact: dict) -> list[dict]:
        metadata = dict(artifact.get("metadata") or {})
        payload = self.artifact_json_payload(artifact)
        refs: list[dict] = []

        def add_ref(*, artifact_id: str = "", digest: str = "", reason: str = "", role: str = "") -> None:
            artifact_token = str(artifact_id or "").strip()
            digest_token = str(digest or "").strip().lower()
            if digest_token.startswith("sha256:"):
                digest_token = digest_token.split(":", 1)[1]
            if not artifact_token and not digest_token:
                return
            refs.append(
                {
                    "artifact_id": artifact_token,
                    "digest": digest_token,
                    "reason": str(reason or "").strip(),
                    "role": str(role or "").strip(),
                }
            )

        parent_artifact_id = str(metadata.get("parent_artifact_id") or "").strip()
        if parent_artifact_id:
            add_ref(artifact_id=parent_artifact_id, reason="parent_artifact")
        for key in ("primary_artifact_id", "subject_artifact_id", "config_artifact_id", "attestation_artifact_id"):
            if str(metadata.get(key) or "").strip():
                add_ref(artifact_id=str(metadata.get(key) or "").strip(), reason=f"metadata:{key}")
        kind = str(payload.get("kind") or "").strip().lower()
        if kind == "ocp.artifact.bundle":
            primary = dict(payload.get("primary") or {})
            add_ref(artifact_id=primary.get("id") or "", digest=primary.get("digest") or "", reason="bundle_primary", role=primary.get("role") or "")
            config = dict(payload.get("config") or {})
            add_ref(digest=config.get("digest") or "", reason="bundle_config")
            subject = dict(payload.get("subject") or {})
            add_ref(digest=subject.get("digest") or "", reason="bundle_subject")
            for descriptor in list(payload.get("descriptors") or []):
                descriptor_item = dict(descriptor or {})
                add_ref(
                    artifact_id=descriptor_item.get("id") or "",
                    digest=descriptor_item.get("digest") or "",
                    reason="bundle_descriptor",
                    role=descriptor_item.get("role") or "",
                )
        elif kind == "ocp.execution.attestation":
            subject = dict(payload.get("subject") or {})
            add_ref(artifact_id=subject.get("artifact_id") or "", digest=subject.get("digest") or "", reason="attestation_subject")
        elif kind == "ocp.artifact.config":
            result = dict(payload.get("result") or {})
            add_ref(artifact_id=result.get("artifact_id") or "", digest=result.get("digest") or "", reason="config_result")

        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for item in refs:
            key = (str(item.get("artifact_id") or "").strip(), str(item.get("digest") or "").strip().lower())
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def artifact_attempt_graph_targets(
        self,
        peer_id: str,
        *,
        remote_client,
        artifact: dict,
        max_items: int = 20,
    ) -> list[dict]:
        metadata = dict(artifact.get("metadata") or {})
        job_id = str(metadata.get("job_id") or "").strip()
        attempt_id = str(metadata.get("attempt_id") or "").strip()
        if not job_id:
            return []
        listing = remote_client.list_artifacts(limit=max(1, int(max_items or 20)), job_id=job_id, attempt_id=attempt_id)
        refs: list[dict] = []
        for item in list(listing.get("artifacts") or []):
            artifact_item = dict(item or {})
            refs.append(
                {
                    "artifact_id": str(artifact_item.get("id") or "").strip(),
                    "digest": str(artifact_item.get("digest") or "").strip().lower(),
                    "reason": "attempt_artifact_set",
                    "role": str(artifact_item.get("artifact_kind") or "").strip(),
                }
            )
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for item in refs:
            key = (item["artifact_id"], item["digest"])
            if key in seen or not any(key):
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def replicate_artifact_from_peer(
        self,
        peer_id: str,
        *,
        artifact_id: str = "",
        digest: str = "",
        client=None,
        base_url: Optional[str] = None,
        request_id: Optional[str] = None,
        pin: bool = False,
    ) -> dict:
        peer_token = (peer_id or "").strip()
        if not peer_token:
            raise self.mesh.MeshPolicyError("peer_id is required")
        artifact_token = (artifact_id or "").strip()
        digest_token = str(digest or "").strip().lower()
        if digest_token.startswith("sha256:"):
            digest_token = digest_token.split(":", 1)[1]
        if not artifact_token and not digest_token:
            raise self.mesh.MeshPolicyError("artifact_id or digest is required")

        local_hit = self.find_local_artifact_by_digest(digest_token) if digest_token else None
        if local_hit is not None:
            self.mesh._record_event(
                "mesh.artifact.sync.local_hit",
                peer_id=peer_token,
                request_id=request_id or "",
                payload={"artifact_id": local_hit["id"], "digest": local_hit["digest"]},
            )
            return {
                "status": "already_present",
                "artifact": local_hit,
                "source": {"peer_id": peer_token, "artifact_id": artifact_token, "digest": digest_token},
            }

        remote_client, remote_artifact, artifact_token = self.resolve_remote_artifact(
            peer_token,
            artifact_id=artifact_token,
            digest=digest_token,
            client=client,
            base_url=base_url,
            include_content=True,
        )
        remote_digest = str(remote_artifact.get("digest") or "").strip().lower()
        if not remote_digest:
            raise self.mesh.MeshPolicyError("remote artifact missing digest")
        if digest_token and remote_digest != digest_token:
            raise self.mesh.MeshPolicyError("remote artifact digest mismatch")

        local_hit = self.find_local_artifact_by_digest(remote_digest)
        if local_hit is not None:
            self.mesh._record_event(
                "mesh.artifact.sync.local_hit",
                peer_id=peer_token,
                request_id=request_id or "",
                payload={"artifact_id": local_hit["id"], "digest": local_hit["digest"]},
            )
            return {
                "status": "already_present",
                "artifact": local_hit,
                "source": {"peer_id": peer_token, "artifact_id": artifact_token, "digest": remote_digest},
            }

        content_base64 = str(remote_artifact.get("content_base64") or "").strip()
        if not content_base64:
            raise self.mesh.MeshPolicyError("remote artifact content missing")
        source_artifact_id = remote_artifact.get("id") or artifact_token
        verification = {
            "status": "verified",
            "verified": True,
            "reason": "replicated_from_peer",
            "checked_at": self._utcnow(),
            "peer_id": peer_token,
            "source_artifact_id": source_artifact_id,
            "local_digest": remote_digest,
            "remote_digest": remote_digest,
            "size_match": int(remote_artifact.get("size_bytes") or 0) == len(self._b64decode(content_base64)),
            "media_type_match": True,
            "descriptor_match": str((remote_artifact.get("oci_descriptor") or {}).get("digest") or "").strip() == self._oci_digest(remote_digest),
        }
        replicated = self.publish_local_artifact(
            self._b64decode(content_base64),
            media_type=remote_artifact.get("media_type") or "application/octet-stream",
            policy=remote_artifact.get("policy") or {"classification": "trusted", "mode": "batch"},
            metadata={
                **dict(remote_artifact.get("metadata") or {}),
                "replicated_from_peer_id": peer_token,
                "replicated_from_artifact_id": source_artifact_id,
                "replicated_from_download_url": remote_artifact.get("download_url") or "",
                "cas_origin_digest": remote_digest,
                "pinned": bool(pin),
                "mirror_verification": verification,
                "artifact_sync": {
                    "mode": "pull-through",
                    "pinned": bool(pin),
                    "source_peer_id": peer_token,
                    "source_artifact_id": source_artifact_id,
                    "source_digest": remote_digest,
                    "synced_at": self._utcnow(),
                    "verified_at": verification["checked_at"],
                    "verification_status": verification["status"],
                },
            },
            owner_peer_id=remote_artifact.get("owner_peer_id") or peer_token,
        )
        if replicated["digest"] != remote_digest:
            raise self.mesh.MeshPolicyError("replicated artifact digest mismatch")
        self.mesh._record_event(
            "mesh.artifact.replicated",
            peer_id=peer_token,
            request_id=request_id or "",
            payload={
                "artifact_id": replicated["id"],
                "digest": replicated["digest"],
                "source_artifact_id": source_artifact_id,
                "pinned": bool(pin),
            },
        )
        return {
            "status": "replicated",
            "artifact": replicated,
            "source": {"peer_id": peer_token, "artifact_id": source_artifact_id, "digest": remote_digest},
            "verification": verification,
        }

    def replicate_artifact_graph_from_peer(
        self,
        peer_id: str,
        *,
        artifact_id: str = "",
        digest: str = "",
        client=None,
        base_url: Optional[str] = None,
        request_id: Optional[str] = None,
        pin: bool = False,
    ) -> dict:
        peer_token = (peer_id or "").strip()
        if not peer_token:
            raise self.mesh.MeshPolicyError("peer_id is required")
        remote_client, remote_root, resolved_artifact_id = self.resolve_remote_artifact(
            peer_token,
            artifact_id=artifact_id,
            digest=digest,
            client=client,
            base_url=base_url,
            include_content=True,
        )
        root = self.replicate_artifact_from_peer(
            peer_token,
            artifact_id=resolved_artifact_id,
            digest=str(remote_root.get("digest") or "").strip(),
            client=remote_client,
            request_id=request_id,
            pin=pin,
        )
        pending = self.artifact_graph_targets(remote_root)
        pending.extend(self.artifact_attempt_graph_targets(peer_token, remote_client=remote_client, artifact=remote_root))
        seen: set[tuple[str, str]] = {
            (str(root["artifact"].get("id") or "").strip(), str(root["artifact"].get("digest") or "").strip().lower())
        }
        replicated_children: list[dict] = []
        for ref in pending:
            ref_id = str(ref.get("artifact_id") or "").strip()
            ref_digest = str(ref.get("digest") or "").strip().lower()
            key = (ref_id, ref_digest)
            if key in seen:
                continue
            seen.add(key)
            if not ref_id and not ref_digest:
                continue
            child = self.replicate_artifact_from_peer(
                peer_token,
                artifact_id=ref_id,
                digest=ref_digest,
                client=remote_client,
                request_id=request_id,
                pin=pin,
            )
            replicated_children.append(
                {
                    "status": child.get("status") or "",
                    "artifact": child.get("artifact") or {},
                    "source": child.get("source") or {},
                    "reason": str(ref.get("reason") or "").strip(),
                    "role": str(ref.get("role") or "").strip(),
                }
            )
        self.mesh._record_event(
            "mesh.artifact.graph.replicated",
            peer_id=peer_token,
            request_id=request_id or "",
            payload={
                "root_artifact_id": (root.get("artifact") or {}).get("id", ""),
                "root_digest": (root.get("artifact") or {}).get("digest", ""),
                "replicated_count": len(replicated_children) + 1,
                "pinned": bool(pin),
            },
        )
        return {
            "status": "replicated",
            "root": root,
            "artifacts": [root["artifact"], *[item["artifact"] for item in replicated_children if item.get("artifact")]],
            "graph": {
                "root_artifact_id": (root.get("artifact") or {}).get("id", ""),
                "root_digest": (root.get("artifact") or {}).get("digest", ""),
                "count": len(replicated_children) + 1,
                "linked": replicated_children,
            },
        }

    def set_artifact_pin(self, artifact_id: str, *, pinned: bool = True, reason: str = "operator_pin") -> dict:
        row = self.artifact_row(artifact_id)
        if row is None:
            raise self.mesh.MeshArtifactAccessError("artifact not found")
        metadata = dict(self._loads_json(row["metadata"], {}))
        artifact_sync = dict(metadata.get("artifact_sync") or {})
        metadata["pinned"] = bool(pinned)
        artifact_sync["pinned"] = bool(pinned)
        artifact_sync["pin_updated_at"] = self._utcnow()
        artifact_sync["pin_reason"] = str(reason or "").strip()
        metadata["artifact_sync"] = artifact_sync
        if pinned:
            updated = self.update_artifact_record(
                artifact_id,
                metadata=metadata,
                retention_class="durable",
                retention_deadline_at="",
            )
        else:
            retention = self.artifact_retention_policy(policy=self._loads_json(row["policy"], {}), metadata=metadata)
            updated = self.update_artifact_record(
                artifact_id,
                metadata=metadata,
                retention_class=retention["retention_class"],
                retention_deadline_at=retention["retention_deadline_at"],
            )
        self.mesh._record_event(
            "mesh.artifact.pin.updated",
            peer_id=updated.get("owner_peer_id") or self.mesh.node_id,
            payload={
                "artifact_id": updated["id"],
                "digest": updated["digest"],
                "pinned": bool(pinned),
                "reason": str(reason or "").strip(),
            },
        )
        return updated

    def verify_artifact_mirror(
        self,
        artifact_id: str,
        *,
        peer_id: str = "",
        source_artifact_id: str = "",
        digest: str = "",
        client=None,
        base_url: Optional[str] = None,
    ) -> dict:
        local = self.get_artifact((artifact_id or "").strip(), include_content=False)
        metadata = dict(local.get("metadata") or {})
        artifact_sync = dict(metadata.get("artifact_sync") or {})
        peer_token = (peer_id or artifact_sync.get("source_peer_id") or metadata.get("replicated_from_peer_id") or "").strip()
        if not peer_token:
            raise self.mesh.MeshPolicyError("peer_id is required for mirror verification")
        source_artifact_token = (
            source_artifact_id
            or artifact_sync.get("source_artifact_id")
            or metadata.get("replicated_from_artifact_id")
            or ""
        ).strip()
        digest_token = str(digest or artifact_sync.get("source_digest") or local.get("digest") or "").strip().lower()
        if digest_token.startswith("sha256:"):
            digest_token = digest_token.split(":", 1)[1]
        remote_client, _ = self.mesh._resolve_peer_client(peer_token, client=client, base_url=base_url)
        verification = {
            "status": "unknown",
            "verified": False,
            "reason": "",
            "checked_at": self._utcnow(),
            "peer_id": peer_token,
            "source_artifact_id": source_artifact_token,
            "local_digest": local.get("digest") or "",
            "remote_digest": "",
            "size_match": False,
            "media_type_match": False,
            "descriptor_match": False,
        }
        try:
            remote = {}
            listing = remote_client.list_artifacts(limit=10, digest=digest_token)
            remote_candidates = list(listing.get("artifacts") or [])
            if source_artifact_token:
                remote = next(
                    (dict(item or {}) for item in remote_candidates if str((item or {}).get("id") or "").strip() == source_artifact_token),
                    {},
                )
            elif remote_candidates:
                remote = dict(remote_candidates[0] or {})
                source_artifact_token = str(remote.get("id") or "").strip()
                verification["source_artifact_id"] = source_artifact_token
            if not remote:
                raise self.mesh.MeshArtifactAccessError("remote artifact not found")
            remote_digest = str(remote.get("digest") or "").strip().lower()
            verification["remote_digest"] = remote_digest
            verification["size_match"] = int(remote.get("size_bytes") or 0) == int(local.get("size_bytes") or 0)
            verification["media_type_match"] = str(remote.get("media_type") or "") == str(local.get("media_type") or "")
            verification["descriptor_match"] = str((remote.get("oci_descriptor") or {}).get("digest") or "").strip() == self._oci_digest(remote_digest)
            verification["verified"] = (
                remote_digest == str(local.get("digest") or "").strip().lower()
                and verification["size_match"]
                and verification["media_type_match"]
                and verification["descriptor_match"]
            )
            verification["status"] = "verified" if verification["verified"] else "mismatch"
            verification["reason"] = "remote_descriptor_match" if verification["verified"] else "remote_descriptor_mismatch"
        except self.mesh.MeshArtifactAccessError:
            verification["status"] = "missing_remote"
            verification["reason"] = "remote_artifact_not_found"
        artifact_sync["verified_at"] = verification["checked_at"]
        artifact_sync["verification_status"] = verification["status"]
        metadata["artifact_sync"] = artifact_sync
        metadata["mirror_verification"] = verification
        updated = self.update_artifact_record(local["id"], metadata=metadata)
        self.mesh._record_event(
            "mesh.artifact.mirror.verified",
            peer_id=peer_token,
            payload={
                "artifact_id": updated["id"],
                "digest": updated["digest"],
                "status": verification["status"],
                "source_artifact_id": verification["source_artifact_id"],
            },
        )
        return {"status": verification["status"], "artifact": updated, "verification": verification}

    def purge_expired_artifacts(self, *, limit: int = 100) -> dict:
        purged = self.purge_expired_rows(limit=limit)
        return {"status": "ok", "peer_id": self.mesh.node_id, "purged": purged}
