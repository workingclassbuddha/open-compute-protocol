from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Optional

from mesh_protocol import MeshPolicyError
from .projections import MeshStateProjectionService

logger = logging.getLogger(__name__)


class MeshStateService:
    """Event journal and secret-store helpers for SovereignMesh."""

    def __init__(
        self,
        mesh,
        *,
        job_attempt_type,
        lease_record_type,
        loads_json,
        mesh_job_type,
        normalize_device_profile,
        normalize_policy,
        normalize_trust_tier,
        queue_message_type,
        secret_value_digest,
        worker_card_type,
        utcnow,
    ):
        self.mesh = mesh
        self._job_attempt_type = job_attempt_type
        self._lease_record_type = lease_record_type
        self._loads_json = loads_json
        self._mesh_job_type = mesh_job_type
        self._normalize_device_profile = normalize_device_profile
        self._normalize_policy = normalize_policy
        self._normalize_trust_tier = normalize_trust_tier
        self._queue_message_type = queue_message_type
        self._secret_value_digest = secret_value_digest
        self._worker_card_type = worker_card_type
        self._utcnow = utcnow
        self.projections = MeshStateProjectionService(
            mesh,
            job_attempt_type=job_attempt_type,
            lease_record_type=lease_record_type,
            loads_json=loads_json,
            mesh_job_type=mesh_job_type,
            normalize_device_profile=normalize_device_profile,
            normalize_policy=normalize_policy,
            normalize_trust_tier=normalize_trust_tier,
            queue_message_type=queue_message_type,
            worker_card_type=worker_card_type,
        )

    def get_peer_row(self, peer_id: str):
        with self.mesh._conn() as conn:
            return conn.execute("SELECT * FROM mesh_peers WHERE peer_id=?", ((peer_id or "").strip(),)).fetchone()

    def record_event(
        self,
        event_type: str,
        *,
        peer_id: str = "",
        request_id: str = "",
        payload: Optional[dict] = None,
    ) -> dict:
        payload = dict(payload or {})
        event_id = str(uuid.uuid4())
        now = self._utcnow()
        with self.mesh._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_events (id, event_type, peer_id, request_id, payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (event_id, event_type, peer_id or None, request_id or None, json.dumps(payload), now),
            )
            row = conn.execute("SELECT * FROM mesh_events WHERE id=?", (event_id,)).fetchone()
            conn.commit()
        try:
            self.mesh.lattice.log_event(
                "mesh",
                f"{event_type} · {peer_id or self.mesh.node_id}",
                source="sovereign_mesh",
                payload=payload,
            )
        except Exception:
            logger.debug("mesh event mirror logging failed", exc_info=True)
        if self.mesh.registry is not None:
            try:
                self.mesh.registry.log_action(
                    event_type,
                    agent_id=peer_id or self.mesh.node_id,
                    agent_name=payload.get("display_name") or payload.get("from_agent") or payload.get("to_agent"),
                    resource=payload.get("resource") or payload.get("job_id") or payload.get("handoff_id"),
                    details=payload,
                )
            except Exception:
                logger.debug("mesh registry logging failed", exc_info=True)
        return self.row_to_event(row)

    def stream_snapshot(self, *, since_seq: int = 0, limit: int = 50) -> dict:
        with self.mesh._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_events
                WHERE seq > ?
                ORDER BY seq ASC
                LIMIT ?
                """,
                (max(0, int(since_seq)), max(1, int(limit))),
            ).fetchall()
        events = [self.row_to_event(row) for row in rows]
        return {
            "organism_id": self.mesh.node_id,
            "device_profile": dict(self.mesh.device_profile),
            "sync_policy": self.mesh._device_profile_sync_policy(self.mesh.device_profile),
            "transport": {
                "route": "/mesh/stream",
                "mode": "snapshot",
                "websocket_bootstrap": True,
            },
            "events": events,
            "next_cursor": (events[-1]["seq"] if events else since_seq),
            "agent_presence": self.mesh.export_agent_presence(limit=50),
            "beacons": self.mesh.export_beacons(limit=12),
            "workers": self.mesh.list_workers(limit=20)["workers"],
            "peers": self.mesh.list_peers(limit=25)["peers"],
            "queue_metrics": self.mesh.queue_metrics(),
            "generated_at": self._utcnow(),
        }

    def row_to_event(self, row) -> dict:
        return {
            "seq": int(row["seq"]),
            "id": row["id"],
            "event_type": row["event_type"],
            "peer_id": row["peer_id"] or "",
            "request_id": row["request_id"] or "",
            "payload": self._loads_json(row["payload"], {}),
            "created_at": row["created_at"],
        }

    def peer_device_profile(self, peer: Optional[dict]) -> dict:
        return self.projections.peer_device_profile(peer)

    def row_to_peer(self, row) -> Optional[dict]:
        return self.projections.row_to_peer(row)

    def list_peers(self, *, limit: int = 25) -> dict:
        return self.projections.list_peers(limit=limit)

    def row_to_discovery_candidate(self, row) -> Optional[dict]:
        return self.projections.row_to_discovery_candidate(row)

    def list_discovery_candidates(self, *, limit: int = 25, status: str = "") -> dict:
        return self.projections.list_discovery_candidates(limit=limit, status=status)

    def row_to_scheduler_decision(self, row) -> Optional[dict]:
        return self.projections.row_to_scheduler_decision(row)

    def row_to_lease(self, row) -> dict:
        return self.projections.row_to_lease(row)

    def row_to_queue_message(self, row) -> Optional[dict]:
        return self.projections.row_to_queue_message(row)

    def row_to_attempt(self, row) -> dict:
        return self.projections.row_to_attempt(row)

    def row_to_worker(self, row) -> Optional[dict]:
        return self.projections.row_to_worker(row)

    def list_workers(self, *, limit: int = 25) -> dict:
        return self.projections.list_workers(limit=limit)

    def row_to_job(self, row) -> dict:
        return self.projections.row_to_job(row)

    def row_to_secret(self, row, *, include_value: bool = False) -> Optional[dict]:
        if row is None:
            return None
        secret = {
            "id": row["id"],
            "scope": row["scope"] or "",
            "name": row["name"] or "",
            "metadata": self._loads_json(row["metadata"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        value = row["value"] or ""
        secret["value_present"] = bool(value)
        secret["value_digest"] = self._secret_value_digest(value) if value else ""
        if include_value:
            secret["value"] = value
        return secret

    def put_secret(
        self,
        name: str,
        value: Any,
        *,
        scope: str,
        metadata: Optional[dict] = None,
    ) -> dict:
        secret_name = str(name or "").strip()
        secret_scope = str(scope or "").strip()
        if not secret_name:
            raise MeshPolicyError("secret name is required")
        if not secret_scope:
            raise MeshPolicyError("secret scope is required")
        now = self._utcnow()
        secret_id = uuid.uuid5(uuid.NAMESPACE_URL, f"ocp-secret:{secret_scope}:{secret_name}").hex
        with self.mesh._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_secrets (id, scope, name, value, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(scope, name) DO UPDATE SET
                    value=excluded.value,
                    metadata=excluded.metadata,
                    updated_at=excluded.updated_at
                """,
                (
                    secret_id,
                    secret_scope,
                    secret_name,
                    str(value),
                    json.dumps(dict(metadata or {})),
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM mesh_secrets WHERE scope=? AND name=?",
                (secret_scope, secret_name),
            ).fetchone()
            conn.commit()
        return self.row_to_secret(row) or {}

    def get_secret(self, name: str, *, scope: str, include_value: bool = False) -> dict:
        secret_name = str(name or "").strip()
        secret_scope = str(scope or "").strip()
        with self.mesh._conn() as conn:
            row = conn.execute(
                "SELECT * FROM mesh_secrets WHERE scope=? AND name=?",
                (secret_scope, secret_name),
            ).fetchone()
        if row is None:
            raise MeshPolicyError("secret not found")
        return self.row_to_secret(row, include_value=include_value) or {}

    def list_secrets(self, *, limit: int = 25, scope: str = "") -> dict:
        params: list[Any] = []
        query = "SELECT * FROM mesh_secrets"
        if str(scope or "").strip():
            query += " WHERE scope=?"
            params.append(str(scope or "").strip())
        query += " ORDER BY updated_at DESC, created_at DESC LIMIT ?"
        params.append(max(1, int(limit or 25)))
        with self.mesh._conn() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        secrets_list = [self.row_to_secret(row) for row in rows if row is not None]
        return {"count": len(secrets_list), "secrets": secrets_list}
