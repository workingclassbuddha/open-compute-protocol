from __future__ import annotations

from typing import Any, Optional


class MeshStateProjectionService:
    """Read-model shaping helpers for mesh state projections."""

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
        worker_card_type,
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
        self._worker_card_type = worker_card_type

    def peer_device_profile(self, peer: Optional[dict]) -> dict:
        source = dict(peer or {})
        card = dict(source.get("card") or {})
        metadata = dict(source.get("metadata") or {})
        return self._normalize_device_profile(
            source.get("device_profile")
            or card.get("device_profile")
            or metadata.get("remote_device_profile")
            or {}
        )

    def row_to_peer(self, row) -> Optional[dict]:
        if row is None:
            return None
        metadata = self._loads_json(row["metadata"], {})
        card = self._loads_json(row["card"], {})
        peer_stub = {
            "peer_id": row["peer_id"],
            "trust_tier": self._normalize_trust_tier(row["trust_tier"]),
            "metadata": metadata,
            "capability_cards": self._loads_json(row["capability_cards"], []),
            "card": card,
        }
        device_profile = self.peer_device_profile({"metadata": metadata, "card": card})
        sync_policy = dict(metadata.get("remote_sync_policy") or self.mesh._device_profile_sync_policy(device_profile))
        habitat_roles = list(card.get("habitat_roles") or self.mesh._device_profile_habitat_roles(device_profile))
        continuity_capabilities = dict(card.get("continuity_capabilities") or self.mesh._continuity_capabilities(device_profile))
        treaty_capabilities = dict(card.get("treaty_capabilities") or {})
        treaty_compatibility = dict(self.mesh._peer_treaty_compatibility(device_profile, card=card))
        governance_summary = dict(card.get("governance_summary") or {})
        return {
            "peer_id": row["peer_id"],
            "organism_id": row["peer_id"],
            "display_name": row["display_name"],
            "public_key": row["public_key"],
            "signature_scheme": row["signature_scheme"],
            "endpoint_url": row["endpoint_url"],
            "stream_url": row["stream_url"],
            "trust_tier": self._normalize_trust_tier(row["trust_tier"]),
            "reachability": row["reachability"],
            "status": row["status"],
            "mesh_session_id": row["mesh_session_id"],
            "protocol_version": row["protocol_version"],
            "capability_cards": self._loads_json(row["capability_cards"], []),
            "card": card,
            "device_profile": device_profile,
            "sync_policy": sync_policy,
            "habitat_roles": habitat_roles,
            "continuity_capabilities": continuity_capabilities,
            "treaty_capabilities": treaty_capabilities,
            "treaty_compatibility": treaty_compatibility,
            "governance_summary": governance_summary,
            "metadata": metadata,
            "reliability": self.mesh._peer_reliability_summary(peer_stub),
            "load": self.mesh._peer_load_summary(peer_stub),
            "sync_state": {
                "remote_cursor": int(metadata.get("remote_cursor") or 0),
                "last_sync_at": metadata.get("last_sync_at") or "",
                "last_imported_event_count": int(metadata.get("last_imported_event_count") or 0),
                "last_sync_error": metadata.get("last_sync_error") or "",
            },
            "heartbeat": dict(metadata.get("heartbeat") or {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "last_seen_at": row["last_seen_at"],
            "last_handshake_at": row["last_handshake_at"],
        }

    def list_peers(self, *, limit: int = 25) -> dict:
        with self.mesh._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_peers
                ORDER BY last_seen_at DESC, updated_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        peers = [self.row_to_peer(row) for row in rows]
        trust_counts = {}
        device_counts = {}
        synced_recently = 0
        degraded = 0
        intermittent = 0
        for peer in peers:
            trust_counts[peer["trust_tier"]] = trust_counts.get(peer["trust_tier"], 0) + 1
            device_class = ((peer.get("device_profile") or {}).get("device_class") or "full").strip().lower()
            device_counts[device_class] = device_counts.get(device_class, 0) + 1
            if (peer.get("sync_policy") or {}).get("mode") == "intermittent":
                intermittent += 1
            if peer.get("sync_state", {}).get("last_sync_at"):
                synced_recently += 1
            if peer.get("status") == "degraded":
                degraded += 1
        return {
            "organism_id": self.mesh.node_id,
            "count": len(peers),
            "peers": peers,
            "health": {
                "connected": sum(1 for peer in peers if peer.get("status") == "connected"),
                "synced_recently": synced_recently,
                "degraded": degraded,
                "intermittent": intermittent,
                "trust_tiers": trust_counts,
                "device_classes": device_counts,
            },
        }

    def row_to_discovery_candidate(self, row) -> Optional[dict]:
        if row is None:
            return None
        manifest = self._loads_json(row["manifest"], {})
        card = dict(manifest.get("organism_card") or {})
        device_profile = self._normalize_device_profile(
            self._loads_json(row["device_profile"], {})
            or manifest.get("device_profile")
            or card.get("device_profile")
            or {}
        )
        return {
            "base_url": row["base_url"] or "",
            "peer_id": row["peer_id"] or "",
            "display_name": row["display_name"] or "",
            "endpoint_url": row["endpoint_url"] or "",
            "status": row["status"] or "discovered",
            "trust_tier": self._normalize_trust_tier(row["trust_tier"]),
            "device_profile": device_profile,
            "manifest": manifest,
            "habitat_roles": list(card.get("habitat_roles") or self.mesh._device_profile_habitat_roles(device_profile)),
            "continuity_capabilities": dict(card.get("continuity_capabilities") or self.mesh._continuity_capabilities(device_profile)),
            "treaty_capabilities": dict(card.get("treaty_capabilities") or self.mesh._treaty_capabilities(device_profile)),
            "treaty_compatibility": dict(self.mesh._peer_treaty_compatibility(device_profile, card=card)),
            "governance_summary": dict(card.get("governance_summary") or manifest.get("governance_summary") or {}),
            "metadata": self._loads_json(row["metadata"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "last_seen_at": row["last_seen_at"] or "",
            "last_error": row["last_error"] or "",
            "last_error_at": row["last_error_at"] or "",
        }

    def list_discovery_candidates(self, *, limit: int = 25, status: str = "") -> dict:
        query = ["SELECT * FROM mesh_discovery_candidates"]
        params: list[Any] = []
        status_token = str(status or "").strip().lower()
        if status_token:
            query.append("WHERE status=?")
            params.append(status_token)
        query.append("ORDER BY last_seen_at DESC, updated_at DESC LIMIT ?")
        params.append(max(1, int(limit or 25)))
        with self.mesh._conn() as conn:
            rows = conn.execute("\n".join(query), tuple(params)).fetchall()
        candidates = [self.row_to_discovery_candidate(row) for row in rows]
        return {"peer_id": self.mesh.node_id, "count": len(candidates), "candidates": candidates}

    def row_to_scheduler_decision(self, row) -> Optional[dict]:
        if row is None:
            return None
        return {
            "id": row["id"],
            "request_id": row["request_id"] or "",
            "job_id": row["job_id"] or "",
            "job_kind": row["job_kind"] or "",
            "status": row["status"] or "",
            "strategy": row["strategy"] or "",
            "target_type": row["target_type"] or "",
            "peer_id": row["peer_id"] or "",
            "score": int(row["score"] or 0),
            "placement": self._loads_json(row["placement"], {}),
            "selected": self._loads_json(row["selected"], {}),
            "candidates": self._loads_json(row["candidates"], []),
            "created_at": row["created_at"],
        }

    def row_to_lease(self, row) -> dict:
        return self._lease_record_type(
            id=row["id"],
            resource=row["resource"],
            peer_id=row["peer_id"],
            agent_id=row["agent_id"] or "",
            job_id=row["job_id"] or "",
            status=row["status"],
            ttl_seconds=int(row["ttl_seconds"] or 300),
            lock_token=row["lock_token"] or "",
            metadata=self._loads_json(row["metadata"], {}),
            created_at=row["created_at"],
            heartbeat_at=row["heartbeat_at"],
            expires_at=row["expires_at"],
            released_at=row["released_at"] or "",
        ).to_dict()

    def row_to_queue_message(self, row) -> Optional[dict]:
        if row is None:
            return None
        return self._queue_message_type(
            id=row["id"],
            job_id=row["job_id"],
            queue_name=row["queue_name"] or "default",
            status=row["status"] or "queued",
            dedupe_key=row["dedupe_key"] or "",
            ack_deadline_seconds=int(row["ack_deadline_seconds"] or 300),
            dead_letter_queue=row["dead_letter_queue"] or "",
            delivery_attempts=int(row["delivery_attempts"] or 0),
            visibility_timeout_at=row["visibility_timeout_at"] or "",
            available_at=row["available_at"] or "",
            claimed_at=row["claimed_at"] or "",
            acked_at=row["acked_at"] or "",
            replay_deadline_at=row["replay_deadline_at"] or "",
            retention_deadline_at=row["retention_deadline_at"] or "",
            lease_id=row["lease_id"] or "",
            worker_id=row["worker_id"] or "",
            current_attempt_id=row["current_attempt_id"] or "",
            last_error=row["last_error"] or "",
            metadata=self._loads_json(row["metadata"], {}),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        ).to_dict()

    def row_to_attempt(self, row) -> dict:
        return self._job_attempt_type(
            id=row["id"],
            job_id=row["job_id"],
            attempt_number=int(row["attempt_number"] or 1),
            worker_id=row["worker_id"],
            status=row["status"],
            lease_id=row["lease_id"] or "",
            executor=row["executor"] or "",
            result_ref=self._loads_json(row["result_ref"], {}),
            error=row["error"] or "",
            metadata=self._loads_json(row["metadata"], {}),
            started_at=row["started_at"],
            heartbeat_at=row["heartbeat_at"],
            finished_at=row["finished_at"] or "",
        ).to_dict()

    def row_to_worker(self, row) -> Optional[dict]:
        if row is None:
            return None
        return self._worker_card_type(
            id=row["id"],
            peer_id=row["peer_id"],
            agent_id=row["agent_id"] or "",
            status=row["status"],
            capabilities=self._loads_json(row["capabilities"], []),
            resources=self._loads_json(row["resources"], {}),
            labels=self._loads_json(row["labels"], []),
            max_concurrent_jobs=int(row["max_concurrent_jobs"] or 1),
            metadata=self._loads_json(row["metadata"], {}),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            last_heartbeat_at=row["last_heartbeat_at"],
        ).to_dict() | {
            "active_attempts": self.mesh._worker_active_attempts(row["id"]),
        }

    def list_workers(self, *, limit: int = 25) -> dict:
        with self.mesh._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_workers
                ORDER BY last_heartbeat_at DESC, updated_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        workers = [self.row_to_worker(row) for row in rows]
        return {"peer_id": self.mesh.node_id, "count": len(workers), "workers": workers}

    def row_to_job(self, row) -> dict:
        lease = {}
        lease_id = (row["lease_id"] or "").strip()
        if lease_id:
            lease_row = self.mesh._lease_row(lease_id)
            if lease_row is not None:
                lease = self.row_to_lease(lease_row)
        attempts = [self.row_to_attempt(attempt_row) for attempt_row in self.mesh._list_attempt_rows(row["id"])]
        queue_message = self.mesh._queue_message_for_job(row["id"])
        metadata = self._loads_json(row["metadata"], {})
        spec = dict(metadata.get("job_spec") or {})
        if not spec:
            spec = self.mesh._normalize_job_spec(
                {
                    "kind": row["kind"],
                    "origin": row["origin_peer_id"],
                    "request_id": row["request_id"],
                    "payload": self._loads_json(row["payload_inline"], {}),
                    "payload_ref": self._loads_json(row["payload_ref"], {}),
                    "artifact_inputs": self._loads_json(row["artifact_inputs"], []),
                    "requirements": self._loads_json(row["requirements"], {}),
                    "policy": self._loads_json(row["policy"], {}),
                    "metadata": metadata,
                    "created_at": row["created_at"],
                },
                requirements=self._loads_json(row["requirements"], {}),
                policy=self._loads_json(row["policy"], {}),
                metadata=metadata,
            )
        recovery = self.mesh._job_recovery_contract(
            {"status": row["status"], "metadata": metadata, "spec": spec},
            metadata=metadata,
            spec=spec,
        )
        return self._mesh_job_type(
            id=row["id"],
            request_id=row["request_id"],
            kind=row["kind"],
            origin=row["origin_peer_id"],
            target=row["target_peer_id"],
            requirements=self._loads_json(row["requirements"], {}),
            policy=self._normalize_policy(self._loads_json(row["policy"], {})),
            payload_ref=self._loads_json(row["payload_ref"], {}),
            artifact_inputs=self._loads_json(row["artifact_inputs"], []),
            status=row["status"],
            result_ref=self._loads_json(row["result_ref"], {}),
            lease=lease,
            metadata=metadata,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        ).to_dict() | {
            "payload_inline": self._loads_json(row["payload_inline"], {}),
            "executor": row["executor"] or "",
            "attempts": attempts,
            "queue": queue_message,
            "spec": spec,
            "mission": dict(metadata.get("mission") or {}),
            "latest_checkpoint_ref": dict(metadata.get("latest_checkpoint_ref") or {}),
            "resume_checkpoint_ref": dict(metadata.get("resume_checkpoint_ref") or {}),
            "selected_resume_checkpoint_ref": dict(metadata.get("resume_checkpoint_ref") or {}),
            "resume_count": int(metadata.get("resume_count") or 0),
            "checkpointed_at": str(metadata.get("checkpointed_at") or ""),
            "last_resumed_at": str(metadata.get("last_resumed_at") or ""),
            "last_resumed_by": str(metadata.get("last_resumed_by") or ""),
            "last_resume_reason": str(metadata.get("last_resume_reason") or ""),
            "last_resume_requested_at": str(metadata.get("last_resume_requested_at") or ""),
            "last_resume_requested_by": str(metadata.get("last_resume_requested_by") or ""),
            "last_resume_requested_reason": str(metadata.get("last_resume_requested_reason") or ""),
            "last_restart_at": str(metadata.get("last_restart_at") or ""),
            "last_restart_by": str(metadata.get("last_restart_by") or ""),
            "last_restart_reason": str(metadata.get("last_restart_reason") or ""),
            "recovery": recovery,
            "result_bundle_ref": dict(metadata.get("result_bundle_ref") or {}),
            "result_config_ref": dict(metadata.get("result_config_ref") or {}),
            "result_attestation_ref": dict(metadata.get("result_attestation_ref") or {}),
            "result_artifacts": dict(metadata.get("result_artifacts") or {}),
            "secret_delivery": [dict(item) for item in list(metadata.get("secret_delivery") or [])],
        }
